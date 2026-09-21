/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * main.c -- MDS daemon entry point.
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>

#include "pnfs_mds.h"
#include "rpc_server.h"
#include "compound.h"
#include "mds_gss.h"
#include "mds_tls.h"
#include "copy_offload.h"
#include "cluster_membership.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "subtree_map.h"
#include "cluster_transport.h"
#include "health.h"
#include "grace.h"
#include "session.h"
#include "commit_queue.h"
#include "layout_recall.h"
#include "ds_health.h"
#include "ds_prealloc.h"
#include "ds_prepare.h"
#include "ds_gc.h"
#include "proxy_io.h"
#include "referral.h"
#include "resilver.h"
#include "rebalance.h"
#include "io_tracker.h"
#include "quota.h"
#include "tiering.h"
#include "open_state.h"
#include "lock_state.h"
#include "failover.h"
#include "failover_watchdog.h"
#include "subtree_split.h"
#include "mds_shard.h"
#include "ds_cache.h"
#include "ds_capacity.h"
#include "ds_io_limits.h"
#include "inode_cache.h"
#include "parent_touch.h"
#include "remove_manifest.h"
#include "dirent_cache.h"
#include "layout_cache.h"
#include "layout_commit_aggregator.h"
#include "delegation.h"
#include "dir_delegation.h"
#include "nfs4_cb.h"
#include "metrics_http.h"
#include "mds_metrics.h"
#include "mds_op_metrics.h"
#include "mountd_compat.h"
#include "hpc_shared.h"
#include "mds_cluster.h"
#include "catalog_image.h"
#include "failover_watchdog.h"

/** Maximum concurrent RPC listener threads. */
#define MAX_RPC_LISTENERS 32

/** Whole-second pause without the signal side effects sleep() may
 * carry (nanosleep is async-signal- and thread-safe).  An EINTR
 * early return is acceptable at every call site, as with sleep(). */
static void sleep_seconds(unsigned int sec)
{
	struct timespec ts = { .tv_sec = (time_t)sec, .tv_nsec = 0 };

	(void)nanosleep(&ts, NULL);
}

/** RPC listener thread entry point (blocking). */
static void *rpc_listener_thread(void *arg)
{
	struct rpc_server *srv = arg;

	(void)rpc_server_start(srv);
	return NULL;
}

/* -----------------------------------------------------------------------
 * Cluster heartbeat thread (Phase 9A)
 *
 * Runs only when mds_cluster_supported(cat): refreshes this node's
 * registry row through the backend-neutral mds_cluster_node_heartbeat
 * dispatcher and, every third cycle, re-reads the partition map and
 * the node registry so ownership / peer changes made by other MDS
 * nodes become visible without flooding the store with scans.
 *
 * The argument block is written completely (including smap and
 * membership) BEFORE pthread_create and never written afterwards, so
 * the thread reads it without synchronisation.
 *
 * Supersession: node_register replaces a row whose boot_epoch is lower
 * (mds_cluster.h), so a second daemon started later under this mds_id
 * takes the row over and every heartbeat from here answers
 * MDS_ERR_STALE.  Continuing to serve unregistered would leave two
 * heads behind one id with no standby watching this one, so the thread
 * fences this daemon: it logs the superseding epoch, sets
 * cluster_superseded (main turns that into EXIT_FAILURE and skips the
 * deregister, which would be STALE too) and raises the process-wide
 * SIGTERM that main's sigwait() consumes -- the same cleanup path the
 * startup deadline takes.
 * ----------------------------------------------------------------------- */

static _Atomic bool cluster_hb_flag = true;
static _Atomic bool cluster_superseded = false;

struct cluster_hb_arg {
	struct mds_catalogue *cat;
	uint32_t mds_id;
	uint64_t boot_epoch;
	_Atomic bool *running;
	struct subtree_map *smap;  /**< Refreshed every heartbeat cycle. */
	struct cluster_membership *membership; /**< Peer refresh cycle. */
};
static struct cluster_hb_arg cluster_hb_arg_g;

static void *cluster_hb_fn(void *a)
{
	struct cluster_hb_arg *arg = a;
	uint32_t cycle = 0;

	while (atomic_load(arg->running)) {
		uint64_t superseder = 0;
		enum mds_status hb_st;

		/* NOTFOUND (row gone) and transient errors are retried
		 * next cycle, as before; STALE is fatal (see above). */
		hb_st = cluster_heartbeat_tick(arg->cat, arg->mds_id,
					       arg->boot_epoch, &superseder);
		if (hb_st == MDS_ERR_STALE) {
			MDS_LOG_FATAL(LOG_COMP_MDS,
				"node registry row for mds_id %u now carries "
				"boot_epoch %llu, not ours (%llu): a newer "
				"incarnation registered while this one is "
				"running; fencing this daemon (shutting down, "
				"deregister skipped)",
				(unsigned)arg->mds_id,
				(unsigned long long)superseder,
				(unsigned long long)arg->boot_epoch);
			atomic_store(&cluster_superseded, true);
			(void)kill(getpid(), SIGTERM);
			break;
		}

		/* Refresh subtree map + membership every 3rd cycle (~15s)
		 * to pick up ownership and peer changes from other MDS
		 * nodes without flooding the store with scans. */
		if ((cycle % 3) == 0) {
			if (arg->smap != NULL) {
				(void)subtree_map_refresh_from_catalogue(
					arg->smap, arg->cat);
			}
			if (arg->membership != NULL) {
				(void)cluster_membership_populate(
					arg->membership, arg->cat);
			}
		}
		cycle++;
		sleep_seconds(5);
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Startup registry conflict report
 *
 * node_register refused with MDS_ERR_EXISTS: the registry already holds
 * a row for this mds_id with an equal or higher boot_epoch
 * (mds_cluster.h).  Read that row back so the fatal message names it --
 * the operator must tell a duplicate live daemon from the stale row of
 * a dead incarnation whose epoch is ahead of this host's wall clock.
 * ----------------------------------------------------------------------- */

struct registry_conflict {
	uint32_t mds_id;
	bool     found;
	uint64_t boot_epoch;
	uint64_t last_heartbeat_ns;
	char     hostname[256];
};

static int registry_conflict_cb(uint32_t mds_id, uint64_t boot_epoch,
				const char *hostname,
				uint16_t nfs_port, uint16_t grpc_port,
				uint64_t last_heartbeat_ns, void *ctx)
{
	struct registry_conflict *conf = ctx;

	(void)nfs_port;
	(void)grpc_port;
	if (mds_id != conf->mds_id) {
		return 0;
	}
	conf->found = true;
	conf->boot_epoch = boot_epoch;
	conf->last_heartbeat_ns = last_heartbeat_ns;
	(void)snprintf(conf->hostname, sizeof(conf->hostname), "%s",
		       hostname != NULL ? hostname : "");
	return 1;
}

static void log_registry_conflict(struct mds_catalogue *cat,
				  uint32_t mds_id, uint64_t our_epoch)
{
	struct registry_conflict conf;

	memset(&conf, 0, sizeof(conf));
	conf.mds_id = mds_id;
	(void)mds_cluster_node_list(cat, registry_conflict_cb, &conf);
	if (conf.found) {
		MDS_LOG_FATAL(LOG_COMP_MDS,
			"node registry already holds mds_id %u with "
			"boot_epoch %llu (host %s, last heartbeat %llu ns), "
			"not below ours (%llu): another live daemon uses this "
			"mds_id, or this host's clock was stepped back below "
			"the previous incarnation's epoch.  Refusing to start "
			"(split-brain).  Stop the duplicate, or -- if that "
			"incarnation is dead -- let the clock pass its epoch "
			"or remove the stale row, then restart.",
			(unsigned)mds_id,
			(unsigned long long)conf.boot_epoch,
			conf.hostname,
			(unsigned long long)conf.last_heartbeat_ns,
			(unsigned long long)our_epoch);
	} else {
		MDS_LOG_FATAL(LOG_COMP_MDS,
			"node registry refused mds_id %u (boot_epoch %llu) "
			"as already registered but the row could not be read "
			"back; refusing to start",
			(unsigned)mds_id, (unsigned long long)our_epoch);
	}
}

/** DS failure callback adapter -- bridges ds_health to layout_recall. */
static void ds_fail_recall_cb(uint32_t ds_id, void *ctx)
{
	struct layout_recall *lr = ctx;

	(void)layout_recall_for_ds(lr, ds_id);
}

/* -----------------------------------------------------------------------
 * LAYOUTCOMMIT aggregator flush callback (Phase F of
 * docs/hpc-nto1-plan.md, integration part A).
 *
 * Runs on the aggregator's timer thread (or inline from
 * layout_commit_aggregator_flush_all_dirty during shutdown).  Reads
 * the current inode, applies max(size) / latest(mtime) merge
 * semantics, and writes back via mds_cat_ns_setattr with the
 * appropriate mask.  After a successful write the inode_cache is
 * invalidated so the next compound observes the fresh values.
 *
 * Returns 0 on success (bucket cleared), -1 on persistence failure
 * (bucket stays dirty for the next attempt).  An MDS_ERR_NOTFOUND
 * from getattr is treated as success so the bucket is dropped
 * -- this covers the unlink-while-flush race.
 * ----------------------------------------------------------------------- */
struct lcommit_flush_ctx {
	struct mds_catalogue *cat;
	struct inode_cache   *icache;  /* may be NULL */
};

/* File-scope storage so the address survives across main()'s scope
 * boundaries and lives for the full daemon lifetime.  The aggregator
 * is created and destroyed in main(), and the flush thread is joined
 * before main() returns, so the lifetime model is straightforward. */
static struct lcommit_flush_ctx s_lcommit_flush_ctx;

static int lcommit_flush_cb(uint64_t fileid, uint64_t size,
			    struct timespec mtime, void *cookie)
{
	struct lcommit_flush_ctx *ctx = cookie;
	struct mds_inode inode;
	enum mds_status st;
	uint32_t mask = 0;

	if (ctx == NULL || ctx->cat == NULL) {
		return -1;
	}

	st = mds_cat_ns_getattr(ctx->cat, fileid, &inode);
	if (st == MDS_ERR_NOTFOUND) {
		/* File was unlinked while the bucket was dirty.  Drop the
		 * snapshot -- the size value no longer has any meaning. */
		return 0;
	}
	if (st != MDS_OK) {
		return -1;
	}

	/* Merge max-size, latest-mtime into the inode. */
	if (size > inode.size) {
		inode.size = size;
		mask |= MDS_ATTR_SIZE;
	}
	if (mtime.tv_sec > inode.mtime.tv_sec ||
	    (mtime.tv_sec == inode.mtime.tv_sec &&
	     mtime.tv_nsec > inode.mtime.tv_nsec)) {
		inode.mtime = mtime;
		mask |= MDS_ATTR_MTIME;
	}
	if (mask == 0) {
		/* Persisted state is already at-or-ahead of the
		 * aggregator snapshot.  Nothing to write. */
		return 0;
	}

	st = mds_cat_ns_setattr(ctx->cat, NULL, fileid, &inode, mask);
	if (st != MDS_OK) {
		return -1;
	}

	if (ctx->icache != NULL) {
		inode_cache_invalidate(ctx->icache, fileid);
	}
	return 0;
}

/** Partner-loss callback -- bridges membership watch to failover. */
static void failover_on_partner_loss(
	const struct cluster_member *removed, void *arg)
{
	struct failover_ctx *fo = arg;
	MDS_LOG_INFO(LOG_COMP_MDS,
		"partner %u (role=%d, lifecycle=%d) lost -- "
		"attempting promotion",
		removed->mds_id, (int)removed->role,
		(int)removed->lifecycle);
	enum mds_status st = failover_promote(fo);
	if (st == MDS_OK) {
		MDS_LOG_INFO(LOG_COMP_MDS,
			"failover promotion succeeded");
	} else {
		MDS_LOG_WARN(LOG_COMP_MDS,
			"failover promotion failed: %d", (int)st);
	}
}


/* -----------------------------------------------------------------------
 * parent_touch flush callback (deferred parent-dir attr aggregator,
 * ported).  Persists the accumulated change delta + flush-time
 * mtime/ctime stamp as ONE interpreted NDB update via
 * mds_cat_ns_parent_touch, then invalidates the cached inode.
 * Returns 0 on success (a vanished parent row counts as success),
 * -1 on persistence failure (the aggregator restores the delta and
 * retries on the next tick).
 * ----------------------------------------------------------------------- */
struct pt_flush_ctx {
	struct mds_catalogue *cat;
	struct inode_cache   *icache;  /* may be NULL */
};

static struct pt_flush_ctx s_pt_flush_ctx;
static struct parent_touch *s_pt;
static struct remove_manifest *s_rmf;

static int pt_flush_cb(uint64_t fileid, uint64_t change_delta,
		       struct timespec stamp, void *cookie)
{
	struct pt_flush_ctx *ctx = cookie;
	enum mds_status st;

	if (ctx == NULL || ctx->cat == NULL) {
		return -1;
	}
	st = mds_cat_ns_parent_touch(ctx->cat, fileid, change_delta,
				     stamp);
	if (st != MDS_OK) {
		return -1;
	}
	if (ctx->icache != NULL) {
		inode_cache_invalidate(ctx->icache, fileid);
	}
	return 0;
}

/* Linear daemon lifecycle: one start-up sequence with a single
 * cleanup label, whose score is dominated by the ~100 logging macro
 * sites (do/while + level gate each); splitting it into helpers would
 * only relocate the sequence.  Same annotation as the other
 * inherently linear dispatchers in this tree. */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int main(int argc, char *argv[])
{
	struct mds_config cfg;
	enum mds_status rc;
	const char *config_path = "/etc/pnfs-mds/mds.conf";
	/* Boot epoch: stamps every shared-state row this daemon instance
	 * writes (open/lock/deleg owner fencing, remove-manifest claims,
	 * node registry) so a restarted MDS can be told apart from the
	 * previous incarnation.  Backend-independent; generated once the
	 * catalogue is open.
	 *
	 * It is CLOCK_REALTIME in nanoseconds (step 2c) because the node
	 * registry contract (mds_cluster.h) needs it monotonic across
	 * restarts of one mds_id, including a host reboot -- a since-boot
	 * clock restarts near zero and would rank a fresh incarnation
	 * below the dead row it replaces.  Every consumer treats the value
	 * as an identity (equality / ordering), never as a time base.
	 * Ordering across restarts therefore relies on the wall clock not
	 * being stepped back below the previous incarnation's value; when
	 * it is, node_register refuses with MDS_ERR_EXISTS and step 2d
	 * exits with a message naming the row and both epochs -- never a
	 * silent unregistered start. */
	uint64_t mds_boot_epoch = 0;
	/* CLOCK_MONOTONIC at a successful node_register (step 2d); 0 when
	 * this node did not register.  Step 4c measures the startup
	 * deadline from it. */
	uint64_t cluster_register_mono_ns = 0;
	/* Cluster heartbeat thread; started iff mds_cluster_supported(cat)
	 * (see step 4c), joined first at shutdown. */
	pthread_t cluster_hb_thread;
	bool cluster_hb_running = false;
	struct health_monitor *hm = NULL;
	struct failover_ctx *fo_ctx = NULL;
	struct failover_watchdog *fo_wd = NULL;
	int exit_code = EXIT_SUCCESS;
	struct mds_catalogue *cat = NULL;
	struct subtree_map *smap = NULL;
	struct cluster_membership *membership = NULL;
	struct mds_proxy_ctx *proxy = NULL;
	struct cluster_server *ct_srv = NULL;
	struct threadpool *rpc_tp = NULL;
	struct commit_queue *cq = NULL;
	struct session_table *session_tbl = NULL;
	struct layout_recall *lr = NULL;
	struct ds_health_monitor *ds_hm = NULL;
	struct ds_prealloc_ctx *ds_pa = NULL;
	struct open_state_table *ot = NULL;
	struct lock_table *lock_tbl = NULL;
	struct resilver_worker *rw = NULL;
	struct rebalance_worker *rbw = NULL;
	struct io_tracker *iot = NULL;
	struct mds_quota_ctx *quota = NULL;
	struct tiering_worker *tw = NULL;
	struct copy_offload_table *cot = NULL;
	struct split_evaluator *split_eval = NULL;
	struct ds_prepare_ctx *ds_prep = NULL;
	struct ds_cache *ds_cache = NULL;
	struct ds_capacity *ds_cap = NULL;
	struct ds_gc *ds_gc = NULL;
	struct mds_shard_map *shard_map = NULL;
	struct layout_commit_aggregator *lcommit_agg = NULL;
	/* Catalog image fed by the backend's changefeed (image mode);
	 * NULL when image mode is off or the backend has no feed. */
	struct catalog_image *image = NULL;
	struct rpc_server *rpc_srv[MAX_RPC_LISTENERS];
	pthread_t rpc_threads[MAX_RPC_LISTENERS];
	uint32_t rpc_listener_count = 0;
	uint16_t bound_port = 0;
	uint32_t started_count = 0;
	struct mountd_compat_ctx *mountd_compat = NULL;
	struct metrics_http_ctx *mhttp = NULL;

	/* Force line-buffered stderr so worker-thread diagnostics
	 * reach the systemd journal promptly (glibc defaults to
	 * fully-buffered when stderr is a pipe). */
	/* NOLINTNEXTLINE(cert-err33-c) */
	setvbuf(stderr, NULL, _IOLBF, 0);

	if (argc > 1) {
		config_path = argv[1];
	}

	memset((void *)rpc_srv, 0, sizeof(rpc_srv));
	memset((void *)rpc_threads, 0, sizeof(rpc_threads));

	/* 1. Load configuration */
	rc = mds_config_load(config_path, &cfg);
	if (rc != MDS_OK) {
		(void)fprintf(stderr, "Failed to load config from %s: %d\n",
			config_path, rc);
		return EXIT_FAILURE;
	}

	/* 1a0. Initialise logging now that config is parsed.  An empty
	 * log_file routes to stderr; a path is opened in append mode.
	 * Resolve each component's level as its per-component override
	 * when set (>= 0), else the global level.  Every diagnostic after
	 * this point flows through the leveled logger. */
	mds_log_init(cfg.log_file[0] != '\0' ? cfg.log_file : NULL);
	for (int comp = 0; comp < LOG_COMP_COUNT; comp++) {
		int lvl = (cfg.log_level_by_component[comp] >= 0)
			? cfg.log_level_by_component[comp]
			: cfg.log_level_global;
		mds_log_set_level(comp, lvl);
	}

	/* 1a. Initialise grace subsystem (must precede any RPC path). */
	grace_init();

	/* 1a'. Apply the master kill-switch for the per-op latency,
	 * per-catalogue-op latency, and per-op*phase histograms.  When
	 * disabled, every observation site takes a one-load early-return
	 * path.  The threadpool's queue-wait sampling (two clock_gettime
	 * calls per work item + histogram) follows this switch too; its
	 * plain counters/gauges stay always-on. */
	mds_op_metrics_set_enabled(cfg.metrics_op_enabled);
	compound_set_perf_threshold_us(cfg.compound_perf_threshold_us);
	MDS_LOG_INFO(LOG_COMP_MDS,
		"op_metrics=%s compound_perf_threshold_us=%u",
		cfg.metrics_op_enabled ? "on" : "off",
		cfg.compound_perf_threshold_us);

	/* 1b. Block SIGINT/SIGTERM before any worker threads.
	 *     All subsequently created threads inherit the blocked mask;
	 *     the main thread uses sigwait() for reliable delivery. */
	sigset_t shutdown_set;
	sigemptyset(&shutdown_set);
	sigaddset(&shutdown_set, SIGINT);
	sigaddset(&shutdown_set, SIGTERM);
	(void)pthread_sigmask(SIG_BLOCK, &shutdown_set, NULL);

	/* 2. Open the catalogue.  Backend-specific config validation
	 * (e.g. RonDB refusing inline_enabled) lives inside the backend's
	 * constructor, so a refused configuration surfaces here. */
	rc = mds_catalogue_open(&cfg, &cat);
	if (rc != MDS_OK) {
		MDS_LOG_ERROR(LOG_COMP_MDS,
			"Failed to open metadata catalogue: %d",
			(int)rc);
		return EXIT_FAILURE;
	}

	/* 2a. Multi-MDS operation needs the store's cluster services
	 * (node registry, heartbeat, partition map) AND a store that
	 * every MDS process can reach (MDS_CAT_CAP_MULTI_PROCESS); that
	 * is exactly mds_cluster_supported().  Refuse to start a
	 * multi-node configuration otherwise rather than run a node that
	 * no peer can see.  An in-process store that populates the slots
	 * for tests never carries the capability, so it is refused here
	 * by construction. */
	if (cfg.cluster_size > 1 && !mds_cluster_supported(cat)) {
		MDS_LOG_FATAL(LOG_COMP_MDS,
			"cluster_size=%u but catalogue backend %d has no "
			"multi-process cluster services",
			(unsigned)cfg.cluster_size,
			(int)mds_catalogue_backend_type(cat));
		mds_catalogue_close(cat);
		return EXIT_FAILURE;
	}

	/* 2b. Schema bootstrap for backends that have one (probe fails =
	 * no tables).  Phase 10A: retry loop guards against concurrent
	 * bootstrap race when multiple MDS instances start
	 * simultaneously.  If another MDS is bootstrapping, the probe
	 * will eventually succeed. */
	if (mds_catalogue_bootstrap_supported(cat)) {
		const int bootstrap_max_retries = 10;
		int attempt;

		/*
		 * Always invoke bootstrap once on startup.  The DDL is
		 * idempotent (create_table_if_not_exists), and this lets
		 * schema-version upgrade blocks inside the backend's
		 * bootstrap run on an existing cluster.
		 * Probe-then-bootstrap-on-failure alone skipped upgrades
		 * because the legacy probe table always exists on a
		 * previously-bootstrapped cluster.
		 */
		rc = mds_catalogue_bootstrap(cat);
		if (rc != MDS_OK) {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"initial bootstrap returned %d, "
				"entering probe-retry loop...",
				(int)rc);
		}
		for (attempt = 0; attempt < bootstrap_max_retries;
		     attempt++) {
			if (mds_catalogue_probe(cat) == MDS_OK) {
				break; /* Schema ready. */
			}
			if (attempt == 0) {
				MDS_LOG_INFO(LOG_COMP_MDS,
					"catalogue probe failed after "
					"initial bootstrap, retrying "
					"bootstrap...");
				rc = mds_catalogue_bootstrap(cat);
				if (rc == MDS_OK) {
					break;
				}
				MDS_LOG_WARN(LOG_COMP_MDS,
					"bootstrap returned %d, "
					"another MDS may be "
					"bootstrapping -- retrying "
					"probe...", (int)rc);
			} else {
				MDS_LOG_INFO(LOG_COMP_MDS,
					"probe retry %d/%d...",
					attempt,
					bootstrap_max_retries);
			}
			sleep_seconds(2);
		}
		if (attempt >= bootstrap_max_retries) {
			MDS_LOG_FATAL(LOG_COMP_MDS,
				"catalogue schema not ready "
				"after %d attempts",
				bootstrap_max_retries);
			if (s_rmf != NULL) {
				remove_manifest_destroy(s_rmf);
				s_rmf = NULL;
			}
			if (s_pt != NULL) {
				(void)parent_touch_flush_all_dirty(s_pt);
				parent_touch_destroy(s_pt);
				s_pt = NULL;
			}
			mds_catalogue_close(cat);
			return EXIT_FAILURE;
		}
	}

	/* 2c. Boot epoch for this daemon instance (see declaration):
	 * wall-clock nanoseconds, monotonic across restarts as long as the
	 * clock is not stepped back below the previous incarnation. */
	{
		struct timespec boot_ts;
		clock_gettime(CLOCK_REALTIME, &boot_ts);
		mds_boot_epoch =
			(uint64_t)boot_ts.tv_sec * 1000000000ULL +
			(uint64_t)boot_ts.tv_nsec;
	}

	/* 2d. Cluster services (Phase 9A): register this incarnation in
	 * the node registry.  The heartbeat thread that keeps the row
	 * fresh is started in step 4c, AFTER the subtree map and
	 * membership it refreshes exist; node_register has already
	 * stamped the row's heartbeat timestamp, so peers see this node
	 * as live from here on -- and step 4c must follow within
	 * CLUSTER_STARTUP_DEADLINE_MS or the row goes stale while we are
	 * still initialising.
	 *
	 * MDS_ERR_EXISTS is fatal: a row with an equal or higher
	 * boot_epoch means another live daemon carries this mds_id (or
	 * this host's clock stepped back past the dead incarnation's
	 * epoch).  Continuing unregistered would let the standby promote
	 * against a node that is about to serve -- split-brain -- so the
	 * daemon exits with the conflicting row in the log. */
	if (mds_cluster_supported(cat)) {
		rc = mds_cluster_node_register(cat, cfg.self.id,
					       mds_boot_epoch,
					       cfg.self.hostname,
					       cfg.self.nfs_port,
					       cfg.self.grpc_port);
		if (rc == MDS_ERR_EXISTS) {
			log_registry_conflict(cat, cfg.self.id, mds_boot_epoch);
			mds_catalogue_close(cat);
			return EXIT_FAILURE;
		}
		if (rc != MDS_OK) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"node registry register failed: %d",
				(int)rc);
		} else {
			struct timespec reg_ts;

			clock_gettime(CLOCK_MONOTONIC, &reg_ts);
			cluster_register_mono_ns =
				(uint64_t)reg_ts.tv_sec * 1000000000ULL +
				(uint64_t)reg_ts.tv_nsec;
			MDS_LOG_INFO(LOG_COMP_MDS,
				"node %u registered "
				"(boot_epoch=%llu)",
				(unsigned)cfg.self.id,
				(unsigned long long)mds_boot_epoch);
		}
	}

	/* 2e. Catalog image (Phase 9C): the image is fed by the backend's
	 * changefeed.  It is a read-side optimisation, so a backend without
	 * a feed is not an error -- the daemon logs once and serves every
	 * read from the authority. */
	if (cfg.catalog_image_mode != MDS_IMAGE_OFF) {
		if (!mds_catalogue_image_feed_supported(cat)) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"catalog_image_mode=%d needs a catalogue "
				"backend with a changefeed; backend %d has "
				"none -- running authority-only",
				(int)cfg.catalog_image_mode,
				(int)mds_catalogue_backend_type(cat));
		} else if (catalog_image_create(&image) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"catalog_image_create failed");
			image = NULL;
		} else {
			rc = mds_catalogue_image_feed_start(cat, image,
							    cfg.self.id, 50);
			if (rc != MDS_OK) {
				MDS_LOG_WARN(LOG_COMP_MDS,
					"changefeed poller start failed: %d",
					(int)rc);
				catalog_image_destroy(image);
				image = NULL;
			} else {
				MDS_LOG_INFO(LOG_COMP_MDS,
					"changefeed poller "
					"started (image_mode=%d)",
					(int)cfg.catalog_image_mode);
			}
		}
	}

	/* 2f. Pre-atomic releases could leave a PENDING wide-create inode
	 * after persisting its namespace rows separately from its stripe map.
	 * The scan repairs those legacy rows before requests are accepted,
	 * but walks the entire namespace from the root, so it is opt-in
	 * (run it once after upgrading from an affected release).  Lazy
	 * lookup-time recovery is always active regardless, so leaving the
	 * scan off only defers reclaiming rows nobody looks up.  A scan
	 * failure is non-fatal for the same reason. */
	if (cfg.hpc_pending_recovery_scan) {
		struct hpc_pending_recovery_stats pending_stats;

		rc = hpc_shared_recover_pending_scan(cat, &pending_stats);
		if (rc != MDS_OK) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"legacy HPC pending recovery scan failed: %d",
				(int)rc);
		} else if (pending_stats.promoted != 0 ||
			   pending_stats.reaped != 0) {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"legacy HPC pending recovery: promoted=%llu "
				"reaped=%llu",
				(unsigned long long)pending_stats.promoted,
				(unsigned long long)pending_stats.reaped);
		}
	}

	/* Create cluster TLS contexts if configured.
	 * Server context for the inbound listener; client context
	 * for replication outbound connections. */
	struct mds_tls_ctx *cluster_tls = NULL;
	struct mds_tls_ctx *cluster_tls_client = NULL;
	if (cfg.cluster_ca_file[0] != '\0') {
		if (mds_tls_ctx_create(cfg.cluster_ca_file,
				cfg.node_cert_file,
				cfg.node_key_file,
				true, cfg.require_mtls,
				&cluster_tls) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"cluster TLS init failed");
		}

		/* Client context for outbound connections (replication, etcd). */
		if (mds_tls_ctx_create(cfg.cluster_ca_file,
				cfg.node_cert_file,
				cfg.node_key_file,
				false, false,
				&cluster_tls_client) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"cluster TLS client init failed");
		}
	}


	/* 3a. Start replication health monitor.
	 *     repl is NULL -- RonDB has native multi-node visibility;
	 *     health_monitor_init(NULL, ...) = no standby monitoring. */
	if (health_monitor_init(NULL, cfg.repl_health_interval_ms,
				cfg.repl_refuse_writes_on_resync,
				&hm) != 0) {
		MDS_LOG_WARN(LOG_COMP_MDS, "health_monitor_init failed");
		hm = NULL;
	}
	if (hm != NULL && health_monitor_start(hm) != 0) {
		MDS_LOG_WARN(LOG_COMP_MDS, "health_monitor_start failed");
		health_monitor_destroy(hm);
		hm = NULL;
	}

	/* 4. Initialise cluster subsystem (subtree map + membership).
	 * With cluster services the map is loaded from the store's
	 * partition map and the membership from its node registry, all
	 * through the backend-neutral mds_cluster_* dispatchers. */
	if (mds_cluster_supported(cat)) {
		rc = subtree_map_init_from_catalogue(cat, cfg.self.id,
						     cfg.self.hostname, &smap);
		if (rc != MDS_OK) {
			MDS_LOG_ERROR(LOG_COMP_MDS,
				"subtree_map_init_from_catalogue failed: %d",
				(int)rc);
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}
		/* Membership: local mode, then populate from the node
		 * registry so all live peers are visible to
		 * transport/failover. */
		rc = cluster_membership_init(&cfg, smap, NULL,
					     &membership);
		if (rc != MDS_OK) {
			MDS_LOG_ERROR(LOG_COMP_MDS,
				"cluster_membership_init failed: %d",
				(int)rc);
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}
		/* Load peers from the node registry. */
		(void)cluster_membership_populate(membership, cat);
		/* 4a-shard. Auto-seed + persist /shardN partition_map rows
		 * when cluster_size > 1 and only the root entry exists.
		 * Must run BEFORE set_membership -- the membership check
		 * would reject remote MDS IDs that haven't joined yet. */
		{
			const char *peers[MDS_MAX_NODES];
			uint32_t pc = cfg.cluster_allowed_peer_count;

			if (pc > MDS_MAX_NODES) {
				pc = MDS_MAX_NODES;
			}
			for (uint32_t pi = 0; pi < pc; pi++) {
				peers[pi] = cfg.cluster_allowed_peers[pi];
			}
			(void)subtree_map_seed_shards(
				smap, cat, cfg.cluster_size,
				peers, pc);
		}

		/* 4a-hosts. Always register MDS hostnames from config
		 * into the subtree map's node table.  Hostnames are
		 * NOT stored in the partition map, so they must be
		 * populated on every startup for referral_build() to
		 * resolve fs_locations server addresses. */
		for (uint32_t hi = 0; hi < cfg.cluster_allowed_peer_count;
		     hi++) {
			uint32_t mds_id = hi + 1;
			subtree_map_register_node(smap, mds_id,
				cfg.cluster_allowed_peers[hi]);
		}

		subtree_map_set_membership(smap, membership);

		/* 4a-junction-roots.  Resolve each partition subtree path
		 * to its root-directory fileid so FH-based ancestry walks
		 * (referral_strict MOVED enforcement) can recognise a
		 * junction from a raw PUTFH with no path.  A subtree whose
		 * directory does not exist yet resolves on the next
		 * restart; until then enforcement for it is inert
		 * (fail-open). */
		{
			uint32_t sm_count = subtree_map_count(smap);

			for (uint32_t si = 0; si < sm_count; si++) {
				struct subtree_entry se;
				char pbuf[MDS_MAX_PATH];
				const char *comp;
				char *save = NULL;
				uint64_t fid = MDS_FILEID_ROOT;
				bool resolved = true;

				if (subtree_map_get_entry(smap, si,
							  &se) != MDS_OK) {
					continue;
				}
				if (se.path[0] == '/' &&
				    se.path[1] == '\0') {
					continue;
				}
				(void)snprintf(pbuf, sizeof(pbuf), "%s",
					       se.path);
				for (comp = strtok_r(pbuf, "/", &save);
				     comp != NULL;
				     comp = strtok_r(NULL, "/", &save)) {
					struct mds_inode child;

					if (mds_cat_ns_lookup(cat, fid, comp,
							      &child)
					    != MDS_OK) {
						resolved = false;
						break;
					}
					fid = child.fileid;
				}
				if (resolved) {
					(void)subtree_map_set_root_fileid(
						smap, se.path, fid);
					MDS_LOG_INFO(LOG_COMP_MDS,
						"junction root %s -> "
						"fileid %llu", se.path,
						(unsigned long long)fid);
				} else {
					MDS_LOG_WARN(LOG_COMP_MDS,
						"junction root %s not "
						"resolved (directory "
						"missing?)", se.path);
				}
			}
		}

		/* 4c. Heartbeat thread (5s interval; each thread gets its
		 * own backend connection from the backend's per-thread
		 * pool).  Created only now, after smap and membership are
		 * stored in its argument block: the previous arrangement
		 * created the thread right after node_register and wrote
		 * arg->smap later from the main thread through a plain
		 * pointer -- a data race.  Liveness: node_register (step
		 * 2d) already wrote the row with a fresh heartbeat
		 * timestamp, and the watchdog's stale threshold (15 s) is
		 * three heartbeat intervals; the work between the two steps
		 * (partition-map load, registry scan, junction-root
		 * lookups) is a handful of bounded catalogue reads.  The
		 * opt-in legacy recovery scan (step 2f) is the one step in
		 * between whose duration depends on namespace size.
		 *
		 * Startup deadline: that gap is bounded by
		 * CLUSTER_STARTUP_DEADLINE_MS (failover_watchdog.h, below the
		 * stale threshold).  A node that overran it may already have
		 * been declared dead by its standby, which then owns its
		 * subtrees; starting to heartbeat and serve now would be
		 * split-brain, so the daemon exits instead (cleanup
		 * deregisters the row). */
		if (cluster_register_mono_ns != 0) {
			struct timespec now_ts;
			uint64_t now_ns;
			uint64_t elapsed_ms;

			clock_gettime(CLOCK_MONOTONIC, &now_ts);
			now_ns = (uint64_t)now_ts.tv_sec * 1000000000ULL +
				 (uint64_t)now_ts.tv_nsec;
			elapsed_ms = (now_ns - cluster_register_mono_ns) /
				     1000000ULL;
			if (elapsed_ms > CLUSTER_STARTUP_DEADLINE_MS) {
				MDS_LOG_FATAL(LOG_COMP_MDS,
					"startup took %llu ms between "
					"node_register and the heartbeat "
					"start, over the %u ms deadline: the "
					"registry row may already be stale "
					"and the standby promoted; exiting "
					"rather than serving into a "
					"split-brain",
					(unsigned long long)elapsed_ms,
					(unsigned)CLUSTER_STARTUP_DEADLINE_MS);
				exit_code = EXIT_FAILURE;
				goto cleanup;
			}
		}
		cluster_hb_arg_g.cat = cat;
		cluster_hb_arg_g.mds_id = cfg.self.id;
		cluster_hb_arg_g.boot_epoch = mds_boot_epoch;
		cluster_hb_arg_g.running = &cluster_hb_flag;
		cluster_hb_arg_g.smap = smap;
		cluster_hb_arg_g.membership = membership;
		if (pthread_create(&cluster_hb_thread, NULL,
				   cluster_hb_fn, &cluster_hb_arg_g) == 0) {
			cluster_hb_running = true;
			MDS_LOG_INFO(LOG_COMP_MDS,
				"cluster heartbeat thread started "
				"(5s interval)");
		} else {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"cluster heartbeat thread start failed");
		}
	}

	/* 4-local. Backends without multi-process cluster services (step
	 * 2a already enforced cluster_size == 1 for them): a local
	 * single-node subtree map ("/" owned by self) and a membership
	 * table holding only this node -- the same setup the unit tests
	 * use.  Every consumer (referrals, transport admin handlers,
	 * split evaluator) then sees a one-node cluster instead of a
	 * NULL map. */
	if (smap == NULL) {
		rc = subtree_map_init(NULL, NULL, cfg.self.id,
				      cfg.self.hostname, NULL, &smap);
		if (rc != MDS_OK) {
			MDS_LOG_ERROR(LOG_COMP_MDS,
				"subtree_map_init failed: %d", (int)rc);
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}
		rc = cluster_membership_init(&cfg, smap, NULL, &membership);
		if (rc != MDS_OK) {
			MDS_LOG_ERROR(LOG_COMP_MDS,
				"cluster_membership_init failed: %d",
				(int)rc);
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}
		subtree_map_set_membership(smap, membership);
		MDS_LOG_INFO(LOG_COMP_MDS,
			"single-node subtree map (backend %d has no "
			"multi-process cluster services)",
			(int)mds_catalogue_backend_type(cat));
	}

	/* 4b. Seed DS registry from config if catalogue is empty. */
	{
		struct mds_ds_info *chk_list = NULL;
		uint32_t chk_cnt = 0;
		enum mds_status chk_st;
		chk_st = mds_cat_ds_list(cat, &chk_list, &chk_cnt);
		free(chk_list);
		if ((chk_st == MDS_OK && chk_cnt == 0) ||
		    chk_st == MDS_ERR_NOTFOUND) {
			for (uint32_t di = 0; di < cfg.ds_count; di++) {
				if (cfg.ds_specs[di][0] == '\0') {
					continue;
				}
				struct mds_ds_info info;
				memset(&info, 0, sizeof(info));
				info.ds_id = di;
				info.state = DS_ONLINE;
				info.transport = DS_TRANSPORT_TCP;
				info.mode = DS_MODE_GENERIC;
				info.port = 2049;
				info.tcp_port = 2049;
				info.total_bytes = (uint64_t)100 * 1024 * 1024 * 1024;
				/* Parse "host:/export" from ds_specs. */
				const char *colon = strchr(cfg.ds_specs[di], ':');
				if (colon != NULL) {
					size_t hlen = (size_t)(colon - cfg.ds_specs[di]);
					if (hlen >= sizeof(info.host)) {
						hlen = sizeof(info.host) - 1;
					}
					memcpy(info.host, cfg.ds_specs[di], hlen);
					info.host[hlen] = '\0';
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
					(void)snprintf(info.export_path,
						sizeof(info.export_path),
						"%s", colon + 1);
					(void)snprintf(info.addr,
						sizeof(info.addr),
						"%s", cfg.ds_specs[di]);
				} else {
					(void)snprintf(info.host,
						sizeof(info.host),
						"%s", cfg.ds_specs[di]);
				}
#pragma GCC diagnostic pop
				if (mds_cat_ds_put(cat, NULL, &info) == MDS_OK) {
					MDS_LOG_INFO(LOG_COMP_MDS,
						"seeded DS %u: %s",
						di, cfg.ds_specs[di]);
				}
			}
		}
	}

	/* 4b1. Migrate DS entries with missing legacy port.
	 *      Earlier seeds set tcp_port but not port.  DS health
	 *      monitor uses port for NFS NULL probe.  Fix in-place.
	 *
	 *      The OFFLINE -> ONLINE reset that follows is gated on
	 *      port_was_zero so it only fires when the cause of the
	 *      offline state (port=0 -> every probe failed) was just
	 *      patched in this iteration.  Without that gate we would
	 *      revive permanently-dead DSes (e.g. hosts that were
	 *      removed from the lab) on every restart, causing
	 *      ds_health to advertise them as ONLINE for ~30s before
	 *      its threshold trips again.  During that window the
	 *      placement and prealloc paths still pick the dead DS,
	 *      producing layouts whose flexfiles writeback never
	 *      completes and wedging the NFSv4 client in an unbounded
	 *      LAYOUTGET retry loop. */
	{
		struct mds_ds_info *fix_list = NULL;
		uint32_t fix_cnt = 0;

		if (mds_cat_ds_list(cat, &fix_list, &fix_cnt) == MDS_OK) {
			for (uint32_t fi = 0; fi < fix_cnt; fi++) {
				bool changed = false;
				bool port_was_zero = false;
				if (fix_list[fi].port == 0 &&
				    fix_list[fi].tcp_port != 0) {
					fix_list[fi].port = fix_list[fi].tcp_port;
					port_was_zero = true;
					changed = true;
				}
				/* Only revive OFFLINE state when we just
				 * fixed the cause (port=0).  Permanently-
				 * offline DSes must stay offline across
				 * restarts; ds_health will probe them back
				 * to ONLINE once they actually recover. */
				if (port_was_zero &&
				    fix_list[fi].state == DS_OFFLINE) {
					fix_list[fi].state = DS_ONLINE;
					changed = true;
				}
				if (changed) {
					(void)mds_cat_ds_put(cat, NULL,
							     &fix_list[fi]);
					MDS_LOG_INFO(LOG_COMP_MDS,
						"fixed DS %u "
						"(port=%u, state=%s)",
						(unsigned)fix_list[fi].ds_id,
						(unsigned)fix_list[fi].port,
						(fix_list[fi].state ==
						 DS_ONLINE) ? "ONLINE"
							     : "OFFLINE");
				}
			}
			free(fix_list);
		}
	}
	/* 4c-fix. Remove stale '/' junction dirent created by a
	 *         prior bug in junction name extraction (the root
	 *         partition entry '/' was not skipped). */
	{
		uint64_t bad_fid = 0;
		uint8_t  bad_type = 0;
		if (mds_cat_dirent_get(cat, MDS_FILEID_ROOT, "/",
				       &bad_fid, &bad_type) == MDS_OK) {
			(void)mds_cat_ns_remove(cat, NULL,
						MDS_FILEID_ROOT, "/");
			MDS_LOG_INFO(LOG_COMP_MDS,
				"removed stale '/' junction "
				"(fid=%llu)",
				(unsigned long long)bad_fid);
		}
	}

	/* 4c. Reconcile junction directories from subtree map.
	 *     For each remote subtree, ensure a junction dir exists
	 *     in the local namespace so referrals work. */
	if (smap != NULL) {
		uint32_t jn = subtree_map_count(smap);
		for (uint32_t ji = 0; ji < jn; ji++) {
			struct subtree_entry je;
			if (subtree_map_get_entry(smap, ji, &je) != MDS_OK) {
				continue;
			}
			if (je.owner_mds_id == cfg.self.id) {
				continue;
			}
			/* Extract junction name from path (last component). */
			const char *slash = strrchr(je.path, '/');
			const char *jname = (slash && slash[1]) ? slash + 1 : je.path;
			if (jname[0] == '\0' || jname[0] == '/') {
				continue;
			}
			enum mds_status jst = referral_ensure_junction(
				cat, MDS_FILEID_ROOT, jname,
				je.owner_mds_id);
			if (jst == MDS_OK) {
				MDS_LOG_INFO(LOG_COMP_MDS,
					"ensured junction /%s -> MDS %u",
					jname, (unsigned)je.owner_mds_id);
			} else {
				MDS_LOG_WARN(LOG_COMP_MDS,
					"failed to create junction /%s: %d",
					jname, (int)jst);
			}
		}
	}

	/* 5. Create proxy I/O context and mount all online data servers. */
	{
		enum mds_status pst = mds_proxy_ctx_create(&proxy);
		if (pst != MDS_OK) {
			MDS_LOG_ERROR(LOG_COMP_MDS, "mds_proxy_ctx_create failed: %d",
				(int)pst);
			proxy = NULL;
		} else {
			struct mds_ds_info *ds_list = NULL;
			uint32_t ds_cnt = 0;

			/* DS FH validation policy for the FH-capture fast
			 * path: opaque by default (RFC 8435), knfsd-only
			 * when the operator pins ds_fh_format = knfsd. */
			mds_proxy_set_fh_knfsd_strict(
				proxy, cfg.ds_fh_knfsd_strict);

			pst = mds_cat_ds_list(cat, &ds_list, &ds_cnt);
			if (pst == MDS_OK) {
				for (uint32_t di = 0; di < ds_cnt; di++) {
					if (ds_list[di].state != DS_ONLINE) {
						continue;
}
					char mpath[256];
/* ds_mount_path_fmt is validated in config.c to contain
					 * exactly one %u and no other specifiers. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
					(void)snprintf(mpath, sizeof(mpath),
						 cfg.ds_mount_path_fmt,
						 ds_list[di].ds_id);
#pragma GCC diagnostic pop
					mds_proxy_mount_set(proxy,
							    ds_list[di].ds_id,
							    mpath);
					/* Populate DS network info for NFS3 RPC
					 * FH capture.  Prefer catalogue fields,
					 * fall back to parsing config ds_specs. */
					{
						const char *h = ds_list[di].host;
						uint16_t p = ds_list[di].port;
						const char *e = ds_list[di].export_path;

						/* Fallback: parse from config if catalogue
						 * fields are empty (legacy rows). */
						char fb_h[MDS_DS_HOST_MAX] = "";
						char fb_e[MDS_DS_EXPORT_MAX] = "";
						if ((h[0] == '\0' || e[0] == '\0') &&
						    di < cfg.ds_count &&
						    cfg.ds_specs[di][0] != '\0') {
							const char *c = strchr(
								cfg.ds_specs[di], ':');
							if (c != NULL) {
								size_t hl = (size_t)
									(c - cfg.ds_specs[di]);
								if (hl >= sizeof(fb_h)) {
									hl = sizeof(fb_h) - 1;
								}
								memcpy(fb_h,
								       cfg.ds_specs[di], hl);
								fb_h[hl] = '\0';
								(void)snprintf(fb_e,
									sizeof(fb_e),
									"%s", c + 1);
								if (h[0] == '\0') {
									h = fb_h;
								}
								if (e[0] == '\0') {
									e = fb_e;
								}
							}
						}
						if (p == 0) {
							p = 2049;
						}
						mds_proxy_mount_set_ds_info(
							proxy, ds_list[di].ds_id,
							h, p, e);
					}
				}
				free(ds_list);
			}
		}
	}

	/* 5a. Init DS pre-alloc pool when DSes are configured.
	 *
	 *      The chosen placement_policy is plumbed into prealloc so
	 *      its pop path delegates DS selection to the same
	 *      placement_select_ex() helper used elsewhere in the
	 *      daemon -- keeping fresh-CREATE placement coherent with
	 *      the LAYOUTGET fallback path.  When the operator-facing
	 *      placement_policy_enabled flag is off we still pin
	 *      PLACEMENT_RR so prealloc's RR is fair across DSes. */
	if (cfg.ds_count > 0 && proxy != NULL) {
		enum mds_placement_policy effective_policy =
			cfg.placement_policy_enabled
				? cfg.placement_policy
				: PLACEMENT_RR;
		if (ds_prealloc_init_ex2(cat, proxy, effective_policy,
					 cfg.prealloc_pool_size,
					 cfg.self.id, cfg.cluster_size,
					 cfg.prealloc_ring_count,
					 &ds_pa) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"ds_prealloc_init failed, "
				"falling back to inline");
			ds_pa = NULL;
		} else {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"DS pre-alloc pool active "
				"(pool_size=%u, rings=%u, policy=%d, "
				"mds=%u/%u)",
				(unsigned)cfg.prealloc_pool_size,
				(unsigned)cfg.prealloc_ring_count,
				(int)effective_policy,
				(unsigned)cfg.self.id,
				(unsigned)cfg.cluster_size);
		}
	}

	/*
	 * Apply runtime defaults that don't belong to any specific
	 * subsystem object: CB recall timeout and dir-deleg recall
	 * timeout.  Both are safe to call at any time and take effect
	 * for the next RPC that asks for the default (timeout_ms=0).
	 */
	nfs4_cb_pending_reply_init();
	nfs4_cb_set_default_timeout(cfg.cb_recall_timeout_ms);
	dir_deleg_set_default_timeout(cfg.dir_deleg_recall_timeout_ms);

	/* 5a2. Init DS registry in-memory cache. */
	if (ds_cache_create(cat, &ds_cache) != 0) {
		MDS_LOG_WARN(LOG_COMP_MDS,
			"ds_cache_create failed, "
			"falling back to direct reads");
		ds_cache = NULL;
	}

	/*
	 * Stamp operator-configured WRR weights onto the cached DS
	 * entries.  Runtime-only (never persisted) so every MDS start
	 * re-applies its own config.  A ds_weight of 0 leaves the DS
	 * at the free-bytes fallback path inside placement_select_ex.
	 */
	if (ds_cache != NULL) {
		ds_cache_apply_weights(ds_cache, cfg.ds_weight_by_id);
	}

	/*
	 * Plumb the DS cache into the prealloc pool so its pop
	 * snapshot picks up live capacity (statvfs probe) and
	 * operator/auto weights -- without this, WRR / CAPACITY policies
	 * collapse to uniform RR for prealloc-backed CREATE because
	 * RonDB's persisted DS registry has zeroed total/used bytes on
	 * 3rd-party DSes.  Done after ds_cache_apply_weights so the
	 * first refresh already sees operator config; ds_capacity_start
	 * below will then feed live numbers on subsequent ticks.
	 */
	if (ds_pa != NULL && ds_cache != NULL) {
		ds_prealloc_set_ds_cache(ds_pa, ds_cache);
	}

	/* v8: RFC 8435 S2.2 stored synthetic DS owner (ds_synth_owner). */
	if (ds_pa != NULL) {
		ds_prealloc_set_synth_owner(ds_pa, cfg.ds_synth_owner);
		if (cfg.ds_synth_owner) {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"ds_synth_owner=on (stored synthetic DS owner; "
				"no per-LAYOUTGET chown)");
		}
	}

	/*
	 * Start the live DS capacity probe.  Uses statvfs() on the
	 * mount path that proxy I/O set up above, so it only produces
	 * live capacity when proxy is enabled.  When disabled (no
	 * proxy, ds_capacity_poll_ms=0, or cache absent) operators
	 * can still drive WRR via ds_weight.<id>.
	 */
	if (ds_cache != NULL && cfg.ds_capacity_poll_ms > 0) {
		if (ds_capacity_start(ds_cache, cat, cfg.self.id,
				      cfg.ds_mount_path_fmt,
				      cfg.ds_capacity_poll_ms,
				      cfg.placement_capacity_weighting,
				      &ds_cap) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"ds_capacity_start failed; "
				"falling back to admin weights / uniform");
			ds_cap = NULL;
		} else if (ds_cap != NULL) {
			const char *mode_name =
				(cfg.placement_capacity_weighting ==
				 CAP_WEIGHT_PROPORTIONAL)
					? "proportional"
					: "off";
			MDS_LOG_INFO(LOG_COMP_MDS,
				"DS capacity probe active "
				"(poll=%u ms, mount_fmt=%s, "
				"capacity_weighting=%s)",
				(unsigned)cfg.ds_capacity_poll_ms,
				cfg.ds_mount_path_fmt, mode_name);
		}
	}

	/* 5b. Init DS async-prepare queue for generic DSes (Phase 6A).
	 *     Only when DSes exist and proxy I/O is available. */
	if (cfg.ds_count > 0 && proxy != NULL) {
		if (ds_prepare_create(cat, proxy, cfg.ds_count,
				      cfg.ds_prepare_queue_depth,
				      &ds_prep) == 0) {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"DS async-prepare queue active");
		} else {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"ds_prepare_create failed");
			ds_prep = NULL;
		}
	}

	/* 5c. Init DS GC drainer.
	 *
	 *      Reclaims DS-side data files for inodes that the
	 *      catalogue layer has already dropped (compound
	 *      op_remove enqueues here on the final unlink).
	 *      Disabled when proxy I/O is not available because the
	 *      worker needs DS mount points to issue unlinks.  The
	 *      poll interval is intentionally fixed at 5s for the
	 *      first deployment -- small enough that a removed file
	 *      reclaims its DS bytes within the lab's smoke timing,
	 *      large enough that NDB peek traffic stays below the
	 *      noise floor on the catalogue. */
	if (cfg.ds_count > 0 && proxy != NULL && cat != NULL) {
		if (ds_gc_start_ex(cat, proxy, 5000U,
				   cfg.ds_gc_workers,
				   cfg.ds_gc_batch_size,
				   &ds_gc) == 0 &&
		    ds_gc != NULL) {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"DS GC drainer active "
				"(poll=5000 ms, workers=%u, batch=%u)",
				(unsigned)cfg.ds_gc_workers,
				(unsigned)cfg.ds_gc_batch_size);
		} else if (ds_gc == NULL) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"ds_gc_start failed; "
				"DS-side cleanup will not run");
		}
	}

	/* 6. Start inter-MDS cluster transport listener.
	 *    Serves rename-2PC, migration, DS admin, and metrics requests. */
	{
		rc = cluster_transport_server_start(cfg.self.grpc_port,
						    cfg.cluster_bind_addr,
						    (const char (*)[64])cfg.cluster_allowed_peers,
						    cfg.cluster_allowed_peer_count,
						    cfg.cluster_max_conns,
						    cat, smap, cluster_tls, &ct_srv);
		if (rc != MDS_OK) {
			MDS_LOG_ERROR(LOG_COMP_MDS,
				"cluster_transport_server_start failed: %d",
				(int)rc);
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}

		/* Wire membership into transport for admin RPCs. */
		cluster_transport_server_set_membership(ct_srv, membership);
		cluster_transport_server_set_sharding(ct_srv, cfg.shard_enabled);
	}

	/* 6.1 Wire failover for standby nodes (Seq 9). */
	{
		struct cluster_member self_m;
		if (cluster_membership_get(membership, cfg.self.id,
					  &self_m) == MDS_OK &&
		    self_m.role == NODE_STANDBY &&
		    self_m.failover_partner_id != 0) {

			struct failover_cfg fo_cfg;
			memset(&fo_cfg, 0, sizeof(fo_cfg));
			fo_cfg.self_id          = cfg.self.id;
			fo_cfg.partner_id       = self_m.failover_partner_id;
			fo_cfg.map              = smap;
			fo_cfg.cat              = cat;
			fo_cfg.grace_period_sec = cfg.grace_period_sec;
			fo_cfg.detect_cb        = NULL;  /* authoritative mode */
			fo_cfg.membership       = membership;
			fo_cfg.hm               = hm;

			enum mds_status fo_st = failover_init(&fo_cfg, &fo_ctx);
			if (fo_st != MDS_OK) {
				MDS_LOG_WARN(LOG_COMP_MDS,
					"failover_init failed: %d",
					(int)fo_st);
				fo_ctx = NULL;
			} else {
				cluster_membership_set_partner_loss_cb(
					membership,
					self_m.failover_partner_id,
					failover_on_partner_loss,
					fo_ctx);
				(void)fprintf(stdout,
					"Standby failover armed: "
					"partner=%u\n",
					(unsigned)self_m.failover_partner_id);
				/* Partner-liveness watchdog: polls the node
				 * registry's heartbeat timestamps through
				 * mds_cluster_node_scan_stale and fires
				 * failover_promote when the partner misses its
				 * heartbeats.  Replaces the old LMDB-delta-
				 * shipping signal that was removed with the
				 * writer thread.  A backend without the stale
				 * scan refuses with NOSUPPORT; that is not an
				 * error, just an unarmed watchdog. */
				{
					struct failover_watchdog_cfg wd_cfg;
					enum mds_status wd_st;

					memset(&wd_cfg, 0, sizeof(wd_cfg));
					wd_cfg.fo         = fo_ctx;
					wd_cfg.cat        = cat;
					wd_cfg.partner_id = self_m.failover_partner_id;
					wd_st = failover_watchdog_start(&wd_cfg,
									&fo_wd);
					if (wd_st == MDS_OK) {
						MDS_LOG_INFO(LOG_COMP_MDS,
							"failover watchdog "
							"active (partner=%u)",
							(unsigned)self_m.failover_partner_id);
					} else if (wd_st == MDS_ERR_NOSUPPORT) {
						MDS_LOG_INFO(LOG_COMP_MDS,
							"failover watchdog not armed: "
							"backend %d has no stale-node "
							"scan (partner=%u)",
							(int)mds_catalogue_backend_type(cat),
							(unsigned)self_m.failover_partner_id);
						fo_wd = NULL;
					} else {
						MDS_LOG_WARN(LOG_COMP_MDS,
							"failover_watchdog_start "
							"failed: %d (partner=%u)",
							(int)wd_st,
							(unsigned)self_m.failover_partner_id);
						fo_wd = NULL;
					}
				}
			}
		}
	}

	/* 6a. Commit queue -- RonDB does not use CQ.
	 *     Mutations go directly to NDB. */
	cq = NULL;

	/* 6b. Create session table.
	 *     For RonDB the catalogue vtable handles recovery
	 *     persistence directly (cq stays NULL). */
	if (session_table_init_ex(cfg.self.id, cfg.lease_time_sec,
				  cfg.session_client_buckets,
				  cfg.session_session_buckets,
				  cfg.session_owner_buckets,
				  &session_tbl) == 0) {

		session_table_set_cat(session_tbl, cat);
		if (cfg.session_fore_slots > 0) {
			session_table_set_max_fore_slots(
				session_tbl, cfg.session_fore_slots);
		}
		if (cq != NULL) {
			session_table_set_cq(session_tbl, cq);
		}
	} else {
		MDS_LOG_WARN(LOG_COMP_MDS, "session_table_init failed");
	}

	/* 6c. Layout recall coordinator + DS health monitor.
	 *     Both lr and ds_hm are bound to the default shard here,
	 *     before ds_health_start() launches a thread, to avoid
	 *     racing on hm->cq (and for consistency, lr too). */
	if (layout_recall_init(cat, cq, 0, &lr) == 0) {
		if (shard_map != NULL) {
			const struct mds_shard *def =
				mds_shard_map_get_default(shard_map);
			if (def != NULL) {
				layout_recall_set_shard(lr, def);
			}
		}
		/* Wire the session table so the recall coordinator can
		 * snapshot holders' backchannels for byte-range
		 * CB_LAYOUTRECALL on op_layoutget conflict (Mark's bug)
		 * and the existing DS-failure / admin paths.  Without
		 * this binding the recall coordinator falls back to
		 * authoritative-revoke-only mode. */
		layout_recall_set_session_table(lr, session_tbl);
		/* RFC 8881 S20.3: a CB_LAYOUTRECALL filehandle must be
		 * byte-identical to the one GETFH gave the client, which on
		 * an MDS with a non-zero id is the v1 form carrying this
		 * id.  Without this the coordinator can only emit the
		 * legacy 8-byte handle and clients cannot match the
		 * recall. */
		layout_recall_set_mds_id(lr, cfg.self.id);
		/* RFC 8435 §14: proxy context for DS-side fencing
		 * on layout revocation.  Without this, revocation
		 * is callback-only. */
		layout_recall_set_proxy(lr, proxy);
		if (cfg.ds_heartbeat_ms == 0) {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"DS health monitoring disabled "
				"(ds_heartbeat_ms=0)");
		} else if (ds_health_init(cat, cq, cfg.ds_heartbeat_ms,
					  cfg.ds_health_fail_threshold,
					  ds_fail_recall_cb, lr, &ds_hm) == 0) {
			if (shard_map != NULL) {
				const struct mds_shard *def =
					mds_shard_map_get_default(shard_map);
				if (def != NULL) {
					ds_health_set_shard(ds_hm, def);
				}
			}
			ds_health_start(ds_hm);
		} else {
			MDS_LOG_WARN(LOG_COMP_MDS, "ds_health_init failed");
		}
	} else {
		MDS_LOG_WARN(LOG_COMP_MDS, "layout_recall_init failed");
	}

	/* 6c2. Per-DS I/O limit prober (Wave 5 T5.1).  Probes each
	 *      ONLINE generic DS with NFSv3 FSINFO so the wire
	 *      advertisements (GETDEVICEINFO ffdv_rsize/wsize,
	 *      MAXREAD/MAXWRITE) reflect what the DS actually accepts.
	 *      Disabled (0) keeps the legacy 1 MiB constants. */
	if (cfg.ds_iolimit_probe_ms > 0 && cat != NULL) {
		(void)ds_io_limits_enable();
		ds_io_limits_set_recall(lr);
		if (ds_io_limits_start(cat, cfg.ds_iolimit_probe_ms) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"ds_io_limits_start failed; "
				"DS I/O limits stay at the unverified "
				"fallback until probing is restored");
		} else {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"DS I/O limit probe active (poll=%u ms)",
				(unsigned)cfg.ds_iolimit_probe_ms);
		}
	}

	/* 6d. Open state table (shared with resilver for writer fencing).
	 * Failure takes the common cleanup path like every other fatal
	 * step: the subsystems created above (DS cache and capacity
	 * probe, cluster transport, session table, DS health monitor)
	 * are torn down there, and lock_tbl is still NULL so its own
	 * destroy is a no-op. */
	if (lock_table_init(cfg.self.id, &lock_tbl) != 0) {
		MDS_LOG_ERROR(LOG_COMP_MDS, "lock_table_init failed");
		exit_code = EXIT_FAILURE;
		goto cleanup;
	}

	if (open_state_table_init_ex(cfg.self.id,
				     cfg.open_state_file_buckets,
				     cfg.open_state_stateid_buckets,
				     cfg.open_state_lock_stripes,
				     &ot) != 0) {
		MDS_LOG_WARN(LOG_COMP_MDS, "open_state_table_init failed");
		ot = NULL;
	} else {
		/* Chain depth is workload-dependent -- log the effective
		 * sizing so operators can correlate it with measurements
		 * instead of trusting arithmetic (Wave 4 T4.2). */
		MDS_LOG_INFO(LOG_COMP_MDS,
			"open-state table: file_buckets=%u "
			"stateid_buckets=%u lock_stripes=%u",
			cfg.open_state_file_buckets != 0
				? cfg.open_state_file_buckets
				: (unsigned)OPEN_STATE_DEFAULT_FILE_BUCKETS,
			cfg.open_state_stateid_buckets != 0
				? cfg.open_state_stateid_buckets
				: (unsigned)OPEN_STATE_DEFAULT_STATEID_BUCKETS,
			cfg.open_state_lock_stripes != 0
				? cfg.open_state_lock_stripes
				: (unsigned)OPEN_STATE_DEFAULT_LOCK_STRIPES);
	}

	/* shared-attr: wire the catalogue into the stateful subsystems
	 * so open/lock/deleg mutations are persisted to the shared
	 * protocol-state tables -- when the backend has them.  The
	 * tables treat MDS_ERR_NOSUPPORT from a slot as "nothing to
	 * persist", so this gate only avoids the pointless call. */
	if (mds_coord_shared_state_supported(cat)) {
		if (ot != NULL) {
			open_state_table_set_cat(ot, cat, mds_boot_epoch);
			if (cfg.transient_state_cache) {
				open_state_table_set_skip_ndb(ot, true);
				MDS_LOG_INFO(LOG_COMP_MDS,
					"transient_state_cache=on "
					"(open/layout backend writes skipped)");
			}
		}
		if (lock_tbl != NULL) {
			lock_table_set_cat(lock_tbl, cat, mds_boot_epoch);
		}
		MDS_LOG_INFO(LOG_COMP_MDS,
			"shared protocol state active "
			"(open+lock write-through to the catalogue)");
	}

	/* 6e. Resilver worker (admin-triggered, not auto-started). */
	if (resilver_init(cat, cq, proxy, ot, &rw) != 0) {
		MDS_LOG_WARN(LOG_COMP_MDS, "resilver_init failed");
		rw = NULL;
	}

	/* 6f. Rebalance worker (admin-triggered, not auto-started). */
	if (rebalance_init(cat, cq, proxy, ot, &rbw) != 0) { /* catalogue escape hatch */
		MDS_LOG_WARN(LOG_COMP_MDS, "rebalance_init failed");
		rbw = NULL;
	}

	/* 6g. I/O tracker for tiering heat signal. */
	if (io_tracker_init(4096, &iot) != 0) {
		MDS_LOG_WARN(LOG_COMP_MDS, "io_tracker_init failed");
		iot = NULL;
	}

	/* 6i. Quota enforcement context. */
	(void)mds_quota_ctx_create(cat, &quota); /* catalogue escape hatch */

	/* 6h. Tiering worker (admin-triggered, not auto-started). */
	if (iot != NULL) {
		if (tiering_init(cat, cq, proxy, ot, iot, &tw) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS, "tiering_init failed");
			tw = NULL;
		}
	}

	/* 6j. Copy offload table for async COPY operations. */
	(void)copy_offload_create(&cot);

	/* 6k. Start automatic split evaluator (Tier 3 Phase 1). */
	if (cfg.auto_split_enabled && smap != NULL) {
		struct split_eval_cfg se_cfg;
		memset(&se_cfg, 0, sizeof(se_cfg));
		se_cfg.auto_split_enabled = true;
		se_cfg.auto_execute       = cfg.auto_split_execute;
		se_cfg.split_threshold    = cfg.auto_split_threshold;
		se_cfg.eval_interval_sec  = cfg.auto_split_interval;
		se_cfg.cooldown_sec       = cfg.auto_split_cooldown;
		se_cfg.sustained_intervals = cfg.auto_split_sustained;
		se_cfg.min_children       = cfg.auto_split_min_children;
		if (split_evaluator_start(smap, cat, NULL, &se_cfg,
					  &split_eval) == 0 &&
		    split_eval != NULL) {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"split evaluator active "
				"(threshold=%lu, interval=%us, "
				"auto_exec=%s)",
				(unsigned long)cfg.auto_split_threshold,
				(unsigned)cfg.auto_split_interval,
				cfg.auto_split_execute ? "yes" : "no");
		} else {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"split_evaluator_start failed");
			split_eval = NULL;
		}
	}

	/* 6l. Bind subsystems to default shard explicitly (Phase 2).
	 *     Makes the daemon-global db/cq assumption visible.  The
	 *     existing init-time set_db/set_cq calls remain for backward
	 *     compatibility with tests that don't use shard_map. */
	if (shard_map != NULL) {
		const struct mds_shard *def =
			mds_shard_map_get_default(shard_map);
		if (def != NULL) {
			if (session_tbl != NULL) {
				session_table_set_shard(session_tbl, def);
			}
			/* lr and ds_hm already bound in step 6c, before
			 * ds_health_start() launches the poll thread. */
			if (rw != NULL) {
				resilver_set_shard(rw, def);
			}
			if (rbw != NULL) {
				rebalance_set_shard(rbw, def);
			}
			if (tw != NULL) {
				tiering_set_shard(tw, def);
			}
		}
	}

	/* 7. Enter grace period.
	 *    NFSv4.1 requires a grace period after startup during which
	 *    only reclaim operations are accepted (RFC 8881 sec 8.4.2.1).
	 *    grace_is_active() is checked by dispatch_op() on every
	 *    compound request.  Must precede RPC listener start. */
	if (cfg.grace_period_sec > 0) {
		grace_enter(cfg.grace_period_sec);
		(void)fprintf(stdout, "Entering grace period (%u sec)\n",
			cfg.grace_period_sec);
	}


	/* 9. Create NFS RPC listener(s).
	 *    Build config with ALL subsystem handles so that every
	 *    compound_data field is populated. */
	{
		struct rpc_server_config rpc_cfg;
		memset(&rpc_cfg, 0, sizeof(rpc_cfg));
		rpc_cfg.port       = cfg.self.nfs_port;
		rpc_cfg.mds_id     = cfg.self.id;
		rpc_cfg.stripe_unit = cfg.stripe_unit_bytes;
		rpc_cfg.ds_getdev_transport = cfg.ds_getdev_transport;
		rpc_cfg.ds_rdma_port = cfg.ds_rdma_port;
		rpc_cfg.auto_widen_lease_on_4k = cfg.auto_widen_lease_on_4k;
		compound_layout_set_grant_max_length(
			cfg.layout_grant_max_length_bytes);
		rpc_cfg.placement_policy = cfg.placement_policy;
		rpc_cfg.placement_policy_enabled = cfg.placement_policy_enabled;
		rpc_cfg.default_stripe_count = cfg.default_stripe_count;
		rpc_cfg.default_mirror_count = cfg.default_mirror_count;
		rpc_cfg.hpc_getattr_mode = cfg.hpc_getattr_mode;
		rpc_cfg.hpc_max_stripe_count = cfg.hpc_max_stripe_count;
		rpc_cfg.hpc_xdr_form = cfg.hpc_xdr_form;
	rpc_cfg.hpc_serve_layouts = cfg.hpc_serve_layouts;
	rpc_cfg.serve_layouts = cfg.serve_layouts;
	rpc_cfg.layoutget_newfile_fastpath = cfg.layoutget_newfile_fastpath;
		if (cfg.placement_policy_enabled) {
			const char *pname =
				(cfg.placement_policy == PLACEMENT_WEIGHTED_RR)
					? "wrr"
				: (cfg.placement_policy == PLACEMENT_CAPACITY)
					? "capacity"
				: "rr";
			MDS_LOG_INFO(LOG_COMP_MDS,
				"placement_policy=%s (dispatcher "
				"active)", pname);
		} else {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"placement_policy_enabled=false "
				"(legacy RR path)");
		}
		rpc_cfg.write_verf = (uint64_t)time(NULL);
		rpc_cfg.cat        = cat;
		rpc_cfg.st         = session_tbl;
		rpc_cfg.ot         = ot;
		session_table_set_ot(session_tbl, ot);
		session_table_set_lt(session_tbl, lock_tbl);
		rpc_cfg.lt         = lock_tbl;
		rpc_cfg.cq         = cq;
		rpc_cfg.shard_map  = shard_map;
		rpc_cfg.gpudirect_required = cfg.gpudirect_required;
		rpc_cfg.skip_transient_ndb = cfg.transient_state_cache;
		rpc_cfg.hide_referral_junctions = cfg.hide_referral_junctions;
		rpc_cfg.posix_dac = cfg.posix_dac;
		rpc_cfg.referral_strict = cfg.referral_strict;

		/* Kerberos auth: initialize GSS if krb5+ requested. */
		struct mds_gss_table *gss_tbl = NULL;
		if (cfg.nfs_auth_mode > NFS_AUTH_MODE_SYS) {
			if (mds_gss_init(cfg.krb5_keytab_path,
			                 cfg.krb5_principal,
			                 &gss_tbl) != 0) {
				MDS_LOG_FATAL(LOG_COMP_MDS,
					"GSS init failed for "
					"nfs_auth_mode=%d",
					(int)cfg.nfs_auth_mode);
				exit_code = EXIT_FAILURE;
				goto cleanup;
			}
			MDS_LOG_INFO(LOG_COMP_MDS,
				"nfs_auth_mode=%d, GSS ready",
				(int)cfg.nfs_auth_mode);
		}
		rpc_cfg.min_auth = cfg.nfs_auth_mode;
		rpc_cfg.gss_tbl = gss_tbl;

	/* Create worker pool for COMPOUND dispatch (3.3).
	 * worker_threads == 0 or 1 -> inline processing (tp = NULL). */
	if (cfg.worker_threads > 1) {
		if (threadpool_create(cfg.worker_threads, &rpc_tp) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"threadpool_create(%u) failed, "
				"falling back to inline dispatch",
				(unsigned)cfg.worker_threads);
			rpc_tp = NULL;
		}
	}
	rpc_cfg.tp = rpc_tp;
	rpc_cfg.max_inflight_per_conn = cfg.max_inflight_per_conn;
	/* Publish the pool to the metrics renderer so /metrics
	 * exports pnfs_mds_rpc_worker_* / pnfs_mds_rpc_queue_*
	 * gauges and the queue-wait histogram. */
	mds_metrics_set_rpc_threadpool(rpc_tp);
		rpc_cfg.ds_hm      = ds_hm;
		rpc_cfg.smap       = smap;
		rpc_cfg.membership = membership;
		rpc_cfg.proxy      = proxy;
		rpc_cfg.hm         = hm;
		rpc_cfg.io_tracker = iot;
		rpc_cfg.quota      = quota;
		rpc_cfg.prealloc   = ds_pa;
		rpc_cfg.ds_prepare = ds_prep;
		rpc_cfg.ds_cache   = ds_cache;
		rpc_cfg.cot        = cot;
		rpc_cfg.transport  = NULL; /* rename 2PC on-demand per compound */

		/* Cross-MDS cache coherence (active-active): the per-daemon
		 * dirent/inode caches are NOT invalidated by mutations on a
		 * peer MDS -- RonDB stays authoritative, but the in-memory
		 * caches in front of it do not.  Bound the staleness window
		 * with a positive-entry TTL so a peer's delete+recreate (new
		 * monotonic fileid) cannot be shadowed by a stale
		 * name->fileid / inode entry for longer than the TTL.
		 * Single-MDS keeps the historical behaviour (positive entries
		 * unbounded, negative 5s): one shared cache is already coherent
		 * via mutation-time invalidation.  Operator config wins. */
		uint32_t cache_pos_ttl_ms = cfg.positive_cache_ttl_ms;
		uint32_t cache_neg_ttl_ms = cfg.negative_cache_ttl_ms;
		if (cfg.cluster_size > 1) {
			if (cache_pos_ttl_ms == 0) {
				cache_pos_ttl_ms = 1000;
			}
			if (cache_neg_ttl_ms == 0) {
				cache_neg_ttl_ms = 1000;
			}
		}

		/* Global inode cache (cross-compound LRU).  Disabled when
		 * inode_cache_size=0 (the default).  Operators who want
		 * the previous lab default set inode_cache_size=16384. */
		{
			struct inode_cache *icache = NULL;

			if (cfg.inode_cache_size > 0) {
				if (inode_cache_init(cfg.inode_cache_size,
						     &icache) == 0) {
					inode_cache_set_ttl_ms(icache,
							       cache_pos_ttl_ms);
					MDS_LOG_INFO(LOG_COMP_MDS,
						"inode cache active "
						"(max=%u entries, ttl=%ums)",
						(unsigned)cfg.inode_cache_size,
						(unsigned)cache_pos_ttl_ms);
				} else {
					MDS_LOG_WARN(LOG_COMP_MDS,
						"inode_cache_init failed");
				}
			} else {
				MDS_LOG_INFO(LOG_COMP_MDS,
					"inode cache disabled "
					"(inode_cache_size=0)");
			}
			rpc_cfg.icache = icache;
			if (ct_srv != NULL) {
				cluster_transport_server_set_inode_cache(ct_srv, icache);
			}

			/* parent_touch deferred parent-dir attr aggregator (ported).
			 * Gated on the config key AND a backend capability probe; init /
			 * start failure is non-fatal: rpc_cfg.pt stays NULL and every
			 * mutation keeps the synchronous in-transaction parent update. */
			rpc_cfg.pt = NULL;
			if (cfg.parent_touch_deferred) {
				if (!mds_cat_ns_parent_touch_supported(cat)) {
					(void)fprintf(stderr,
						"WARN: parent_touch_deferred=true but the "
						"catalogue backend does not implement "
						"ns_parent_touch; feature disabled\n");
				} else if (parent_touch_init(cfg.parent_touch_max_dirs,
							     cfg.parent_touch_flush_ms,
							     &s_pt) != 0) {
					(void)fprintf(stderr,
						"WARN: parent_touch_init failed; keeping "
						"synchronous parent updates\n");
					s_pt = NULL;
				} else {
					s_pt_flush_ctx.cat = cat;
					s_pt_flush_ctx.icache = icache;
					parent_touch_set_flush_fn(s_pt, pt_flush_cb,
								  &s_pt_flush_ctx);
					if (parent_touch_start(s_pt) != 0) {
						(void)fprintf(stderr,
							"WARN: parent_touch_start failed; "
							"tearing down aggregator\n");
						parent_touch_destroy(s_pt);
						s_pt = NULL;
					} else {
						(void)fprintf(stderr,
							"INFO: parent_touch deferred "
							"aggregator active (dirs=%u, "
							"flush=%ums)\n",
							(unsigned)cfg.parent_touch_max_dirs,
							(unsigned)cfg.parent_touch_flush_ms);
					}
				}
				rpc_cfg.pt = s_pt;
			}


			/* Pre-warm cache with root inode -- eliminates
			 * 1 NDB RT from every PUTROOTFH compound. */
			if (icache != NULL && cat != NULL) {
				struct mds_inode root;
				if (mds_cat_ns_getattr(cat, MDS_FILEID_ROOT,
						       &root) == MDS_OK) {
					inode_cache_put(icache, &root);
				}
			}
		}

		/* Global dirent cache (cross-compound LRU + negative entries). */
		{
			struct dirent_cache *dcache = NULL;
			uint32_t dcache_size = cfg.dirent_cache_size;
			uint32_t neg_ttl = cache_neg_ttl_ms > 0
				? cache_neg_ttl_ms : 5000;
			if (dcache_size > 0 && dirent_cache_init(dcache_size, neg_ttl,
					      &dcache) == 0) {
				dirent_cache_set_pos_ttl_ms(dcache,
							    cache_pos_ttl_ms);
				MDS_LOG_INFO(LOG_COMP_MDS,
					"dirent cache active "
					"(max=%u, neg_ttl=%ums, pos_ttl=%ums)",
					(unsigned)dcache_size,
					(unsigned)neg_ttl,
					(unsigned)cache_pos_ttl_ms);
			} else {
				MDS_LOG_INFO(LOG_COMP_MDS,
					cfg.dirent_cache_size
					? "dirent_cache_init failed"
					: "dirent cache disabled "
					  "(dirent_cache_size=0)");
			}
		rpc_cfg.dcache = dcache;
		}

		/* Layout recall coordinator -- used by op_layoutget for
		 * byte-range conflict-recall (Mark's bug) and reused by
		 * the existing DS-failure / admin paths.  NULL-safe in
		 * compound_data; op_layoutget skips the conflict scan. */
		rpc_cfg.lr = lr;

		/* HPC-Shared layout cache (Phase D of
		 * docs/hpc-nto1-plan.md).  Populated only for inodes with
		 * MDS_IFLAG_HPC_SHARED.  init failure is non-fatal: we
		 * leave rpc_cfg.lcache NULL and the LAYOUTGET path
		 * transparently falls back to the catalogue read -- this
		 * matches the pre-Phase-D behaviour. */
		{
			struct layout_cache *lcache = NULL;
			uint32_t lcache_size = cfg.layout_cache_size;
			if (lcache_size > 0 && layout_cache_init(lcache_size, &lcache) == 0) {
				MDS_LOG_INFO(LOG_COMP_MDS,
					"HPC layout cache active "
					"(max=%u entries)",
					(unsigned)lcache_size);
			} else {
				MDS_LOG_INFO(LOG_COMP_MDS,
					cfg.layout_cache_size
					? "layout_cache_init failed; falling back "
					  "to catalogue reads"
					: "HPC layout cache disabled "
					  "(layout_cache_size=0)");
			}
			rpc_cfg.lcache = lcache;
			if (ct_srv != NULL) {
				cluster_transport_server_set_layout_cache(ct_srv, lcache);
			}
		}

		/* HPC-Shared LAYOUTCOMMIT aggregator (Phase F of
		 * docs/hpc-nto1-plan.md, integration part A).  v1
		 * scope: create the aggregator, wire the flush callback,
		 * start the timer thread.  No callers submit yet -- op_*
		 * handlers continue to take the synchronous LAYOUTCOMMIT
		 * path until integration part B lands.  Init failure is
		 * non-fatal: rpc_cfg.lcommit_agg stays NULL and the
		 * synchronous path runs unchanged. */
		{
			uint32_t agg_size = cfg.layout_commit_aggregator_size;
			uint32_t agg_flush_ms =
				cfg.layout_commit_aggregator_flush_ms;
			if (layout_commit_aggregator_init(agg_size,
							  agg_flush_ms,
							  &lcommit_agg) == 0) {
				s_lcommit_flush_ctx.cat = cat;
				s_lcommit_flush_ctx.icache = rpc_cfg.icache;
				layout_commit_aggregator_set_flush_fn(
					lcommit_agg, lcommit_flush_cb,
					&s_lcommit_flush_ctx);
				if (layout_commit_aggregator_start(
						lcommit_agg) != 0) {
					MDS_LOG_WARN(LOG_COMP_MDS,
						"layout_commit_aggregator_"
						"start failed; tearing down "
						"aggregator");
					layout_commit_aggregator_destroy(
						lcommit_agg);
					lcommit_agg = NULL;
				} else {
					MDS_LOG_INFO(LOG_COMP_MDS,
						"HPC layout_commit "
						"aggregator active "
						"(buckets=%u, flush=%ums)",
						(unsigned)(agg_size != 0 ?
							agg_size :
							LCA_DEFAULT_MAX_BUCKETS),
						(unsigned)(agg_flush_ms != 0 ?
							agg_flush_ms :
							LCA_DEFAULT_FLUSH_INTERVAL_MS));
				}
			} else {
				MDS_LOG_WARN(LOG_COMP_MDS,
					"layout_commit_aggregator_init "
					"failed; falling back to synchronous "
					"LAYOUTCOMMIT");
			}
			rpc_cfg.lcommit_agg = lcommit_agg;
		}

		/* Delegation state table (RFC 8881 S10.4).
		 *
		 * Gated by cfg.file_delegations_enabled (default false).  When
		 * false, we skip the table init entirely and leave
		 * rpc_cfg.dt == NULL so cd->dt == NULL in compound_data;
		 * op_open's deleg-grant arm short-circuits at the
		 * `if (cd->dt != NULL)` guard in compound_data_io.c, so the
		 * server never grants a file delegation and never has to
		 * issue CB_RECALL.  Operators reach for this when the kernel
		 * client cannot honour delegation hints (e.g. Linux v4.1+
		 * does not translate `clientaddr=0.0.0.0` into
		 * OPEN4_SHARE_ACCESS_WANT_NO_DELEG \u2014 see Mark's two-client
		 * harness report). */
		rpc_cfg.dt = NULL;
		if (cfg.file_delegations_enabled) {
			struct deleg_table *dt = NULL;
			if (deleg_table_init(cfg.self.id, &dt) == 0) {
				MDS_LOG_INFO(LOG_COMP_MDS,
					"delegation table active");
				if (mds_coord_shared_state_supported(cat)) {
					deleg_table_set_cat(dt, cat,
							    mds_boot_epoch);
				}
				/* Wire the session table so deleg_recall_file()
				 * can snapshot the holder's backchannel and
				 * issue CB_RECALL on a dup'd fd.  Without this,
				 * the recall path silently revokes the
				 * delegation without notifying the client \u2014
				 * legacy pre-fix behaviour, retained when
				 * session_tbl == NULL. */
				deleg_table_set_session_table(dt, session_tbl);
				/* Transient-state profile keeps delegations
				 * in memory only \u2014 removes the NDB write
				 * from the OPEN hot path. */
				if (cfg.transient_state_cache) {
					deleg_table_set_skip_transient(dt, true);
				}
				rpc_cfg.dt = dt;
			} else {
				MDS_LOG_WARN(LOG_COMP_MDS,
					"deleg_table_init failed");
			}
		} else {
			MDS_LOG_INFO(LOG_COMP_MDS,
				"file_delegations_enabled=false "
				"(deleg table not wired; OPEN never grants"
				" file delegations)");
		}

		/* Directory delegation table (RFC 8881 S10.9). */
		{
			struct dir_deleg_table *ddt = NULL;

			rpc_cfg.ddt = NULL;
			if (cfg.dir_delegations_enabled) {
				if (dir_deleg_table_init(cfg.self.id,
							 &ddt) == 0) {
					MDS_LOG_INFO(LOG_COMP_MDS,
						"directory delegation "
						"table active");
					/* Lets recall / notify read a
					 * directory inode's generation so
					 * the CB filehandle matches GETFH
					 * (RFC 8881 S20.2 / S20.4). */
					dir_deleg_table_set_cat(ddt, cat);
					dir_deleg_table_set_session_table(
						ddt, session_tbl);
					rpc_cfg.ddt = ddt;
				} else {
					MDS_LOG_WARN(LOG_COMP_MDS,
						"dir_deleg_table_init "
						"failed");
				}
			}
		}




		/* Async-REMOVE delete manifest (ported; delete-at-ack is the
		 * ONLY mode in this port: the ack transaction removes the
		 * dirent and flags the inode, so name hiding is
		 * DB-authoritative on every MDS via RonDB synchronous
		 * replication).  Gated on the config key AND an active
		 * parent_touch aggregator (REMOVE change_info is served from
		 * it).  The startup load completes before the RPC listeners
		 * serve. */
		rpc_cfg.rmf = NULL;
		if (cfg.remove_async && cat != NULL) {
			if (s_pt == NULL) {
				(void)fprintf(stderr,
					"WARN: remove_async=true requires an "
					"active parent_touch aggregator "
					"(parent_touch_deferred=true); "
					"keeping the synchronous REMOVE "
					"path\n");
			} else if (remove_manifest_init(cat, rpc_cfg.proxy,
					rpc_cfg.lcache, rpc_cfg.lcommit_agg,
					rpc_cfg.quota, cfg.self.id,
					mds_boot_epoch, 65536U,
					cfg.remove_async_workers,
					cfg.remove_async_batch,
					cfg.remove_async_poll_ms,
					(uint64_t)cfg.remove_async_claim_ttl_ms
						* 1000000ULL,
					&s_rmf) != 0) {
				(void)fprintf(stderr,
					"WARN: remove_manifest_init failed; "
					"keeping the synchronous REMOVE "
					"path\n");
				s_rmf = NULL;
			} else if (remove_manifest_load(s_rmf) != 0 ||
				   remove_manifest_start(s_rmf) != 0) {
				(void)fprintf(stderr,
					"WARN: remove_manifest load/start "
					"failed; keeping the synchronous "
					"REMOVE path\n");
				remove_manifest_destroy(s_rmf);
				s_rmf = NULL;
			} else {
				(void)fprintf(stderr,
					"INFO: async-REMOVE delete manifest "
					"active (delete-at-ack; workers=%u, "
					"batch=%u); name hiding is "
					"DB-authoritative on every MDS\n",
					(unsigned)cfg.remove_async_workers,
					(unsigned)cfg.remove_async_batch);
			}
			rpc_cfg.rmf = s_rmf;
		}

		if (rpc_server_create(&rpc_cfg, &rpc_srv[0]) != 0) {
			MDS_LOG_ERROR(LOG_COMP_MDS, "rpc_server_create failed");
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}
		rpc_listener_count = 1;
		bound_port = rpc_server_port(rpc_srv[0]);

		/* 9a. Multi-listener: if worker_threads > 1 and SO_REUSEPORT
		 *     is available, create additional listeners on the same
		 *     port.  Kernel distributes connections across listeners,
		 *     each with its own epoll loop in its own thread.
		 *     This eliminates the single-epoll I/O bottleneck. */
#ifdef SO_REUSEPORT
		if (cfg.worker_threads > 1) {
			uint32_t target;

			if (cfg.rpc_listener_threads > 0) {
				/* Explicit operator count (already bounded
				 * to 1..32 by the parser).  Clamp to online
				 * CPUs so a copy-pasted config cannot
				 * oversubscribe a small node with idle epoll
				 * loops. */
				long ncpu = sysconf(_SC_NPROCESSORS_ONLN);

				target = cfg.rpc_listener_threads;
				if (ncpu > 0 && target > (uint32_t)ncpu) {
					MDS_LOG_WARN(LOG_COMP_MDS,
						"rpc_listener_threads=%u "
						"exceeds online CPUs (%ld); "
						"using %ld",
						(unsigned)target, ncpu, ncpu);
					target = (uint32_t)ncpu;
				}
			} else {
				/* Auto (key absent or 0): the historical
				 * rule min(worker_threads, 4).  The auto
				 * value stays unchanged until a bandwidth
				 * sweep (nconnect=8/16) measures where more
				 * listeners pay off. */
				target = cfg.worker_threads;
				if (target > 4) {
					target = 4;
				}
			}
			if (target > MAX_RPC_LISTENERS) {
				target = MAX_RPC_LISTENERS;
			}
			rpc_cfg.port = rpc_server_port(rpc_srv[0]);
			for (uint32_t li = 1; li < target; li++) {
				if (rpc_server_create(&rpc_cfg, &rpc_srv[li]) != 0) {
					MDS_LOG_WARN(LOG_COMP_MDS,
						"RPC listener %u/%u bind failed, "
						"using %u listener(s)",
						(unsigned)(li + 1), (unsigned)target,
						(unsigned)li);
					break;
				}
				rpc_listener_count = li + 1;
			}
			MDS_LOG_INFO(LOG_COMP_MDS,
				"%u RPC listener(s) on port %u",
				(unsigned)rpc_listener_count,
				(unsigned)bound_port);
		}
#endif

		/* Launch listener threads. */
		started_count = 0;
		for (uint32_t li = 0; li < rpc_listener_count; li++) {
			if (pthread_create(&rpc_threads[li], NULL,
					   rpc_listener_thread,
					   rpc_srv[li]) != 0) {
				MDS_LOG_WARN(LOG_COMP_MDS,
					"pthread_create for listener %u "
					"failed", (unsigned)li);
				rpc_server_destroy(rpc_srv[li]);
				rpc_srv[li] = NULL;
			} else {
				started_count++;
			}
		}

		if (started_count == 0) {
			MDS_LOG_FATAL(LOG_COMP_MDS,
				"no RPC listener threads started");
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}
	}


	/* Wire commit queue and proxy into transport for DS admin RPCs. */
	if (ct_srv != NULL) {
		cluster_transport_server_set_cq(ct_srv, cq);
		cluster_transport_server_set_proxy(ct_srv, proxy,
			cfg.ds_mount_path_fmt);
		cluster_transport_server_set_ds_cache(ct_srv, ds_cache);
	}
	/* Register resilver worker with cluster transport for admin RPCs. */
	if (ct_srv != NULL && split_eval != NULL) {
		cluster_transport_server_set_evaluator(ct_srv, split_eval);
	}
	if (ct_srv != NULL && rw != NULL) {
		cluster_transport_server_set_resilver(ct_srv, rw);
	}
	if (ct_srv != NULL && rbw != NULL) {
		cluster_transport_server_set_rebalance(ct_srv, rbw);
	}
	if (ct_srv != NULL && tw != NULL) {
		cluster_transport_server_set_tiering(ct_srv, tw);
	}
	if (ct_srv != NULL && quota != NULL) {
		cluster_transport_server_set_quota(ct_srv, quota);
	}
	if (ct_srv != NULL) {
		/* C2: attach the live config so the admin-introspection
		 * CONFIG_SHOW handler can render key=value pairs. */
		cluster_transport_server_set_config(ct_srv, &cfg);
	}

	/* 9b. Prometheus metrics HTTP endpoint.
	 * Port 0 disables the endpoint; non-zero binds 0.0.0.0:<port>.
	 * Configurable via `metrics_http_port` INI key (default 9090).
	 * The handle is kept so shutdown can stop+join the scrape
	 * thread BEFORE the RPC threadpool it reads from is destroyed
	 * (see the metrics_http_stop call in cleanup). */
	if (cfg.metrics_http_port > 0) {
		if (metrics_http_start(cfg.metrics_http_port, cat,
				       &mhttp) != 0) {
			mhttp = NULL;
		}
	}

	/* 9c. `showmount -e` compatibility responder.
	 * Synthesizes ONC-RPC program 100005 v3 NULL/EXPORT/DUMP
	 * replies on a separate UDP+TCP listener.  Never proxies to
	 * any DS; rejects MNT/UMNT with PROC_UNAVAIL so accidental
	 * NFSv3 mounts of the MDS are impossible.  Enabled by default
	 * (binds UDP+TCP 20048 + registers with rpcbind); operators
	 * who want the shim off set `mountd_compat_enabled = false`
	 * in mds.conf.  See docs/mountd-compat.md. */
	if (cfg.mountd_compat_enabled) {
		if (mountd_compat_start(&cfg, &mountd_compat) != 0) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"mountd_compat_start failed; "
				"showmount -e will not work");
			mountd_compat = NULL;
		}
	}

	(void)fprintf(stdout, "pnfs-mds node %u started on %s:%u "
		"(%u RPC listener(s), cluster transport port %u)%s%s\n",
		cfg.self.id, cfg.self.hostname,
		(unsigned)bound_port,
		(unsigned)started_count,
		(unsigned)cluster_transport_server_port(ct_srv),
		"",
		"");
	/* stdout is a pipe under systemd and therefore fully buffered:
	 * without this flush the banner (and the grace / failover lines
	 * above it) reach the journal only at exit, i.e. never while the
	 * daemon is being watched for its listener to come up. */
	(void)fflush(stdout);

	/* 10. Wait for shutdown signal via sigwait().
	 *     SIGINT/SIGTERM were blocked before thread creation;
	 *     sigwait() atomically dequeues the pending signal. */
	{
		int caught_sig = 0;
		(void)sigwait(&shutdown_set, &caught_sig);
	}
	/* A SIGTERM raised by the heartbeat thread on supersession is a
	 * failure exit, not an operator stop. */
	if (atomic_load(&cluster_superseded)) {
		exit_code = EXIT_FAILURE;
	}

	/* Orderly shutdown -- ordering matters:
	 * 1. Stop RPC servers + join threads (no new COMPOUNDs)
	 * 2. Destroy copy offload (drain in-flight async copies)
	 * 3. Stop cluster transport (no in-flight rename 2PC)
	 * 4. Destroy remaining subsystems in reverse-init order */
cleanup:
	if (exit_code == EXIT_SUCCESS) {
		(void)fprintf(stdout, "pnfs-mds shutting down.\n");
	} else if (atomic_load(&cluster_superseded)) {
		MDS_LOG_ERROR(LOG_COMP_MDS,
			"pnfs-mds superseded by a newer incarnation, "
			"cleaning up.");
	} else {
		MDS_LOG_ERROR(LOG_COMP_MDS, "pnfs-mds startup failed, cleaning up.");
	}

	/* Phase 1: stop RPC listeners. */
	for (uint32_t li = 0; li < rpc_listener_count; li++) {
		if (rpc_srv[li] != NULL) {
			rpc_server_stop(rpc_srv[li]);
		}
	}

	/* Stop the showmount-compat responder early -- it has no
	 * dependency on the catalogue or any other subsystem, but
	 * unregistering from rpcbind here keeps `showmount -e` from
	 * returning a stale port while the rest of cleanup runs. */
	mountd_compat_stop(mountd_compat);
	mountd_compat = NULL;
	for (uint32_t li = 0; li < rpc_listener_count; li++) {
		if (rpc_srv[li] != NULL) {
			(void)pthread_join(rpc_threads[li], NULL);
		}
	}

	/* Stop+join the metrics HTTP scrape thread BEFORE destroying
	 * the RPC threadpool: a concurrent /metrics render may have
	 * loaded the registered pool pointer and still be calling
	 * threadpool_get_stats on it.  The NULL store below only stops
	 * NEW scrapes from seeing the pool -- it does not make the
	 * destroy safe against an in-flight render (atomicity is not
	 * lifetime).  Joining the listener first does. */
	metrics_http_stop(mhttp);
	mhttp = NULL;

	/* Drain the RPC worker pool AFTER epoll loops have exited
	 * but BEFORE destroying any rpc_server.  Workers hold raw
	 * pointers to rpc_server / rpc_conn -- the pool must be
	 * fully joined before that memory is freed. */
	mds_metrics_set_rpc_threadpool(NULL);
	threadpool_destroy(rpc_tp);
	rpc_tp = NULL;

	for (uint32_t li = 0; li < rpc_listener_count; li++) {
		if (rpc_srv[li] != NULL) {
			rpc_server_destroy(rpc_srv[li]);
		}
	}

	/* Phase 2: drain async copies (no COMPOUND can start new ones). */
	copy_offload_destroy(cot);

	/* Phase 3: stop cluster transport. */
	if (ct_srv != NULL) {
		cluster_transport_server_stop(ct_srv);
	}

	/* Phase 3b: cluster services teardown.  Order is load-bearing and
	 * unchanged from the backend-specific version:
	 *   1. join the heartbeat thread (it dereferences cat, smap and
	 *      membership on every cycle);
	 *   2. stop the changefeed feed, then destroy the image it fed;
	 *   3. deregister this incarnation from the node registry;
	 *   4. (Phase 5 below) close the catalogue last.
	 * The heartbeat thread exists only when mds_cluster_supported(cat)
	 * (step 4c), so cluster_hb_running is the sole guard for 1. */
	if (cluster_hb_running) {
		atomic_store(&cluster_hb_flag, false);
		(void)pthread_join(cluster_hb_thread, NULL);
	}
	if (image != NULL) {
		(void)mds_catalogue_image_feed_stop(cat);
		catalog_image_destroy(image);
		image = NULL;
	}
	if (cat != NULL && mds_cluster_supported(cat)) {
		if (atomic_load(&cluster_superseded)) {
			/* The row belongs to the newer incarnation; the
			 * epoch-guarded delete would be STALE and must not
			 * even be attempted against its registration. */
			MDS_LOG_INFO(LOG_COMP_MDS,
				"node %u superseded; registry row left to "
				"the newer incarnation",
				(unsigned)cfg.self.id);
		} else {
			(void)mds_cluster_node_deregister(cat, cfg.self.id,
							  mds_boot_epoch);
			MDS_LOG_INFO(LOG_COMP_MDS,
				"node %u deregistered",
				(unsigned)cfg.self.id);
		}
	}

	/* Phase 4: remaining subsystems. */
	split_evaluator_stop(split_eval);
	failover_watchdog_stop(fo_wd);
	failover_destroy(fo_ctx);
	tiering_destroy(tw);
	mds_quota_ctx_destroy(quota);
	rebalance_destroy(rbw);
	io_tracker_destroy(iot);
	resilver_destroy(rw);
	/* Stop the GC drainer first so its peek/dequeue traffic
	 * stops before we tear down the catalogue and proxy. */
	ds_gc_stop(ds_gc);
	ds_gc = NULL;
	ds_prepare_destroy(ds_prep);
	ds_prealloc_destroy(ds_pa);
	if (ds_hm != NULL) {
		ds_health_stop(ds_hm);
		ds_health_destroy(ds_hm);
	}
	/* Stop the I/O-limit prober before destroying the recall
	 * coordinator its sweeps borrow.  ds_io_limits_stop joins the
	 * prober thread; no-op when probing was never started. */
	ds_io_limits_stop();
	layout_recall_destroy(lr);
	/* The process-wide layout-seqid tracker is written by the COMPOUND
	 * path (Phase 1), the transport admin handlers (Phase 3), the DS
	 * health monitor and the recall coordinator (both just above); with
	 * all of them down its entries can be released. */
	layout_seqid_table_destroy();
	/* Stop the capacity probe before destroying the DS cache it
	 * writes into.  ds_capacity_stop joins the worker thread. */
	ds_capacity_stop(ds_cap);
	ds_cap = NULL;
	/* Every DS cache reader is down at this point: the RPC workers
	 * (pool joined and servers destroyed in Phase 1), the transport
	 * admin handlers (Phase 3), the prealloc pool and the capacity
	 * probe (both just above).  cq never carried a cache reference in
	 * this daemon (it is NULL, step 6a). */
	ds_cache_destroy(ds_cache);
	ds_cache = NULL;
	session_table_destroy(session_tbl);
	open_state_table_destroy(ot);
	/* The lock table's only readers are the COMPOUND path (Phase 1)
	 * and the session table's client-expiry path, which is gone with
	 * the table destroyed just above. */
	lock_table_destroy(lock_tbl);
	lock_tbl = NULL;
	commit_queue_destroy(cq);
	mds_proxy_ctx_destroy(proxy);
	/* Drain + tear down the LAYOUTCOMMIT aggregator BEFORE the
	 * catalogue is closed.  Stop the timer first so flush_all_dirty
	 * runs single-threaded with no racing periodic ticks; then
	 * flush every dirty bucket through the configured callback so
	 * shutdown is durable for HPC-Shared files; finally destroy
	 * the aggregator.  Safe under NULL when init / start failed. */
	if (lcommit_agg != NULL) {
		layout_commit_aggregator_stop(lcommit_agg);
		(void)layout_commit_aggregator_flush_all_dirty(lcommit_agg);
		layout_commit_aggregator_destroy(lcommit_agg);
		lcommit_agg = NULL;
	}
	/* Stop the ported async-REMOVE / parent_touch machinery while
	 * the catalogue is still open: the drainer joins its workers
	 * and shutdown-flushes remaining tombstones, the aggregator
	 * persists pending parent deltas.  Without this teardown the
	 * threads ran into exit() and deadlocked against catalogue/NDB
	 * teardown until systemd escalated to SIGKILL after 90 s
	 * (stop-sigterm timeout on every armed stop). */
	if (s_rmf != NULL) {
		remove_manifest_destroy(s_rmf);
		s_rmf = NULL;
	}
	if (s_pt != NULL) {
		(void)parent_touch_flush_all_dirty(s_pt);
		parent_touch_destroy(s_pt);
		s_pt = NULL;
	}
	/* Cluster map and membership go last among the subsystems: their
	 * readers were the heartbeat thread (joined in Phase 3b), the
	 * transport listener (Phase 3), the failover context / watchdog
	 * and the split evaluator (Phase 4) and the COMPOUND path
	 * (Phase 1).  The membership holds a pointer to the map, so it is
	 * destroyed first; both are NULL-safe for the early-exit paths
	 * that never created them. */
	cluster_membership_destroy(membership);
	membership = NULL;
	subtree_map_destroy(smap);
	smap = NULL;
	if (cat != NULL) {
		mds_catalogue_close(cat);
		cat = NULL;
	}
	/* Process-wide backend state (e.g. the FoundationDB client network
	 * thread) goes after the LAST handle is closed and before the log
	 * is shut down; no catalogue may be opened after this point. */
	mds_catalogue_process_shutdown();
	mds_tls_ctx_destroy(cluster_tls);
	mds_tls_ctx_destroy(cluster_tls_client);

	/* Flush and close the log file last so shutdown diagnostics above
	 * are durable.  No-op when logging to stderr. */
	mds_log_shutdown();

	return exit_code;
}
