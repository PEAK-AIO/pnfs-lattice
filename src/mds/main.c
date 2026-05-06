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

#include "pnfs_mds.h"
#include "rpc_server.h"
#include "mds_gss.h"
#include "mds_tls.h"
#include "copy_offload.h"
#include "cluster_membership.h"
#include "mds_catalogue.h"
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
#include "failover_rondb.h"
#include "subtree_split.h"
#include "mds_shard.h"
#include "ds_cache.h"
#include "ds_capacity.h"
#include "inode_cache.h"
#include "dirent_cache.h"
#include "layout_cache.h"
#include "layout_commit_aggregator.h"
#include "delegation.h"
#include "dir_delegation.h"
#include "nfs4_cb.h"
#include "metrics_http.h"
#include "mountd_compat.h"
#ifdef HAVE_RONDB
#include "catalogue_rondb.h"
#include "catalog_image.h"
#endif

/** Maximum concurrent RPC listener threads. */
#define MAX_RPC_LISTENERS 32

/** RPC listener thread entry point (blocking). */
static void *rpc_listener_thread(void *arg)
{
	struct rpc_server *srv = arg;

	(void)rpc_server_start(srv);
	return NULL;
}

/* -----------------------------------------------------------------------
 * RonDB heartbeat thread (Phase 9A)
 * ----------------------------------------------------------------------- */

#ifdef HAVE_RONDB
static _Atomic bool rondb_hb_flag = true;

struct rondb_hb_arg {
	struct mds_catalogue *cat;
	uint32_t mds_id;
	uint64_t boot_epoch;
	_Atomic bool *running;
	struct subtree_map *smap;  /**< Refreshed every heartbeat cycle. */
	struct cluster_membership *membership; /**< Peer refresh cycle. */
};
static struct rondb_hb_arg rondb_hb_arg_g;

static void *rondb_hb_fn(void *a)
{
	struct rondb_hb_arg *arg = a;
	uint32_t cycle = 0;

	while (atomic_load(arg->running)) {
		(void)catalogue_rondb_mds_heartbeat(
			arg->cat, arg->mds_id, arg->boot_epoch);

		/* Refresh subtree map + membership every 3rd cycle (~15s)
		 * to pick up ownership and peer changes from other MDS
		 * nodes without flooding RonDB with scans. */
		if ((cycle % 3) == 0) {
			if (arg->smap != NULL) {
				(void)subtree_map_refresh_rondb(
					arg->smap, arg->cat);
			}
			if (arg->membership != NULL) {
				(void)cluster_membership_populate_rondb(
					arg->membership, arg->cat);
			}
		}
		cycle++;
		sleep(5);
	}
	return NULL;
}
#endif

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
	(void)fprintf(stderr,
		"INFO: partner %u (role=%d, lifecycle=%d) lost -- "
		"attempting promotion\n",
		removed->mds_id, (int)removed->role,
		(int)removed->lifecycle);
	enum mds_status st = failover_promote(fo);
	if (st == MDS_OK) {
		(void)fprintf(stderr,
			"INFO: failover promotion succeeded\n");
	} else {
		(void)fprintf(stderr,
			"WARN: failover promotion failed: %d\n", (int)st);
	}
}



/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int main(int argc, char *argv[])
{
	struct mds_config cfg;
	enum mds_status rc;
	const char *config_path = "/etc/pnfs-mds/mds.conf";
#ifdef HAVE_RONDB
	uint64_t rondb_boot_epoch = 0;
	pthread_t rondb_hb_thread;
	bool rondb_hb_running = false;
#endif
	struct health_monitor *hm = NULL;
	struct failover_ctx *fo_ctx = NULL;
#ifdef HAVE_RONDB
	struct failover_watchdog *fo_wd = NULL;
#endif
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
#ifdef HAVE_RONDB
	struct catalog_image *rondb_image = NULL;
#endif
	struct rpc_server *rpc_srv[MAX_RPC_LISTENERS];
	pthread_t rpc_threads[MAX_RPC_LISTENERS];
	uint32_t rpc_listener_count = 0;
	uint16_t bound_port = 0;
	uint32_t started_count = 0;
	struct mountd_compat_ctx *mountd_compat = NULL;

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

	/* 1a. Initialise grace subsystem (must precede any RPC path). */
	grace_init();

	/* 1b. Block SIGINT/SIGTERM before any worker threads.
	 *     All subsequently created threads inherit the blocked mask;
	 *     the main thread uses sigwait() for reliable delivery. */
	sigset_t shutdown_set;
	sigemptyset(&shutdown_set);
	sigaddset(&shutdown_set, SIGINT);
	sigaddset(&shutdown_set, SIGTERM);
	(void)pthread_sigmask(SIG_BLOCK, &shutdown_set, NULL);

	/* 2. Backend-specific validation. */
#ifdef HAVE_RONDB
	if (cfg.catalogue_backend == MDS_BACKEND_RONDB &&
	    cfg.inline_enabled) {
		(void)fprintf(stderr,
			"FATAL: catalogue_backend=rondb requires "
			"inline_enabled=false.\n");
		return EXIT_FAILURE;
	}
#endif

	rc = mds_catalogue_open(&cfg, &cat);
	if (rc != MDS_OK) {
		(void)fprintf(stderr,
			"Failed to open metadata catalogue: %d\n",
			(int)rc);
		return EXIT_FAILURE;
	}

#ifdef HAVE_RONDB
	/* RonDB: auto-bootstrap on first start (probe fails = no tables).
	 * Phase 10A: retry loop guards against concurrent bootstrap race
	 * when multiple MDS instances start simultaneously.  If another
	 * MDS is bootstrapping, the probe will eventually succeed. */
	if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
		{
			const int bootstrap_max_retries = 10;
			int attempt;

			/*
			 * Always invoke bootstrap once on startup.  The DDL is
			 * idempotent (create_table_if_not_exists), and this lets
			 * schema-version upgrade blocks inside
			 * rondb_shim_bootstrap_metadata run on an existing
			 * cluster.  Probe-then-bootstrap-on-failure alone
			 * skipped upgrades because the legacy probe table
			 * always exists on a previously-bootstrapped cluster.
			 */
			rc = mds_rondb_bootstrap(cat);
			if (rc != MDS_OK) {
				(void)fprintf(stderr,
					"INFO: initial bootstrap returned %d, "
					"entering probe-retry loop...\n",
					(int)rc);
			}
			for (attempt = 0; attempt < bootstrap_max_retries;
			     attempt++) {
				if (mds_catalogue_probe(cat) == MDS_OK) {
					break; /* Schema ready. */
				}
				if (attempt == 0) {
					(void)fprintf(stderr,
						"INFO: RonDB probe failed after "
						"initial bootstrap, retrying "
						"bootstrap...\n");
					rc = mds_rondb_bootstrap(cat);
					if (rc == MDS_OK) {
						break;
					}
					(void)fprintf(stderr,
						"WARN: bootstrap returned %d, "
						"another MDS may be "
						"bootstrapping -- retrying "
						"probe...\n", (int)rc);
				} else {
					(void)fprintf(stderr,
						"INFO: probe retry %d/%d...\n",
						attempt,
						bootstrap_max_retries);
				}
				sleep(2);
			}
			if (attempt >= bootstrap_max_retries) {
				(void)fprintf(stderr,
					"FATAL: RonDB schema not ready "
					"after %d attempts\n",
					bootstrap_max_retries);
				mds_catalogue_close(cat);
				return EXIT_FAILURE;
			}
		}

		/* Phase 9A: generate boot_epoch, register in node registry,
		 * start heartbeat thread. */
		{
			struct timespec boot_ts;
			clock_gettime(CLOCK_MONOTONIC, &boot_ts);
			rondb_boot_epoch =
				(uint64_t)boot_ts.tv_sec * 1000000000ULL +
				(uint64_t)boot_ts.tv_nsec;
		}
		rc = catalogue_rondb_mds_register(cat, cfg.self.id,
						  rondb_boot_epoch,
						  cfg.self.hostname,
						  cfg.self.nfs_port,
						  cfg.self.grpc_port);
		if (rc != MDS_OK) {
			(void)fprintf(stderr,
				"WARN: RonDB node registry register failed: %d\n",
				(int)rc);
		} else {
			(void)fprintf(stderr,
				"INFO: RonDB node %u registered "
				"(boot_epoch=%llu)\n",
				(unsigned)cfg.self.id,
				(unsigned long long)rondb_boot_epoch);
		}

		/* Phase 9A: heartbeat thread (5s interval).
		 * Each thread gets its own Ndb via the shim's TLS pool. */
		{
			rondb_hb_arg_g.cat = cat;
			rondb_hb_arg_g.mds_id = cfg.self.id;
			rondb_hb_arg_g.boot_epoch = rondb_boot_epoch;
			rondb_hb_arg_g.running = &rondb_hb_flag;
			rondb_hb_arg_g.smap = NULL; /* set after subtree_map_init */

			if (pthread_create(&rondb_hb_thread, NULL,
					   rondb_hb_fn, &rondb_hb_arg_g) == 0) {
				rondb_hb_running = true;
				(void)fprintf(stderr,
					"INFO: RonDB heartbeat thread "
					"started (5s interval)\n");
			} else {
				(void)fprintf(stderr,
					"WARN: RonDB heartbeat thread "
					"start failed\n");
			}
		}

		/* Phase 9C: start changefeed poller if image mode enabled. */
		if (cfg.catalog_image_mode != MDS_IMAGE_OFF) {
			if (catalog_image_create(&rondb_image) != 0) {
				(void)fprintf(stderr,
					"WARN: catalog_image_create failed\n");
				rondb_image = NULL;
			} else {
				if (catalogue_rondb_poller_start(
					    cat, rondb_image,
					    cfg.self.id, 50) != 0) {
					(void)fprintf(stderr,
						"WARN: changefeed poller "
						"start failed\n");
					catalog_image_destroy(rondb_image);
					rondb_image = NULL;
				} else {
					(void)fprintf(stderr,
						"INFO: changefeed poller "
						"started (image_mode=%d)\n",
						(int)cfg.catalog_image_mode);
				}
			}
		}
	}
#endif


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
			(void)fprintf(stderr,
				"WARN: cluster TLS init failed\n");
		}

		/* Client context for outbound connections (replication, etcd). */
		if (mds_tls_ctx_create(cfg.cluster_ca_file,
				cfg.node_cert_file,
				cfg.node_key_file,
				false, false,
				&cluster_tls_client) != 0) {
			(void)fprintf(stderr,
				"WARN: cluster TLS client init failed\n");
		}
	}


	/* 3a. Start replication health monitor.
	 *     repl is NULL -- RonDB has native multi-node visibility;
	 *     health_monitor_init(NULL, ...) = no standby monitoring. */
	if (health_monitor_init(NULL, cfg.repl_health_interval_ms,
				cfg.repl_refuse_writes_on_resync,
				&hm) != 0) {
		(void)fprintf(stderr, "WARN: health_monitor_init failed\n");
		hm = NULL;
	}
	if (hm != NULL && health_monitor_start(hm) != 0) {
		(void)fprintf(stderr, "WARN: health_monitor_start failed\n");
		health_monitor_destroy(hm);
		hm = NULL;
	}

	/* 4. Initialise cluster subsystem (subtree map + membership). */
#ifdef HAVE_RONDB
	if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
		/* RonDB mode: load subtree map from partition_map table.
		 * No etcd dependency. */
		rc = subtree_map_init_rondb(cat, cfg.self.id,
					   cfg.self.hostname, &smap);
		if (rc != MDS_OK) {
			(void)fprintf(stderr,
				"subtree_map_init_rondb failed: %d\n",
				(int)rc);
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}
		/* Membership: local mode, then populate from node_registry
		 * so all live peers are visible to transport/failover. */
		rc = cluster_membership_init(&cfg, smap, NULL,
					     &membership);
		if (rc != MDS_OK) {
			(void)fprintf(stderr,
				"cluster_membership_init failed: %d\n",
				(int)rc);
			exit_code = EXIT_FAILURE;
			goto cleanup;
		}
		/* Load peers from RonDB node_registry. */
		(void)cluster_membership_populate_rondb(membership, cat);
		/* 4a-shard. Auto-seed partition_map with /shardN entries
		 * when cluster_size > 1 and only the root entry exists.
		 * Each MDS gets its own subtree for referral-based sharding.
		 * Must run BEFORE set_membership -- the membership check
		 * would reject remote MDS IDs that haven't joined yet. */
		if (cfg.cluster_size > 1 &&
		    subtree_map_count(smap) <= 1) {
			for (uint32_t si = 0; si < cfg.cluster_size; si++) {
				char spath[64];
				uint32_t mds_id = si + 1;
				(void)snprintf(spath, sizeof(spath),
					"/shard%u", (unsigned)mds_id);
				char host[64] = "";
				if (si < cfg.cluster_allowed_peer_count) {
					(void)snprintf(host, sizeof(host),
						"%s",
						cfg.cluster_allowed_peers[si]);
				}
				enum mds_status ast = subtree_map_add(
					smap, spath, mds_id, host,
					SUBTREE_ACTIVE, 1);
				if (ast == MDS_OK) {
					(void)fprintf(stderr,
						"INFO: seeded partition "
						"%s -> MDS %u\n",
						spath, (unsigned)mds_id);
				} else if (ast != MDS_ERR_EXISTS) {
					(void)fprintf(stderr,
						"WARN: partition seed "
						"%s failed: %d\n",
						spath, (int)ast);
				}
			}
		}

		/* 4a-hosts. Always register MDS hostnames from config
		 * into the subtree map's node table.  Hostnames are
		 * NOT stored in partition_map (RonDB), so they must be
		 * populated on every startup for referral_build() to
		 * resolve fs_locations server addresses. */
		for (uint32_t hi = 0; hi < cfg.cluster_allowed_peer_count;
		     hi++) {
			uint32_t mds_id = hi + 1;
			subtree_map_register_node(smap, mds_id,
				cfg.cluster_allowed_peers[hi]);
		}

		subtree_map_set_membership(smap, membership);

		/* Wire smap + membership into heartbeat thread for
		 * periodic refresh of subtree ownership and peers. */
		rondb_hb_arg_g.smap = smap;
		rondb_hb_arg_g.membership = membership;
	}
#endif

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
					(void)fprintf(stderr,
						"INFO: seeded DS %u: %s\n",
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
					(void)fprintf(stderr,
						"INFO: fixed DS %u "
						"(port=%u, state=%s)\n",
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
			(void)fprintf(stderr,
				"INFO: removed stale '/' junction "
				"(fid=%llu)\n",
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
			enum mds_status jst = referral_create_junction(
				cat, MDS_FILEID_ROOT, jname,
				je.owner_mds_id);
			if (jst == MDS_OK) {
				(void)fprintf(stderr,
					"INFO: created junction /%s -> MDS %u\n",
					jname, (unsigned)je.owner_mds_id);
			} else if (jst == MDS_ERR_EXISTS) {
				/* Junction already exists -- no action needed. */
			} else {
				(void)fprintf(stderr,
					"WARN: failed to create junction /%s: %d\n",
					jname, (int)jst);
			}
		}
	}

	/* 5. Create proxy I/O context and mount all online data servers. */
	{
		enum mds_status pst = mds_proxy_ctx_create(&proxy);
		if (pst != MDS_OK) {
			(void)fprintf(stderr, "mds_proxy_ctx_create failed: %d\n",
				(int)pst);
			proxy = NULL;
		} else {
			struct mds_ds_info *ds_list = NULL;
			uint32_t ds_cnt = 0;
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
		if (ds_prealloc_init_ex(cat, proxy, effective_policy,
					cfg.prealloc_pool_size,
					&ds_pa) != 0) {
			(void)fprintf(stderr,
				"WARN: ds_prealloc_init failed, "
				"falling back to inline\n");
			ds_pa = NULL;
		} else {
			(void)fprintf(stderr,
				"INFO: DS pre-alloc pool active "
				"(pool_size=%u, policy=%d)\n",
				(unsigned)cfg.prealloc_pool_size,
				(int)effective_policy);
		}
	}

	/*
	 * Apply runtime defaults that don't belong to any specific
	 * subsystem object: CB recall timeout and dir-deleg recall
	 * timeout.  Both are safe to call at any time and take effect
	 * for the next RPC that asks for the default (timeout_ms=0).
	 */
	nfs4_cb_set_default_timeout(cfg.cb_recall_timeout_ms);
	dir_deleg_set_default_timeout(cfg.dir_deleg_recall_timeout_ms);

	/* 5a2. Init DS registry in-memory cache. */
	if (ds_cache_create(cat, &ds_cache) != 0) {
		(void)fprintf(stderr,
			"WARN: ds_cache_create failed, "
			"falling back to direct reads\n");
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
			(void)fprintf(stderr,
				"WARN: ds_capacity_start failed; "
				"falling back to admin weights / uniform\n");
			ds_cap = NULL;
		} else if (ds_cap != NULL) {
			const char *mode_name =
				(cfg.placement_capacity_weighting ==
				 CAP_WEIGHT_PROPORTIONAL)
					? "proportional"
					: "off";
			(void)fprintf(stderr,
				"INFO: DS capacity probe active "
				"(poll=%u ms, mount_fmt=%s, "
				"capacity_weighting=%s)\n",
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
			(void)fprintf(stderr,
				"INFO: DS async-prepare queue active\n");
		} else {
			(void)fprintf(stderr,
				"WARN: ds_prepare_create failed\n");
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
			(void)fprintf(stderr,
				"INFO: DS GC drainer active "
				"(poll=5000 ms, workers=%u, batch=%u)\n",
				(unsigned)cfg.ds_gc_workers,
				(unsigned)cfg.ds_gc_batch_size);
		} else if (ds_gc == NULL) {
			(void)fprintf(stderr,
				"WARN: ds_gc_start failed; "
				"DS-side cleanup will not run\n");
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
			(void)fprintf(stderr,
				"cluster_transport_server_start failed: %d\n",
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
				(void)fprintf(stderr,
					"WARN: failover_init failed: %d\n",
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
#ifdef HAVE_RONDB
				/* RonDB-native partner-liveness watchdog:
				 * polls mds_node_registry.last_heartbeat_ns
				 * and fires failover_promote when the partner
				 * misses its heartbeats.  Replaces the old
				 * LMDB-delta-shipping signal that was removed
				 * with the writer thread. */
				if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
					struct failover_watchdog_cfg wd_cfg;
					memset(&wd_cfg, 0, sizeof(wd_cfg));
					wd_cfg.fo         = fo_ctx;
					wd_cfg.cat        = cat;
					wd_cfg.partner_id = self_m.failover_partner_id;
					if (failover_watchdog_start(&wd_cfg,
								    &fo_wd) == MDS_OK) {
						(void)fprintf(stderr,
							"INFO: RonDB failover watchdog "
							"active (partner=%u)\n",
							(unsigned)self_m.failover_partner_id);
					} else {
						(void)fprintf(stderr,
							"WARN: failover_watchdog_start "
							"failed (partner=%u)\n",
							(unsigned)self_m.failover_partner_id);
						fo_wd = NULL;
					}
				}
#endif
			}
		}
	}

	/* 6a. Commit queue -- RonDB does not use CQ.
	 *     Mutations go directly to NDB. */
	cq = NULL;

	/* 6b. Create session table.
	 *     For RonDB the catalogue vtable handles recovery
	 *     persistence directly (cq stays NULL). */
	if (session_table_init(cfg.self.id, cfg.lease_time_sec,
			       &session_tbl) == 0) {

		session_table_set_cat(session_tbl, cat);
		if (cq != NULL) {
			session_table_set_cq(session_tbl, cq);
		}
	} else {
		(void)fprintf(stderr, "WARN: session_table_init failed\n");
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
		if (cfg.ds_heartbeat_ms == 0) {
			(void)fprintf(stderr,
				"INFO: DS health monitoring disabled "
				"(ds_heartbeat_ms=0)\n");
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
			(void)fprintf(stderr, "WARN: ds_health_init failed\n");
		}
	} else {
		(void)fprintf(stderr, "WARN: layout_recall_init failed\n");
	}

	/* 6d. Open state table (shared with resilver for writer fencing). */
	if (lock_table_init(cfg.self.id, &lock_tbl) != 0) {
		(void)fprintf(stderr, "lock_table_init failed\n");
		return EXIT_FAILURE;
	}

	if (open_state_table_init(cfg.self.id, &ot) != 0) {
		(void)fprintf(stderr, "WARN: open_state_table_init failed\n");
		ot = NULL;
	}

	/* shared-attr: wire RonDB catalogue into stateful subsystems
	 * so open/lock/deleg mutations are persisted to shared tables. */
#ifdef HAVE_RONDB
	if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
		if (ot != NULL) {
			open_state_table_set_cat(ot, cat, rondb_boot_epoch);
			if (cfg.transient_state_cache) {
				open_state_table_set_skip_ndb(ot, true);
				(void)fprintf(stderr,
					"INFO: transient_state_cache=on "
					"(open/layout NDB writes skipped)\n");
			}
		}
		if (lock_tbl != NULL) {
			lock_table_set_cat(lock_tbl, cat, rondb_boot_epoch);
		}
		(void)fprintf(stderr,
			"INFO: shared protocol state active "
			"(open+lock write-through to RonDB)\n");
	}
#endif

	/* 6e. Resilver worker (admin-triggered, not auto-started). */
	if (resilver_init(cat, cq, proxy, ot, &rw) != 0) {
		(void)fprintf(stderr, "WARN: resilver_init failed\n");
		rw = NULL;
	}

	/* 6f. Rebalance worker (admin-triggered, not auto-started). */
	if (rebalance_init(cat, cq, proxy, ot, &rbw) != 0) { /* catalogue escape hatch */
		(void)fprintf(stderr, "WARN: rebalance_init failed\n");
		rbw = NULL;
	}

	/* 6g. I/O tracker for tiering heat signal. */
	if (io_tracker_init(4096, &iot) != 0) {
		(void)fprintf(stderr, "WARN: io_tracker_init failed\n");
		iot = NULL;
	}

	/* 6i. Quota enforcement context. */
	(void)mds_quota_ctx_create(cat, &quota); /* catalogue escape hatch */

	/* 6h. Tiering worker (admin-triggered, not auto-started). */
	if (iot != NULL) {
		if (tiering_init(cat, cq, proxy, ot, iot, &tw) != 0) {
			(void)fprintf(stderr, "WARN: tiering_init failed\n");
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
			(void)fprintf(stderr,
				"INFO: split evaluator active "
				"(threshold=%lu, interval=%us, "
				"auto_exec=%s)\n",
				(unsigned long)cfg.auto_split_threshold,
				(unsigned)cfg.auto_split_interval,
				cfg.auto_split_execute ? "yes" : "no");
		} else {
			(void)fprintf(stderr,
				"WARN: split_evaluator_start failed\n");
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
		rpc_cfg.placement_policy = cfg.placement_policy;
		rpc_cfg.placement_policy_enabled = cfg.placement_policy_enabled;
		rpc_cfg.default_stripe_count = cfg.default_stripe_count;
		rpc_cfg.default_mirror_count = cfg.default_mirror_count;
		rpc_cfg.hpc_getattr_mode = cfg.hpc_getattr_mode;
		rpc_cfg.hpc_max_stripe_count = cfg.hpc_max_stripe_count;
		rpc_cfg.hpc_xdr_form = cfg.hpc_xdr_form;
		if (cfg.placement_policy_enabled) {
			const char *pname =
				(cfg.placement_policy == PLACEMENT_WEIGHTED_RR)
					? "wrr"
				: (cfg.placement_policy == PLACEMENT_CAPACITY)
					? "capacity"
				: "rr";
			(void)fprintf(stderr,
				"INFO: placement_policy=%s (dispatcher "
				"active)\n", pname);
		} else {
			(void)fprintf(stderr,
				"INFO: placement_policy_enabled=false "
				"(legacy RR path)\n");
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

		/* Kerberos auth: initialize GSS if krb5+ requested. */
		struct mds_gss_table *gss_tbl = NULL;
		if (cfg.nfs_auth_mode > NFS_AUTH_MODE_SYS) {
			if (mds_gss_init(cfg.krb5_keytab_path,
			                 cfg.krb5_principal,
			                 &gss_tbl) != 0) {
				(void)fprintf(stderr,
					"FATAL: GSS init failed for "
					"nfs_auth_mode=%d\n",
					(int)cfg.nfs_auth_mode);
				goto cleanup;
			}
			(void)fprintf(stderr,
				"INFO: nfs_auth_mode=%d, GSS ready\n",
				(int)cfg.nfs_auth_mode);
		}
		rpc_cfg.min_auth = cfg.nfs_auth_mode;
		rpc_cfg.gss_tbl = gss_tbl;

	/* Create worker pool for COMPOUND dispatch (3.3).
	 * worker_threads == 0 or 1 -> inline processing (tp = NULL). */
	if (cfg.worker_threads > 1) {
		if (threadpool_create(cfg.worker_threads, &rpc_tp) != 0) {
			(void)fprintf(stderr,
				"WARN: threadpool_create(%u) failed, "
				"falling back to inline dispatch\n",
				(unsigned)cfg.worker_threads);
			rpc_tp = NULL;
		}
	}
	rpc_cfg.tp = rpc_tp;
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

		/* Global inode cache (cross-compound LRU). */
		{
			struct inode_cache *icache = NULL;
			uint32_t icache_size = cfg.inode_cache_size > 0
				? cfg.inode_cache_size : 16384;
			if (inode_cache_init(icache_size, &icache) == 0) {
				(void)fprintf(stderr,
					"INFO: inode cache active "
					"(max=%u entries)\n",
					(unsigned)icache_size);
			} else {
				(void)fprintf(stderr,
					"WARN: inode_cache_init failed\n");
			}
			rpc_cfg.icache = icache;

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
			uint32_t dcache_size = cfg.dirent_cache_size > 0
				? cfg.dirent_cache_size : 32768;
			uint32_t neg_ttl = cfg.negative_cache_ttl_ms > 0
				? cfg.negative_cache_ttl_ms : 5000;
			if (dirent_cache_init(dcache_size, neg_ttl,
					      &dcache) == 0) {
				(void)fprintf(stderr,
					"INFO: dirent cache active "
					"(max=%u, neg_ttl=%ums)\n",
					(unsigned)dcache_size,
					(unsigned)neg_ttl);
			} else {
				(void)fprintf(stderr,
					"WARN: dirent_cache_init failed\n");
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
			uint32_t lcache_size = cfg.layout_cache_size > 0
				? cfg.layout_cache_size : 1024;
			if (layout_cache_init(lcache_size, &lcache) == 0) {
				(void)fprintf(stderr,
					"INFO: HPC layout cache active "
					"(max=%u entries)\n",
					(unsigned)lcache_size);
			} else {
				(void)fprintf(stderr,
					"WARN: layout_cache_init failed; "
					"falling back to catalogue reads\n");
			}
			rpc_cfg.lcache = lcache;
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
					(void)fprintf(stderr,
						"WARN: layout_commit_aggregator_"
						"start failed; tearing down "
						"aggregator\n");
					layout_commit_aggregator_destroy(
						lcommit_agg);
					lcommit_agg = NULL;
				} else {
					(void)fprintf(stderr,
						"INFO: HPC layout_commit "
						"aggregator active "
						"(buckets=%u, flush=%ums)\n",
						(unsigned)(agg_size != 0 ?
							agg_size :
							LCA_DEFAULT_MAX_BUCKETS),
						(unsigned)(agg_flush_ms != 0 ?
							agg_flush_ms :
							LCA_DEFAULT_FLUSH_INTERVAL_MS));
				}
			} else {
				(void)fprintf(stderr,
					"WARN: layout_commit_aggregator_init "
					"failed; falling back to synchronous "
					"LAYOUTCOMMIT\n");
			}
			rpc_cfg.lcommit_agg = lcommit_agg;
		}

		/* Delegation state table (RFC 8881 S10.4).
		 *
		 * Gated by cfg.file_delegations_enabled (default true).  When
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
				(void)fprintf(stderr,
					"INFO: delegation table active\n");
#ifdef HAVE_RONDB
				if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
					deleg_table_set_cat(dt, cat,
							    rondb_boot_epoch);
				}
#endif
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
				(void)fprintf(stderr,
					"WARN: deleg_table_init failed\n");
			}
		} else {
			(void)fprintf(stderr,
				"INFO: file_delegations_enabled=false "
				"(deleg table not wired; OPEN never grants"
				" file delegations)\n");
		}

		/* Directory delegation table (RFC 8881 S10.9). */
		{
			struct dir_deleg_table *ddt = NULL;

			rpc_cfg.ddt = NULL;
			if (cfg.dir_delegations_enabled) {
				if (dir_deleg_table_init(cfg.self.id,
							 &ddt) == 0) {
					(void)fprintf(stderr,
						"INFO: directory delegation "
						"table active\n");
					dir_deleg_table_set_session_table(
						ddt, session_tbl);
					rpc_cfg.ddt = ddt;
				} else {
					(void)fprintf(stderr,
						"WARN: dir_deleg_table_init "
						"failed\n");
				}
			}
		}

		if (rpc_server_create(&rpc_cfg, &rpc_srv[0]) != 0) {
			(void)fprintf(stderr, "rpc_server_create failed\n");
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
			uint32_t target = cfg.worker_threads;
			if (target > MAX_RPC_LISTENERS) {
				target = MAX_RPC_LISTENERS;
			}
			if (target > 4) {
				target = 4; /* Cap listeners; workers handle concurrency */
			}
			rpc_cfg.port = rpc_server_port(rpc_srv[0]);
			for (uint32_t li = 1; li < target; li++) {
				if (rpc_server_create(&rpc_cfg, &rpc_srv[li]) != 0) {
					(void)fprintf(stderr,
						"WARN: RPC listener %u/%u bind failed, "
						"using %u listener(s)\n",
						(unsigned)(li + 1), (unsigned)target,
						(unsigned)li);
					break;
				}
				rpc_listener_count = li + 1;
			}
			(void)fprintf(stderr,
				"INFO: %u RPC listener(s) on port %u\n",
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
				(void)fprintf(stderr,
					"WARN: pthread_create for listener %u "
					"failed\n", (unsigned)li);
				rpc_server_destroy(rpc_srv[li]);
				rpc_srv[li] = NULL;
			} else {
				started_count++;
			}
		}

		if (started_count == 0) {
			(void)fprintf(stderr,
				"FATAL: no RPC listener threads started\n");
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
	 * Configurable via `metrics_http_port` INI key (default 9090). */
	if (cfg.metrics_http_port > 0) {
		struct metrics_http_ctx *mhttp = NULL;
		if (metrics_http_start(cfg.metrics_http_port, cat,
				       &mhttp) == 0) {
			/* mhttp cleaned up at process exit. */
			(void)mhttp;
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
			(void)fprintf(stderr,
				"WARN: mountd_compat_start failed; "
				"showmount -e will not work\n");
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

	/* 10. Wait for shutdown signal via sigwait().
	 *     SIGINT/SIGTERM were blocked before thread creation;
	 *     sigwait() atomically dequeues the pending signal. */
	{
		int caught_sig = 0;
		(void)sigwait(&shutdown_set, &caught_sig);
	}

	/* Orderly shutdown -- ordering matters:
	 * 1. Stop RPC servers + join threads (no new COMPOUNDs)
	 * 2. Destroy copy offload (drain in-flight async copies)
	 * 3. Stop cluster transport (no in-flight rename 2PC)
	 * 4. Destroy remaining subsystems in reverse-init order */
cleanup:
	if (exit_code == EXIT_SUCCESS) {
		(void)fprintf(stdout, "pnfs-mds shutting down.\n");
	} else {
		(void)fprintf(stderr, "pnfs-mds startup failed, cleaning up.\n");
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

	/* Drain the RPC worker pool AFTER epoll loops have exited
	 * but BEFORE destroying any rpc_server.  Workers hold raw
	 * pointers to rpc_server / rpc_conn -- the pool must be
	 * fully joined before that memory is freed. */
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

	/* Phase 3b: RonDB heartbeat + changefeed poller stop + deregister. */
#ifdef HAVE_RONDB
	if (cfg.catalogue_backend == MDS_BACKEND_RONDB && cat != NULL) {
		/* Stop heartbeat thread first. */
		if (rondb_hb_running) {
			atomic_store(&rondb_hb_flag, false);
			(void)pthread_join(rondb_hb_thread, NULL);
			rondb_hb_running = false;
		}
		if (rondb_image != NULL) {
			catalogue_rondb_poller_stop(cat);
			catalog_image_destroy(rondb_image);
			rondb_image = NULL;
		}
		(void)catalogue_rondb_mds_deregister(cat, cfg.self.id);
		(void)fprintf(stderr,
			"INFO: RonDB node %u deregistered\n",
			(unsigned)cfg.self.id);
	}
#endif

	/* Phase 4: remaining subsystems. */
	split_evaluator_stop(split_eval);
#ifdef HAVE_RONDB
	failover_watchdog_stop(fo_wd);
#endif
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
	layout_recall_destroy(lr);
	/* Stop the capacity probe before destroying the DS cache it
	 * writes into.  ds_capacity_stop joins the worker thread. */
	ds_capacity_stop(ds_cap);
	ds_cap = NULL;
	session_table_destroy(session_tbl);
	open_state_table_destroy(ot);
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
	if (cat != NULL) {
		mds_catalogue_close(cat);
		cat = NULL;
	}
	mds_tls_ctx_destroy(cluster_tls);
	mds_tls_ctx_destroy(cluster_tls_client);

	return exit_code;
}
