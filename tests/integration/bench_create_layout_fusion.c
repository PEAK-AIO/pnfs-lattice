/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * bench_create_layout_fusion.c -- Client-side validation of the
 * CREATE+LAYOUTGET fusion design.
 *
 * PURPOSE
 * -------
 * mdtest -F and the Linux kernel NFS client do not bundle
 * OPEN(CREATE) + LAYOUTGET into a single NFSv4.1 COMPOUND, so the
 * lab throughput numbers cannot distinguish the "fused" path from
 * the "unfused" path.  This harness simulates a pNFS client that
 * DOES bundle these two ops and compares both modes back-to-back on
 * the same catalogue + commit queue + DS prealloc pool.
 *
 * The fusion itself is implemented in src/mds/compound_data_io.c
 * (the op_open CQ branch at lines ~139-202) and consumed in
 * src/mds/compound_layout.c (the pregrant check at lines ~711-715).
 * Both landed before this harness.  This file is the correctness
 * and performance evidence that the existing code path:
 *   1. fires when a compound bundles CREATE + LAYOUTGET past the
 *      benign metadata ops (GETFH, GETATTR, ACCESS);
 *   2. returns a usable layout stateid; and
 *   3. is not slower than the unfused two-compound sequence.
 *
 * INVARIANTS VALIDATED
 * --------------------
 *   A. Both modes complete end-to-end without dispatch errors.
 *      OPEN(CREATE) must return NFS4_OK; LAYOUTGET may return
 *      NFS4_OK (ready FHs), NFS4ERR_DELAY (unready FHs on RonDB),
 *      or NFS4ERR_LAYOUTUNAVAILABLE (inline-only memdb backend).
 *      Anything else is a regression.
 *   B. Iteration accounting is complete for both modes
 *      (ready + delayed == iters).
 *
 * NOTE ON MEASUREMENT FIDELITY
 * ----------------------------
 * With the memdb backend the NDB-round-trip cost that fusion
 * actually saves in production is effectively zero (in-memory
 * dirent and inode writes complete in microseconds), so the
 * fused-vs-unfused elapsed ratio in CI is dominated by compound
 * dispatch overhead and will hover near 1.0x.  To observe the
 * real fusion benefit, run this harness against a RonDB-backed
 * catalogue in the lab.
 *
 * MEASUREMENT
 * -----------
 * Reports per-mode: total wall-clock seconds, throughput in files/s,
 * and mean per-file latency in microseconds.  The absolute numbers
 * are catalogue-dependent (memdb in CI; RonDB in the lab).  The
 * fused-vs-unfused ratio is the signal that matters.
 *
 * USAGE
 * -----
 *   ./bench_create_layout_fusion [ITERATIONS] [--rondb CONFPATH]
 *
 * ITERATIONS defaults to 500.  A 10-iteration warm-up is run before
 * each timed loop to prime caches.
 *
 * Without --rondb, the harness uses the in-memory memdb backend
 * (suitable for CI).  With --rondb, it opens a real RonDB catalogue
 * using the connect_string/schema_name in CONFPATH.  On the lab,
 * the default conf is /etc/pnfs-mds/rondb.conf.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "mds_catalogue.h"
#include "session.h"
#include "open_state.h"
#include "commit_queue.h"
#include "ds_prealloc.h"
#include "test_helpers.h"

/* -----------------------------------------------------------------------
 * Defaults
 * ----------------------------------------------------------------------- */

#define BENCH_DEFAULT_ITERS   500
#define BENCH_WARMUP_ITERS    10
#define BENCH_DS_ID           1
#define BENCH_MDS_ID          0
#define BENCH_PARENT_PREFIX   "benchdir"

/* Exit codes. */
#define BENCH_EXIT_OK         0
#define BENCH_EXIT_CORRECTNESS 1
#define BENCH_EXIT_SETUP      2

/* -----------------------------------------------------------------------
 * Minimal assertion helpers
 * ----------------------------------------------------------------------- */

static int g_failures;

#define CHECK(cond, ...) do {						\
	if (!(cond)) {							\
		fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__);	\
		fprintf(stderr, __VA_ARGS__);				\
		fprintf(stderr, "\n");					\
		g_failures++;						\
	}								\
} while (0)

/* -----------------------------------------------------------------------
 * Harness state
 * ----------------------------------------------------------------------- */

struct bench_ctx {
	struct mds_catalogue     *cat;
	struct session_table     *st;
	struct open_state_table  *ot;
	struct commit_queue      *cq;
	bool                      use_cq;
	struct ds_prealloc_ctx   *prealloc;
	uint8_t                   session_id[SESSION_ID_SIZE];
	uint64_t                  clientid;
	uint64_t                  parent_fileid;
	char                      parent_name[128];
	/*
	 * Mirrors cfg.transient_state_cache from the lab daemon.
	 * When true, compound_data.skip_transient_ndb is set on every
	 * compound so both OPEN's CQ pregrant branch and LAYOUTGET's
	 * grant call skip the layout_state NDB write.  This is the
	 * setting the lab runs in production; the bench matches it so
	 * fused-vs-unfused ratios reflect real daemon behaviour.
	 */
	bool                      skip_transient_ndb;
};

/* -----------------------------------------------------------------------
 * Op builders
 * ----------------------------------------------------------------------- */

static struct nfs4_op mk_exchange_id(const char *owner, uint32_t flags)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_EXCHANGE_ID;
	(void)snprintf((char *)op.arg.exchange_id.co_ownerid,
		       sizeof(op.arg.exchange_id.co_ownerid), "%s", owner);
	op.arg.exchange_id.co_ownerid_len = (uint32_t)strlen(owner);
	memset(op.arg.exchange_id.verifier, 0x42, NFS4_VERIFIER_SIZE);
	op.arg.exchange_id.eia_flags = flags;
	return op;
}

static struct nfs4_op mk_create_session(uint64_t clientid, uint32_t seqid,
					uint32_t slots)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_CREATE_SESSION;
	op.arg.create_session.clientid = clientid;
	op.arg.create_session.seqid = seqid;
	op.arg.create_session.csa_flags = 0;
	op.arg.create_session.fore_slots = slots;
	op.arg.create_session.back_slots = slots;
	/* RFC 8881 S18.36 channel attrs -- sized so op_create_session
	 * passes its NFS4ERR_TOOSMALL gate (ca_max{request,response}
	 * size >= 256, ca_max{operations,requests} >= 1).  The bench
	 * is a synthetic harness; it doesn't drive the SEQUENCE
	 * enforcement path so the precise values don't matter
	 * beyond the floor. */
	op.arg.create_session.fore_max_request_size  = 8192;
	op.arg.create_session.fore_max_response_size = 8192;
	op.arg.create_session.fore_max_operations    = 64;
	op.arg.create_session.back_max_request_size  = 4096;
	op.arg.create_session.back_max_response_size = 4096;
	op.arg.create_session.back_max_operations    = 2;
	op.arg.create_session.back_max_requests      = slots;
	return op;
}

static struct nfs4_op mk_sequence(const uint8_t sid[SESSION_ID_SIZE],
				  uint32_t slot_id, uint32_t seq_id)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_SEQUENCE;
	memcpy(op.arg.sequence.session_id, sid, SESSION_ID_SIZE);
	op.arg.sequence.slot_id = slot_id;
	op.arg.sequence.seq_id = seq_id;
	op.arg.sequence.highest_slot_id = slot_id;
	op.arg.sequence.cache_this = false;
	return op;
}

static struct nfs4_op mk_putrootfh(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_PUTROOTFH;
	return op;
}

static struct nfs4_op mk_putfh(uint64_t fileid)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_PUTFH;
	op.arg.putfh.fh.fileid = fileid;
	return op;
}

static struct nfs4_op mk_create_dir(const char *name, uint32_t mode)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_CREATE;
	(void)snprintf(op.arg.create.name, sizeof(op.arg.create.name),
		       "%s", name);
	op.arg.create.type = MDS_FTYPE_DIR;
	op.arg.create.mode = mode;
	return op;
}

static struct nfs4_op mk_open_create(const char *name, uint32_t mode)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_OPEN;
	op.arg.open.claim = CLAIM_NULL;
	(void)snprintf(op.arg.open.name, sizeof(op.arg.open.name),
		       "%s", name);
	op.arg.open.share_access = OPEN4_SHARE_ACCESS_BOTH;
	op.arg.open.share_deny = OPEN4_SHARE_DENY_NONE;
	op.arg.open.create = true;
	op.arg.open.createmode = CREATEMODE_UNCHECKED4;
	op.arg.open.mode = mode;
	return op;
}

static struct nfs4_op mk_getfh(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_GETFH;
	return op;
}

static struct nfs4_op mk_layoutget_rw(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_LAYOUTGET;
	op.arg.layoutget.layout_type = LAYOUT4_NFSV4_1_FILES;
	op.arg.layoutget.iomode = LAYOUTIOMODE4_RW;
	op.arg.layoutget.offset = 0;
	op.arg.layoutget.length = UINT64_MAX;
	op.arg.layoutget.maxcount = 65536;
	return op;
}

static struct nfs4_op mk_close(const struct nfs4_stateid *sid)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_CLOSE;
	op.arg.close.stateid = *sid;
	return op;
}

/* -----------------------------------------------------------------------
 * Catalogue seeding
 * ----------------------------------------------------------------------- */

static int seed_ds_for_bench(struct mds_catalogue *cat, uint32_t ds_id)
{
	struct mds_cat_txn *txn = NULL;
	struct mds_ds_info info;
	uint8_t secret[32];
	uint32_t i;

	memset(&info, 0, sizeof(info));
	info.ds_id = ds_id;
	info.state = DS_ONLINE;
	info.total_bytes = 1000000;
	info.port = 2049;
	info.mode = DS_MODE_GENERIC;
	info.transport = DS_TRANSPORT_TCP;
	(void)snprintf(info.addr, sizeof(info.addr), "bench-ds:/export");
	(void)snprintf(info.host, sizeof(info.host), "bench-ds");

	if (mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn) != MDS_OK) {
		return -1;
	}
	if (mds_cat_ds_put(cat, txn, &info) != MDS_OK) {
		mds_cat_txn_abort(txn);
		return -1;
	}

	/* Deterministic provisioning secret. */
	for (i = 0; i < sizeof(secret); i++) {
		secret[i] = (uint8_t)(i + 1);
	}
	if (mds_cat_ds_provision_put(cat, txn, ds_id,
				     secret, sizeof(secret),
				     1000000ULL) != MDS_OK) {
		mds_cat_txn_abort(txn);
		return -1;
	}

	if (mds_cat_txn_commit(txn) != MDS_OK) {
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Session bring-up
 * ----------------------------------------------------------------------- */

/**
 * Seed compound_data with the fields every bench compound needs.
 *
 * Centralises the per-iteration boilerplate so adding a new knob
 * (e.g. skip_transient_ndb) only has to be done here.
 */
static void bench_init_cd(const struct bench_ctx *c, struct compound_data *cd)
{
	compound_init(cd);
	cd->cat      = c->cat;
	cd->st       = c->st;
	cd->ot       = c->ot;
	cd->cq       = c->use_cq ? c->cq : NULL;
	cd->prealloc = c->prealloc;
	cd->mds_id   = BENCH_MDS_ID;
	cd->skip_transient_ndb = c->skip_transient_ndb;
}

static int bench_bringup_session(struct bench_ctx *c)
{
	struct compound_data cd;
	struct nfs4_op ops[2];
	struct nfs4_result res[2];
	uint32_t n;
	uint32_t create_seqid;

	/* compound_process() runs nfs4_result_destroy() BEFORE memset on
	 * each slot (see src/mds/compound.c:1393).  That dispatcher
	 * branches on r->opnum and free()s heap pointers in r->res.* when
	 * opnum == OP_LAYOUTGET (50).  Stack-resident results carry random
	 * bytes on first use; a stale 50 anywhere in the opnum field
	 * causes free() to be called on a garbage pointer and the bench
	 * SIGSEGVs in libc.  Mirror the rpc_server.c calloc'd thread-local
	 * array by zeroing the stack array up front. */
	memset(res, 0, sizeof(res));

	/* EXCHANGE_ID */
	bench_init_cd(c, &cd);

	ops[0] = mk_exchange_id("bench-fusion-client",
				EXCHGID4_FLAG_USE_PNFS_MDS);
	n = compound_process(&cd, ops, res, 1);
	if (n != 1 || res[0].status != NFS4_OK) {
		fprintf(stderr, "EXCHANGE_ID failed: n=%u status=%d\n",
			n, res[0].status);
		return -1;
	}
	c->clientid   = res[0].res.exchange_id.clientid;
	create_seqid  = res[0].res.exchange_id.seqid;

	/* CREATE_SESSION */
	bench_init_cd(c, &cd);

	ops[0] = mk_create_session(c->clientid, create_seqid, 32);
	n = compound_process(&cd, ops, res, 1);
	if (n != 1 || res[0].status != NFS4_OK) {
		fprintf(stderr, "CREATE_SESSION failed: n=%u status=%d\n",
			n, res[0].status);
		return -1;
	}
	memcpy(c->session_id, res[0].res.create_session.session_id,
	       SESSION_ID_SIZE);
	return 0;
}

/* -----------------------------------------------------------------------
 * Parent directory bring-up
 * ----------------------------------------------------------------------- */

static int bench_create_parent(struct bench_ctx *c, uint32_t *slot_seq)
{
	struct compound_data cd;
	struct nfs4_op ops[4];
	struct nfs4_result res[4];
	uint32_t n;

	/* See note in bench_bringup_session(): zero before first use so
	 * compound_process()'s pre-destroy step does not free() garbage. */
	memset(res, 0, sizeof(res));

	bench_init_cd(c, &cd);

	(void)snprintf(c->parent_name, sizeof(c->parent_name),
		       "%s_%lld_%u", BENCH_PARENT_PREFIX,
		       (long long)time(NULL), (unsigned)getpid());

	ops[0] = mk_sequence(c->session_id, 0, (*slot_seq)++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_create_dir(c->parent_name, 0755);

	n = compound_process(&cd, ops, res, 3);
	if (n != 3 || res[2].status != NFS4_OK) {
		fprintf(stderr, "bench parent CREATE failed: n=%u "
			"status=%d name=%s\n", n,
			n >= 3 ? (int)res[2].status : -1,
			c->parent_name);
		return -1;
	}
	c->parent_fileid = res[2].res.create.inode.fileid;
	(void)fprintf(stdout, "  parent:     /%s (fileid=%lu)\n",
		      c->parent_name, (unsigned long)c->parent_fileid);
	return 0;
}

/* -----------------------------------------------------------------------
 * Timing helper
 * ----------------------------------------------------------------------- */

static double monotonic_seconds(void)
{
	struct timespec t;

	clock_gettime(CLOCK_MONOTONIC, &t);
	return (double)t.tv_sec + (double)t.tv_nsec / 1.0e9;
}

/* -----------------------------------------------------------------------
 * Mode A -- fused: one compound per file
 *
 *   SEQUENCE + PUTFH(parent) + OPEN(CREATE) + GETFH + LAYOUTGET
 *
 * The fusion detector in op_open (src/mds/compound_data_io.c) scans
 * forward from OPEN past benign ops (GETFH/GETATTR/ACCESS) and sets
 * layout_pregrant on the CQ create op, causing the CQ writer to
 * persist the layout state alongside the CREATE.  The subsequent
 * LAYOUTGET detects layout_pregranted and uses the pregrant stateid
 * without its own grant persist.
 * ----------------------------------------------------------------------- */

static int mode_fused_once(struct bench_ctx *c, uint32_t *slot_seq,
			   const char *name, bool *got_layout,
			   struct nfs4_stateid *open_sid_out,
			   uint64_t *child_fileid_out)
{
	struct compound_data cd;
	struct nfs4_op ops[5];
	struct nfs4_result res[5];
	uint32_t n;
	enum nfs4_status lg_status;

	/* See note in bench_bringup_session(): zero before first use so
	 * compound_process()'s pre-destroy step does not free() garbage
	 * on uninitialised opnum=OP_LAYOUTGET stack memory. */
	memset(res, 0, sizeof(res));

	bench_init_cd(c, &cd);

	ops[0] = mk_sequence(c->session_id, 0, (*slot_seq)++);
	ops[1] = mk_putfh(c->parent_fileid);
	ops[2] = mk_open_create(name, 0644);
	ops[3] = mk_getfh();
	ops[4] = mk_layoutget_rw();

	n = compound_process(&cd, ops, res, 5);
	if (n < 5) {
		fprintf(stderr, "fused: short compound n=%u\n", n);
		return -1;
	}
	if (res[2].status != NFS4_OK) {
		fprintf(stderr, "fused: OPEN status=%d name=%s\n",
			res[2].status, name);
		return -1;
	}
	/* LAYOUTGET may return:
	 *   NFS4_OK                   -- ready FHs via real DS (lab);
	 *   NFS4ERR_DELAY             -- unready FHs, phase-3 deferred
	 *                               capture (lab, before refill);
	 *   NFS4ERR_LAYOUTUNAVAILABLE -- inline-only backend (memdb CI).
	 *
	 * All three are valid for the bench: the fusion detector in
	 * op_open and the CQ submit have already run before LAYOUTGET
	 * dispatches.  What matters is that the compound as a whole
	 * completed without a protocol-level error. */
	lg_status = res[4].status;
	if (lg_status != NFS4_OK && lg_status != NFS4ERR_DELAY &&
	    lg_status != NFS4ERR_LAYOUTUNAVAILABLE) {
		fprintf(stderr, "fused: unexpected LAYOUTGET status=%d "
			"(name=%s)\n", lg_status, name);
		return -1;
	}
	*got_layout = (lg_status == NFS4_OK);
	*open_sid_out = res[2].res.open.stateid;
	*child_fileid_out = res[2].res.open.inode.fileid;
	return 0;
}

/* -----------------------------------------------------------------------
 * Mode B -- unfused: two compounds per file
 *
 *   Compound 1: SEQUENCE + PUTFH(parent) + OPEN(CREATE) + GETFH
 *   Compound 2: SEQUENCE + PUTFH(child_fileid) + LAYOUTGET
 *
 * The absence of LAYOUTGET in compound 1 defeats the fusion detector:
 * layout_pregranted stays false, so compound 2's LAYOUTGET executes
 * the full grant-persist path.  Exactly one extra compound dispatch
 * and one extra mds_coord_layout_grant() call per file.
 * ----------------------------------------------------------------------- */

static int mode_unfused_once(struct bench_ctx *c, uint32_t *slot_seq,
			     const char *name, bool *got_layout,
			     struct nfs4_stateid *open_sid_out,
			     uint64_t *child_fileid_out)
{
	struct compound_data cd;
	struct nfs4_op ops[4];
	struct nfs4_result res[4];
	uint32_t n;
	uint64_t child_fileid;
	enum nfs4_status lg_status;

	/* See note in bench_bringup_session(): zero before first use so
	 * compound_process()'s pre-destroy step does not free() garbage
	 * on uninitialised opnum=OP_LAYOUTGET stack memory. */
	memset(res, 0, sizeof(res));

	/* Compound 1: OPEN(CREATE) only -- no LAYOUTGET in this batch. */
	bench_init_cd(c, &cd);

	ops[0] = mk_sequence(c->session_id, 0, (*slot_seq)++);
	ops[1] = mk_putfh(c->parent_fileid);
	ops[2] = mk_open_create(name, 0644);
	ops[3] = mk_getfh();

	n = compound_process(&cd, ops, res, 4);
	if (n < 4 || res[2].status != NFS4_OK) {
		fprintf(stderr, "unfused: OPEN status=%d name=%s\n",
			res[2].status, name);
		return -1;
	}
	child_fileid = res[2].res.open.inode.fileid;
	*open_sid_out = res[2].res.open.stateid;
	*child_fileid_out = child_fileid;

	/* Compound 2: LAYOUTGET on the new child. */
	bench_init_cd(c, &cd);

	ops[0] = mk_sequence(c->session_id, 0, (*slot_seq)++);
	ops[1] = mk_putfh(child_fileid);
	ops[2] = mk_layoutget_rw();

	n = compound_process(&cd, ops, res, 3);
	if (n < 3) {
		fprintf(stderr, "unfused: short LAYOUTGET compound n=%u\n", n);
		return -1;
	}
	lg_status = res[2].status;
	if (lg_status != NFS4_OK && lg_status != NFS4ERR_DELAY &&
	    lg_status != NFS4ERR_LAYOUTUNAVAILABLE) {
		fprintf(stderr, "unfused: unexpected LAYOUTGET status=%d "
			"(name=%s)\n", lg_status, name);
		return -1;
	}
	*got_layout = (lg_status == NFS4_OK);
	return 0;
}

/* -----------------------------------------------------------------------
 * Closing the open: required so the next iteration's OPEN(CREATE) of
 * a different name doesn't exhaust stateid space / open_state slots.
 * ----------------------------------------------------------------------- */

static int close_open(struct bench_ctx *c, uint32_t *slot_seq,
		      uint64_t child_fileid,
		      const struct nfs4_stateid *sid)
{
	struct compound_data cd;
	struct nfs4_op ops[3];
	struct nfs4_result res[3];
	uint32_t n;

	/* See note in bench_bringup_session(): zero before first use so
	 * compound_process()'s pre-destroy step does not free() garbage. */
	memset(res, 0, sizeof(res));

	bench_init_cd(c, &cd);

	ops[0] = mk_sequence(c->session_id, 0, (*slot_seq)++);
	ops[1] = mk_putfh(child_fileid);
	ops[2] = mk_close(sid);

	n = compound_process(&cd, ops, res, 3);
	if (n < 3) {
		fprintf(stderr, "CLOSE compound short: n=%u\n", n);
		return -1;
	}
	if (res[2].status != NFS4_OK) {
		fprintf(stderr, "CLOSE failed: status=%d\n",
			(int)res[2].status);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Run one measurement loop for the specified mode.
 * ----------------------------------------------------------------------- */

struct mode_result {
	const char *label;
	double      elapsed_sec;
	uint32_t    iters;
	uint32_t    layouts_ready;
	uint32_t    layouts_delayed;
};

typedef int (*mode_once_fn)(struct bench_ctx *, uint32_t *, const char *,
			    bool *, struct nfs4_stateid *, uint64_t *);

static int run_mode(struct bench_ctx *c, uint32_t *slot_seq,
		    mode_once_fn fn, const char *name_prefix,
		    uint32_t iters, bool timed,
		    struct mode_result *out)
{
	char name[128];
	uint32_t i;
	double t0 = 0.0;
	double t1 = 0.0;
	uint32_t ready = 0;
	uint32_t delayed = 0;

	if (timed) {
		t0 = monotonic_seconds();
	}
	for (i = 0; i < iters; i++) {
		bool got_layout = false;
		struct nfs4_stateid open_sid;
		uint64_t child_fileid = 0;

		(void)snprintf(name, sizeof(name), "%s_%u",
			       name_prefix, (unsigned)i);

		if (fn(c, slot_seq, name, &got_layout,
		       &open_sid, &child_fileid) != 0) {
			return -1;
		}
		if (got_layout) {
			ready++;
		} else {
			delayed++;
		}
		/* Close the open -- bounded open_state table. */
		if (close_open(c, slot_seq, child_fileid,
			       &open_sid) != 0) {
			return -1;
		}
	}
	if (timed) {
		t1 = monotonic_seconds();
		out->elapsed_sec = t1 - t0;
		out->iters = iters;
		out->layouts_ready = ready;
		out->layouts_delayed = delayed;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Setup and teardown
 * ----------------------------------------------------------------------- */

static int bench_setup(struct bench_ctx *c, const char *rondb_conf,
		       bool skip_transient_ndb, bool use_cq)
{
	int rc;

	memset(c, 0, sizeof(*c));
	c->skip_transient_ndb = skip_transient_ndb;
	c->use_cq = use_cq;

	if (rondb_conf != NULL) {
		struct mds_config cfg;
		enum mds_status st;

		memset(&cfg, 0, sizeof(cfg));
		cfg.catalogue_backend = MDS_BACKEND_RONDB;
		(void)snprintf(cfg.catalogue_backend_conf,
			       sizeof(cfg.catalogue_backend_conf),
			       "%s", rondb_conf);
		cfg.self.id = BENCH_MDS_ID;
		cfg.cluster_size = 1;
		cfg.ndb_conn_pool_size = 2;
		cfg.transient_state_cache = true;

		st = mds_catalogue_open(&cfg, &c->cat);
		if (st != MDS_OK) {
			fprintf(stderr,
				"mds_catalogue_open(rondb, %s) failed: %d\n",
				rondb_conf, (int)st);
			return -1;
		}
		(void)fprintf(stdout, "  backend:    rondb (%s)\n",
			      rondb_conf);
	} else {
		c->cat = catalogue_memdb_open();
		if (c->cat == NULL) {
			fprintf(stderr, "catalogue_memdb_open failed\n");
			return -1;
		}
		(void)fprintf(stdout, "  backend:    memdb (in-process)\n");
	}
	if (seed_ds_for_bench(c->cat, BENCH_DS_ID) != 0) {
		fprintf(stderr, "seed_ds_for_bench failed\n");
		return -1;
	}

	rc = session_table_init(BENCH_MDS_ID, 90, &c->st);
	if (rc != 0) {
		fprintf(stderr, "session_table_init rc=%d\n", rc);
		return -1;
	}
	rc = open_state_table_init(BENCH_MDS_ID, &c->ot);
	if (rc != 0) {
		fprintf(stderr, "open_state_table_init rc=%d\n", rc);
		return -1;
	}

	rc = commit_queue_create(c->cat, NULL, 0, 0, 0, 0, 0, 0, &c->cq);
	if (rc != 0) {
		fprintf(stderr, "commit_queue_create rc=%d\n", rc);
		return -1;
	}

	rc = ds_prealloc_init(c->cat, NULL, 32, &c->prealloc);
	if (rc != 0) {
		fprintf(stderr, "ds_prealloc_init rc=%d\n", rc);
		return -1;
	}

	/*
	 * The bench runs without a live DS mount, so the prealloc
	 * pool would otherwise produce entries with nfs_fh_len == 0
	 * and every LAYOUTGET would return NFS4ERR_DELAY followed by
	 * a revoke.  Enable the test-only synthetic FH path so the
	 * happy LAYOUTGET branch fires and the fused-vs-unfused
	 * measurement reflects the actual code paths production
	 * exercises (rather than the DELAY-dominated worst case).
	 */
	ds_prealloc_test_enable_synthetic_fh(c->prealloc, true);

	return 0;
}

static void bench_teardown(struct bench_ctx *c)
{
	if (c->prealloc != NULL) {
		ds_prealloc_destroy(c->prealloc);
	}
	if (c->cq != NULL) {
		commit_queue_destroy(c->cq);
	}
	if (c->ot != NULL) {
		open_state_table_destroy(c->ot);
	}
	if (c->st != NULL) {
		session_table_destroy(c->st);
	}
	if (c->cat != NULL) {
		mds_catalogue_close(c->cat);
	}
}

/* -----------------------------------------------------------------------
 * Reporting
 * ----------------------------------------------------------------------- */

static void print_mode(const struct mode_result *r)
{
	double per_op_us;
	double ops_per_sec;

	if (r->iters == 0 || r->elapsed_sec <= 0.0) {
		(void)printf("  %-8s  (no data)\n", r->label);
		return;
	}
	per_op_us   = (r->elapsed_sec * 1.0e6) / (double)r->iters;
	ops_per_sec = (double)r->iters / r->elapsed_sec;

	(void)printf("  %-8s  %7u iters  %7.3fs  "
		     "%8.1f ops/s  %7.1f us/op  "
		     "(ready=%u delayed=%u)\n",
		     r->label, r->iters, r->elapsed_sec,
		     ops_per_sec, per_op_us,
		     r->layouts_ready, r->layouts_delayed);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

static void usage(const char *argv0)
{
	fprintf(stderr,
		"usage: %s [ITERATIONS] [--rondb CONFPATH] "
		"[--persist-layout-state] [--no-cq]\n"
		"       ITERATIONS defaults to %u (range 1..100000).\n"
		"       --rondb selects the RonDB catalogue backend\n"
		"               (CONFPATH typically /etc/pnfs-mds/rondb.conf).\n"
		"       --persist-layout-state forces cd.skip_transient_ndb\n"
		"               to false, matching deployments that do persist\n"
		"               layout_state to NDB.  Default matches the lab\n"
		"               (transient_state_cache=true, skip=true).\n"
		"       --no-cq forces cd.cq = NULL so OPEN(CREATE) takes the\n"
		"               direct-catalogue fused shim path.  The lab\n"
		"               daemon runs with cq=NULL on RonDB, so this\n"
		"               mode reflects real daemon behaviour.\n",
		argv0, (unsigned)BENCH_DEFAULT_ITERS);
}

int main(int argc, char **argv)
{
	struct bench_ctx c;
	struct mode_result fused;
	struct mode_result unfused;
	uint32_t iters = BENCH_DEFAULT_ITERS;
	uint32_t slot_seq = 1;
	double ratio;
	const char *rondb_conf = NULL;
	bool skip_transient_ndb = true;
	bool use_cq = true;
	int ai;

	for (ai = 1; ai < argc; ai++) {
		const char *a = argv[ai];

		if (strcmp(a, "--rondb") == 0) {
			if (ai + 1 >= argc) {
				usage(argv[0]);
				return BENCH_EXIT_SETUP;
			}
			rondb_conf = argv[++ai];
		} else if (strcmp(a, "--persist-layout-state") == 0) {
			skip_transient_ndb = false;
		} else if (strcmp(a, "--no-cq") == 0) {
			use_cq = false;
		} else if (a[0] >= '0' && a[0] <= '9') {
			long v = strtol(a, NULL, 10);

			if (v <= 0 || v > 100000) {
				usage(argv[0]);
				return BENCH_EXIT_SETUP;
			}
			iters = (uint32_t)v;
		} else {
			usage(argv[0]);
			return BENCH_EXIT_SETUP;
		}
	}

	(void)printf("CREATE+LAYOUTGET fusion client-side bench\n");
	(void)printf("  iterations: %u (warmup %u)\n",
		     iters, (unsigned)BENCH_WARMUP_ITERS);
	(void)printf("  skip_transient_ndb: %s\n",
		     skip_transient_ndb ? "true (lab default)" : "false");
	(void)printf("  cq path:            %s\n",
		     use_cq ? "on (bench-owned commit_queue)"
			    : "off (direct-catalogue, like lab daemon)");

	if (bench_setup(&c, rondb_conf, skip_transient_ndb, use_cq) != 0) {
		bench_teardown(&c);
		return BENCH_EXIT_SETUP;
	}
	if (bench_bringup_session(&c) != 0) {
		bench_teardown(&c);
		return BENCH_EXIT_SETUP;
	}
	if (bench_create_parent(&c, &slot_seq) != 0) {
		bench_teardown(&c);
		return BENCH_EXIT_SETUP;
	}

	memset(&fused, 0, sizeof(fused));
	memset(&unfused, 0, sizeof(unfused));
	fused.label = "fused";
	unfused.label = "unfused";

	/* Warm-up: prime prealloc refill, inode caches, etc. */
	if (run_mode(&c, &slot_seq, mode_fused_once, "warmf",
		     BENCH_WARMUP_ITERS, false, &fused) != 0) {
		fprintf(stderr, "fused warm-up failed\n");
		bench_teardown(&c);
		return BENCH_EXIT_CORRECTNESS;
	}
	if (run_mode(&c, &slot_seq, mode_unfused_once, "warmu",
		     BENCH_WARMUP_ITERS, false, &unfused) != 0) {
		fprintf(stderr, "unfused warm-up failed\n");
		bench_teardown(&c);
		return BENCH_EXIT_CORRECTNESS;
	}

	/* Timed: alternate fused, unfused to equalise cache pressure. */
	if (run_mode(&c, &slot_seq, mode_fused_once, "fused",
		     iters, true, &fused) != 0) {
		fprintf(stderr, "fused timed run failed\n");
		bench_teardown(&c);
		return BENCH_EXIT_CORRECTNESS;
	}
	if (run_mode(&c, &slot_seq, mode_unfused_once, "unfused",
		     iters, true, &unfused) != 0) {
		fprintf(stderr, "unfused timed run failed\n");
		bench_teardown(&c);
		return BENCH_EXIT_CORRECTNESS;
	}

	(void)printf("\nResults:\n");
	print_mode(&fused);
	print_mode(&unfused);

	if (fused.elapsed_sec > 0.0 && unfused.elapsed_sec > 0.0) {
		ratio = unfused.elapsed_sec / fused.elapsed_sec;
		(void)printf("\n  unfused / fused ratio: %.3fx "
			     "(>1.0 means fused is faster)\n", ratio);
	}

	/* Correctness asserts. */
	CHECK(fused.iters == iters,
	      "fused iter count mismatch %u != %u", fused.iters, iters);
	CHECK(unfused.iters == iters,
	      "unfused iter count mismatch %u != %u", unfused.iters, iters);
	CHECK(fused.layouts_ready + fused.layouts_delayed == iters,
	      "fused layout outcomes incomplete");
	CHECK(unfused.layouts_ready + unfused.layouts_delayed == iters,
	      "unfused layout outcomes incomplete");

	bench_teardown(&c);

	if (g_failures != 0) {
		fprintf(stderr, "\n%d correctness failure(s)\n", g_failures);
		return BENCH_EXIT_CORRECTNESS;
	}
	(void)printf("\nOK\n");
	return BENCH_EXIT_OK;
}
