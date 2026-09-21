/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_indoubt_mapping.c -- MDS_ERR_INDOUBT: status mapping and the
 * caller audit.
 *
 * Part 1 drives every enum mds_status value through mds_status_str()
 * and mds_status_to_nfs4(): every value has a name, MDS_ERR_INDOUBT
 * maps to NFS4ERR_IO, and nothing but MDS_ERR_DELAY maps to
 * NFS4ERR_DELAY.
 *
 * Part 2 fabricates a catalogue handle (catalogue_internal.h, in the
 * style of test_coord_shared_state.c): a proxy whose vtables forward
 * every populated memdb slot through a counting wrapper, with ONE
 * slot at a time replaced by a stand-in that returns MDS_ERR_INDOUBT
 * without touching the store -- exactly what a backend does when its
 * commit outcome could not be resolved.  compound_process() runs
 * CREATE, OPEN(CREATE)+LAYOUTGET, REMOVE (fused, plain, async
 * manifest), RENAME, LAYOUTGET (fused) and LINK against it and every
 * test asserts the same four properties:
 *   1. the NFS status is NFS4ERR_IO (never NFS4_OK, never DELAY);
 *   2. no catalogue slot was invoked after the in-doubt return -- no
 *      retry, no fallback path, no compensating mutation;
 *   3. a placement pop consumed by the create stays consumed: exactly
 *      one pop, never a second one (ds_prealloc has no return path);
 *   4. the store behind the proxy is unchanged.
 * The proxy hands the memdb slots the REAL handle and gives the proxy
 * no backend_private, so a slot the proxy forgot to wrap faults
 * instead of running uncounted.
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "catalogue_internal.h"
#include "catalogue_memdb.h"
#include "compound.h"
#include "ds_prealloc.h"
#include "layout_recall.h"
#include "open_state.h"
#include "parent_touch.h"
#include "remove_manifest.h"
#include "test_helpers.h"

/* Private to src/mds (compound_internal.h); re-declared here like
 * test_compound.c does for its private call targets. */
enum nfs4_status mds_status_to_nfs4(enum mds_status st);

/* Last enumerator of enum mds_status.  Bump when a value is appended
 * so the range tests below cover it (and name it in error.c). */
#define INDOUBT_LAST_STATUS MDS_ERR_INDOUBT

/* -----------------------------------------------------------------------
 * Minimal test framework (test_compound.c shape)
 * ----------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;
static int tests_failed;

#define ASSERT_EQ(a, b) do {                                              \
	long long _av = (long long)(a);                                   \
	long long _bv = (long long)(b);                                   \
	if (_av != _bv) {                                                 \
		(void)fprintf(stderr,                                     \
			      "  FAIL %s:%d: %s (%lld) != %s (%lld)\n",     \
			      __FILE__, __LINE__, #a, _av, #b, _bv);      \
		tests_failed++;                                           \
		return;                                                   \
	}                                                                 \
} while (0)

#define ASSERT_TRUE(x) do {                                               \
	if (!(x)) {                                                       \
		(void)fprintf(stderr, "  FAIL %s:%d: !(%s)\n",            \
			      __FILE__, __LINE__, #x);                    \
		tests_failed++;                                           \
		return;                                                   \
	}                                                                 \
} while (0)

#define RUN_TEST(fn) do {                                                 \
	int _fail_before = tests_failed;                                  \
	tests_run++;                                                      \
	(void)fprintf(stdout, "  %-48s", #fn);                            \
	(void)fflush(stdout);                                             \
	fn();                                                             \
	if (tests_failed == _fail_before) {                               \
		tests_passed++;                                           \
		(void)fprintf(stdout, "PASS\n");                          \
	}                                                                 \
} while (0)

/* Every ASSERT_* above is one branch: the complexity metric counts the
 * assertions, not control flow (same shape as every test in tests/unit). */
/* NOLINTBEGIN(readability-function-cognitive-complexity) */

/* -----------------------------------------------------------------------
 * Part 1: every status has a name and an NFS mapping
 * ----------------------------------------------------------------------- */

static void test_every_status_has_a_name(void)
{
	int v;

	for (v = (int)MDS_OK; v >= (int)INDOUBT_LAST_STATUS; v--) {
		const char *name = mds_status_str((enum mds_status)v);

		ASSERT_TRUE(name != NULL);
		if (strcmp(name, "unknown error") == 0) {
			(void)fprintf(stderr, "  status %d has no name\n", v);
			ASSERT_TRUE(false);
		}
	}
	ASSERT_EQ(strcmp(mds_status_str(MDS_ERR_INDOUBT),
			 "commit outcome in doubt"), 0);
	/* One past the enum: still printable, still distinguishable. */
	ASSERT_EQ(strcmp(mds_status_str((enum mds_status)
					((int)INDOUBT_LAST_STATUS - 1)),
			 "unknown error"), 0);
}

static void test_every_status_has_an_nfs_mapping(void)
{
	int v;

	for (v = (int)MDS_OK; v >= (int)INDOUBT_LAST_STATUS; v--) {
		enum mds_status s = (enum mds_status)v;
		enum nfs4_status nst = mds_status_to_nfs4(s);

		/* Success maps to success and nothing else does. */
		ASSERT_EQ(nst == NFS4_OK, s == MDS_OK);
		/* DELAY is the ONLY retry-inviting mapping. */
		ASSERT_EQ(nst == NFS4ERR_DELAY, s == MDS_ERR_DELAY);
	}
	ASSERT_EQ(mds_status_to_nfs4(MDS_ERR_INDOUBT), NFS4ERR_IO);
	ASSERT_EQ(mds_status_to_nfs4(MDS_ERR_IO), NFS4ERR_IO);
	ASSERT_EQ(mds_status_to_nfs4(MDS_ERR_DELAY), NFS4ERR_DELAY);
}

/* -----------------------------------------------------------------------
 * Part 2: the counting proxy catalogue
 * ----------------------------------------------------------------------- */

#define FAKE_AFTER_MAX 8
#define FAKE_NAME_MAX  40

struct fake_slot_count {
	const char *name;
	int         calls;
};

/* Slots whose invocation count a test may ask for by name. */
static struct fake_slot_count g_counts[] = {
	{ "ns_create", 0 },              { "ns_create_with_layout", 0 },
	{ "ns_create_wide", 0 },         { "ns_remove", 0 },
	{ "ns_remove_known_gc", 0 },     { "remove_pending_enqueue_unlink", 0 },
	{ "ns_rename", 0 },              { "ns_rename_flags", 0 },
	{ "ns_link", 0 },                { "ns_nlink_adjust", 0 },
	{ "gc_enqueue", 0 },             { "stripe_map_get", 0 },
	{ "stripe_map_del", 0 },         { "inode_del", 0 },
	{ "inode_put", 0 },              { "dirent_del", 0 },
	{ "alloc_fileid", 0 },           { "layout_grant", 0 },
	{ "layout_grant_union", 0 },     { "layout_return", 0 },
	{ "layoutget_fused", 0 },        { "journal_put", 0 },
};

static struct {
	struct mds_catalogue       *real;      /* memdb, owns the store */
	struct mds_catalogue        proxy;     /* handed to compound_data */
	struct mds_authority_ops    auth;      /* installed tables */
	struct mds_coordination_ops coord;
	struct mds_catalogue_ops    lifecycle;
	struct mds_authority_ops    real_auth; /* memdb's originals */
	struct mds_coordination_ops real_coord;
	pthread_t                   owner;     /* the test thread */
	int  indoubt_fired;                    /* in-doubt returns so far */
	int  calls_after_indoubt;              /* slot calls after the first */
	char after_names[FAKE_AFTER_MAX][FAKE_NAME_MAX];
	int  calls_other_threads;              /* background callers, not judged */
	int  pops;                             /* ds_prealloc_pop by fake creates */
	int  total_calls;
} g_fake;

static void fake_note(const char *slot)
{
	size_t i;

	if (!pthread_equal(pthread_self(), g_fake.owner)) {
		g_fake.calls_other_threads++;
		return;
	}
	g_fake.total_calls++;
	for (i = 0; i < sizeof(g_counts) / sizeof(g_counts[0]); i++) {
		if (strcmp(g_counts[i].name, slot) == 0) {
			g_counts[i].calls++;
			break;
		}
	}
	if (g_fake.indoubt_fired > 0) {
		if (g_fake.calls_after_indoubt < FAKE_AFTER_MAX) {
			(void)snprintf(
				g_fake.after_names[g_fake.calls_after_indoubt],
				FAKE_NAME_MAX, "%s", slot);
		}
		g_fake.calls_after_indoubt++;
	}
}

static int fake_calls(const char *slot)
{
	size_t i;

	for (i = 0; i < sizeof(g_counts) / sizeof(g_counts[0]); i++) {
		if (strcmp(g_counts[i].name, slot) == 0) {
			return g_counts[i].calls;
		}
	}
	return -1;
}

/* The in-doubt return itself: counted as the slot's call, then armed
 * so every later slot call is an offence. */
static enum mds_status fake_indoubt(void)
{
	g_fake.indoubt_fired++;
	return MDS_ERR_INDOUBT;
}

/* Mirror the backends: a regular-file create pops ONE placement before
 * its transaction; the pop stays consumed whatever the outcome. */
static void fake_pop(struct ds_prealloc_ctx *prealloc, enum mds_file_type type)
{
	struct mds_ds_map_entry e;
	uint32_t su = 0;
	uint64_t fid = 0;

	if (prealloc == NULL || type != MDS_FTYPE_REG) {
		return;
	}
	if (ds_prealloc_pop(prealloc, &e, &su, &fid) == 0) {
		g_fake.pops++;
	}
}

static void fake_dump_after(void)
{
	int i;

	for (i = 0; i < g_fake.calls_after_indoubt && i < FAKE_AFTER_MAX; i++) {
		(void)fprintf(stderr, "  slot called after INDOUBT: %s\n",
			      g_fake.after_names[i]);
	}
}

/* Forwarding wrappers.  The first parameter is always named `cat` (the
 * proxy); the memdb slot receives the real handle. */
#define AUTH_FWD(slot, params, args)                                      \
	static enum mds_status w_##slot params                            \
	{                                                                 \
		(void)cat;                                                \
		fake_note(#slot);                                         \
		return g_fake.real_auth.slot args;                        \
	}
#define COORD_FWD(slot, params, args)                                     \
	static enum mds_status w_##slot params                            \
	{                                                                 \
		(void)cat;                                                \
		fake_note(#slot);                                         \
		return g_fake.real_coord.slot args;                       \
	}

#define C struct mds_catalogue *cat
#define T struct mds_cat_txn *txn
#define R g_fake.real

/* --- authority slots memdb populates -------------------------------- */
AUTH_FWD(ns_create,
	(C, T, uint64_t parent, const char *name, enum mds_file_type type,
	 uint32_t mode, uint64_t uid, uint64_t gid, struct ds_prealloc_ctx *pa,
	 struct mds_inode *out),
	(R, txn, parent, name, type, mode, uid, gid, pa, out))
AUTH_FWD(ns_create_wide,
	(C, uint64_t parent, const char *name, const struct mds_inode *child,
	 uint32_t sc, uint32_t su, uint32_t mc,
	 const struct mds_ds_map_entry *entries, bool *safe_to_discard),
	(R, parent, name, child, sc, su, mc, entries, safe_to_discard))
AUTH_FWD(ns_remove, (C, T, uint64_t parent, const char *name),
	(R, txn, parent, name))
AUTH_FWD(ns_remove_known_gc,
	(C, T, uint64_t parent, const char *name, const struct mds_inode *child,
	 uint32_t sc, const struct mds_ds_map_entry *gc, uint32_t gc_n,
	 uint32_t hint, bool *folded),
	(R, txn, parent, name, child, sc, gc, gc_n, hint, folded))
AUTH_FWD(ns_parent_touch,
	(C, T, uint64_t fileid, uint64_t delta, struct timespec stamp),
	(R, txn, fileid, delta, stamp))
AUTH_FWD(remove_pending_enqueue,
	(C, T, uint64_t dir, const char *name, uint64_t child, uint64_t gen,
	 uint64_t *seq),
	(R, txn, dir, name, child, gen, seq))
AUTH_FWD(remove_pending_enqueue_unlink,
	(C, T, uint64_t dir, const char *name, uint64_t child, uint64_t gen,
	 uint64_t *seq),
	(R, txn, dir, name, child, gen, seq))
AUTH_FWD(remove_pending_peek_batch,
	(C, uint64_t now_ns, struct mds_remove_pending_entry *entries,
	 uint32_t cap, uint32_t *n_out),
	(R, now_ns, entries, cap, n_out))
AUTH_FWD(remove_pending_claim,
	(C, uint64_t seq, uint32_t mds_id, uint64_t boot, uint64_t now_ns,
	 uint64_t ttl_ns),
	(R, seq, mds_id, boot, now_ns, ttl_ns))
AUTH_FWD(remove_pending_complete, (C, uint64_t seq), (R, seq))
AUTH_FWD(remove_pending_bump_retry, (C, uint64_t seq), (R, seq))
AUTH_FWD(remove_pending_count, (C, uint32_t *count), (R, count))
AUTH_FWD(remove_pending_scan_all,
	(C, mds_cat_remove_pending_scan_cb cb, void *ctx), (R, cb, ctx))
AUTH_FWD(ns_rename,
	(C, T, uint64_t sp, const char *sn, uint64_t dp, const char *dn),
	(R, txn, sp, sn, dp, dn))
AUTH_FWD(ns_rename_flags,
	(C, T, uint64_t sp, const char *sn, uint64_t dp, const char *dn,
	 uint32_t flags),
	(R, txn, sp, sn, dp, dn, flags))
AUTH_FWD(ns_link, (C, T, uint64_t parent, const char *name, uint64_t target),
	(R, txn, parent, name, target))
AUTH_FWD(ns_lookup, (C, uint64_t parent, const char *name, struct mds_inode *child),
	(R, parent, name, child))
AUTH_FWD(ns_getattr, (C, uint64_t fileid, struct mds_inode *inode),
	(R, fileid, inode))
AUTH_FWD(ns_setattr,
	(C, T, uint64_t fileid, const struct mds_inode *attrs, uint32_t mask),
	(R, txn, fileid, attrs, mask))
AUTH_FWD(ns_readdir,
	(C, uint64_t parent, const char *start_after, uint32_t max_entries, T,
	 mds_readdir_cb cb, void *ctx),
	(R, parent, start_after, max_entries, txn, cb, ctx))
AUTH_FWD(dirent_name_for_child,
	(C, uint64_t parent, uint64_t child, char *name_out, size_t name_len),
	(R, parent, child, name_out, name_len))
AUTH_FWD(ns_readdir_plus_from,
	(C, uint64_t parent, uint64_t cookie, uint32_t max_entries, T,
	 mds_readdir_plus_cb cb, void *ctx),
	(R, parent, cookie, max_entries, txn, cb, ctx))
AUTH_FWD(ns_nlink_adjust, (C, uint64_t fileid, int32_t delta),
	(R, fileid, delta))
AUTH_FWD(alloc_fileid, (C, T, uint64_t *fileid), (R, txn, fileid))
AUTH_FWD(inode_put, (C, T, const struct mds_inode *inode), (R, txn, inode))
AUTH_FWD(inode_del, (C, T, uint64_t fileid), (R, txn, fileid))
AUTH_FWD(dirent_put,
	(C, T, uint64_t parent, const char *name, uint64_t child, uint8_t type),
	(R, txn, parent, name, child, type))
AUTH_FWD(dirent_insert,
	(C, T, uint64_t parent, const char *name, uint64_t child, uint8_t type),
	(R, txn, parent, name, child, type))
AUTH_FWD(dirent_del, (C, T, uint64_t parent, const char *name),
	(R, txn, parent, name))
AUTH_FWD(inline_get,
	(C, uint64_t fileid, void *buf, uint32_t buflen, uint32_t *outlen),
	(R, fileid, buf, buflen, outlen))
AUTH_FWD(inline_put, (C, T, uint64_t fileid, const void *buf, uint32_t len),
	(R, txn, fileid, buf, len))
AUTH_FWD(inline_del, (C, T, uint64_t fileid), (R, txn, fileid))
AUTH_FWD(xattr_get,
	(C, uint64_t fileid, const char *name, void **val, uint32_t *vallen),
	(R, fileid, name, val, vallen))
AUTH_FWD(xattr_put,
	(C, T, uint64_t fileid, const char *name, const void *val, uint32_t vallen),
	(R, txn, fileid, name, val, vallen))
AUTH_FWD(xattr_del, (C, T, uint64_t fileid, const char *name),
	(R, txn, fileid, name))
AUTH_FWD(xattr_list, (C, uint64_t fileid, mds_xattr_list_cb cb, void *ctx),
	(R, fileid, cb, ctx))
AUTH_FWD(xattr_exists, (C, uint64_t fileid, const char *name),
	(R, fileid, name))
AUTH_FWD(stripe_map_get,
	(C, uint64_t fileid, uint32_t *sc, uint32_t *su, uint32_t *mc,
	 struct mds_ds_map_entry **entries),
	(R, fileid, sc, su, mc, entries))
AUTH_FWD(stripe_map_put,
	(C, T, uint64_t fileid, uint32_t sc, uint32_t su, uint32_t mc,
	 const struct mds_ds_map_entry *entries),
	(R, txn, fileid, sc, su, mc, entries))
AUTH_FWD(stripe_map_del, (C, T, uint64_t fileid), (R, txn, fileid))
AUTH_FWD(stripe_map_scan, (C, mds_cat_stripe_map_scan_cb cb, void *ctx),
	(R, cb, ctx))
AUTH_FWD(ds_get, (C, uint32_t ds_id, struct mds_ds_info *info),
	(R, ds_id, info))
AUTH_FWD(ds_put, (C, T, const struct mds_ds_info *info), (R, txn, info))
AUTH_FWD(ds_del, (C, T, uint32_t ds_id), (R, txn, ds_id))
AUTH_FWD(ds_list, (C, struct mds_ds_info **list, uint32_t *count),
	(R, list, count))
AUTH_FWD(ds_provision_get,
	(C, uint32_t ds_id, uint8_t *secret, uint32_t secret_len, uint64_t *epoch),
	(R, ds_id, secret, secret_len, epoch))
AUTH_FWD(ds_provision_put,
	(C, T, uint32_t ds_id, const uint8_t *secret, uint32_t secret_len,
	 uint64_t epoch),
	(R, txn, ds_id, secret, secret_len, epoch))
AUTH_FWD(ds_provision_del, (C, T, uint32_t ds_id), (R, txn, ds_id))
AUTH_FWD(quota_rule_get,
	(C, uint8_t scope_type, uint64_t scope_id, struct mds_quota_rule *rule),
	(R, scope_type, scope_id, rule))
AUTH_FWD(quota_rule_put,
	(C, T, uint8_t scope_type, uint64_t scope_id,
	 const struct mds_quota_rule *rule),
	(R, txn, scope_type, scope_id, rule))
AUTH_FWD(quota_usage_get,
	(C, uint8_t usage_type, uint64_t scope_id, struct mds_quota_usage *usage),
	(R, usage_type, scope_id, usage))
AUTH_FWD(quota_usage_put,
	(C, T, uint8_t usage_type, uint64_t scope_id,
	 const struct mds_quota_usage *usage),
	(R, txn, usage_type, scope_id, usage))
AUTH_FWD(gc_enqueue,
	(C, T, uint64_t fileid, uint32_t ds_id, const uint8_t *fh, uint32_t fh_len,
	 uint32_t hint),
	(R, txn, fileid, ds_id, fh, fh_len, hint))
AUTH_FWD(gc_peek, (C, struct mds_gc_entry *entry), (R, entry))
AUTH_FWD(gc_dequeue, (C, T, uint64_t gc_seq), (R, txn, gc_seq))
AUTH_FWD(gc_count, (C, uint32_t *count), (R, count))
AUTH_FWD(gc_peek_batch,
	(C, struct mds_gc_entry *entries, uint32_t cap, uint32_t *n_out),
	(R, entries, cap, n_out))
AUTH_FWD(shard_fileid_get, (C, uint64_t fileid, uint32_t *shard_id),
	(R, fileid, shard_id))
AUTH_FWD(shard_fileid_put, (C, T, uint64_t fileid, uint32_t shard_id),
	(R, txn, fileid, shard_id))
AUTH_FWD(shard_fileid_del, (C, T, uint64_t fileid), (R, txn, fileid))
AUTH_FWD(ext_dirent_get,
	(C, uint64_t parent, const char *name, uint32_t *owner, uint64_t *target,
	 uint8_t *type, uint64_t *anchor),
	(R, parent, name, owner, target, type, anchor))
AUTH_FWD(ext_dirent_put,
	(C, T, uint64_t parent, const char *name, uint32_t owner, uint64_t target,
	 uint8_t type, uint64_t anchor),
	(R, txn, parent, name, owner, target, type, anchor))
AUTH_FWD(ext_dirent_del, (C, T, uint64_t parent, const char *name),
	(R, txn, parent, name))
AUTH_FWD(link_anchor_put,
	(C, T, uint64_t anchor, uint32_t remote, uint64_t parent, const char *name),
	(R, txn, anchor, remote, parent, name))
AUTH_FWD(link_anchor_del, (C, T, uint64_t anchor), (R, txn, anchor))

/* --- coordination slots memdb populates ----------------------------- */
COORD_FWD(journal_put, (C, T, const struct mds_coord_journal_record *rec),
	(R, txn, rec))
COORD_FWD(journal_get,
	(C, T, uint64_t txn_id, uint8_t role, struct mds_coord_journal_record *rec),
	(R, txn, txn_id, role, rec))
COORD_FWD(journal_del, (C, T, uint64_t txn_id, uint8_t role),
	(R, txn, txn_id, role))
COORD_FWD(journal_scan, (C, mds_coord_journal_scan_cb cb, void *ctx),
	(R, cb, ctx))
COORD_FWD(layout_grant,
	(C, T, uint64_t clientid, uint64_t fileid, uint32_t iomode, uint64_t off,
	 uint64_t len, const struct nfs4_stateid *sid, const uint32_t *ds_ids,
	 uint32_t ds_count),
	(R, txn, clientid, fileid, iomode, off, len, sid, ds_ids, ds_count))
COORD_FWD(layout_grant_union,
	(C, T, uint64_t clientid, uint64_t fileid, uint32_t iomode, uint64_t off,
	 uint64_t len, const struct nfs4_stateid *sid, const uint32_t *ds_ids,
	 uint32_t ds_count),
	(R, txn, clientid, fileid, iomode, off, len, sid, ds_ids, ds_count))
COORD_FWD(layout_return,
	(C, T, const uint8_t *other, uint64_t clientid, uint64_t fileid,
	 const uint32_t *ds_ids, uint32_t ds_count),
	(R, txn, other, clientid, fileid, ds_ids, ds_count))
COORD_FWD(layout_get_by_stateid,
	(C, const uint8_t *other, uint64_t *clientid, uint64_t *fileid,
	 uint32_t *iomode, uint64_t *off, uint64_t *len, uint32_t *seqid),
	(R, other, clientid, fileid, iomode, off, len, seqid))
COORD_FWD(layout_scan_for_file, (C, uint64_t fileid, bool *has_layout),
	(R, fileid, has_layout))
COORD_FWD(layout_del_all_for_client, (C, uint64_t clientid), (R, clientid))
COORD_FWD(ds_layout_idx_scan,
	(C, uint32_t ds_id, mds_coord_ds_layout_cb cb, void *ctx),
	(R, ds_id, cb, ctx))
COORD_FWD(layout_iter_file,
	(C, uint64_t fileid, mds_coord_layout_file_iter_cb cb, void *ctx),
	(R, fileid, cb, ctx))
COORD_FWD(recovery_put,
	(C, T, uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
	 const uint8_t *verifier),
	(R, txn, clientid, owner, owner_len, verifier))
COORD_FWD(recovery_del, (C, T, uint64_t clientid), (R, txn, clientid))
COORD_FWD(recovery_get,
	(C, uint64_t clientid, uint8_t *owner, uint32_t *owner_len,
	 uint8_t *verifier),
	(R, clientid, owner, owner_len, verifier))
COORD_FWD(recovery_list,
	(C, uint32_t owner_mds_id, mds_recovery_list_cb cb, void *ctx),
	(R, owner_mds_id, cb, ctx))
COORD_FWD(open_put, (C, const struct mds_coord_open_row *row), (R, row))
COORD_FWD(open_get, (C, const uint8_t *other, struct mds_coord_open_row *row),
	(R, other, row))
COORD_FWD(open_del, (C, const uint8_t *other), (R, other))
COORD_FWD(open_scan_file,
	(C, uint64_t fileid, mds_coord_open_scan_cb cb, void *ctx),
	(R, fileid, cb, ctx))
COORD_FWD(open_scan_client,
	(C, uint64_t clientid, mds_coord_open_scan_cb cb, void *ctx),
	(R, clientid, cb, ctx))
COORD_FWD(lock_put, (C, const struct mds_coord_lock_row *row), (R, row))
COORD_FWD(lock_del, (C, uint64_t fileid, uint64_t lock_id),
	(R, fileid, lock_id))
COORD_FWD(lock_test,
	(C, uint64_t fileid, uint32_t lock_type, uint64_t off, uint64_t len,
	 uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
	 struct mds_coord_lock_row *conflict),
	(R, fileid, lock_type, off, len, clientid, owner, owner_len, conflict))
COORD_FWD(lock_scan_file,
	(C, uint64_t fileid, mds_coord_lock_scan_cb cb, void *ctx),
	(R, fileid, cb, ctx))
COORD_FWD(lock_scan_owner,
	(C, uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
	 mds_coord_lock_scan_cb cb, void *ctx),
	(R, clientid, owner, owner_len, cb, ctx))
COORD_FWD(lock_reap_client, (C, uint64_t clientid), (R, clientid))
COORD_FWD(deleg_put, (C, const struct mds_coord_deleg_row *row), (R, row))
COORD_FWD(deleg_get, (C, const uint8_t *other, struct mds_coord_deleg_row *row),
	(R, other, row))
COORD_FWD(deleg_del, (C, const uint8_t *other), (R, other))
COORD_FWD(deleg_scan_file,
	(C, uint64_t fileid, mds_coord_deleg_scan_cb cb, void *ctx),
	(R, fileid, cb, ctx))
COORD_FWD(deleg_scan_client,
	(C, uint64_t clientid, mds_coord_deleg_scan_cb cb, void *ctx),
	(R, clientid, cb, ctx))
COORD_FWD(client_put, (C, const struct mds_coord_client_row *row), (R, row))
COORD_FWD(client_get, (C, uint64_t clientid, struct mds_coord_client_row *row),
	(R, clientid, row))
COORD_FWD(client_del, (C, uint64_t clientid), (R, clientid))
COORD_FWD(session_put, (C, const struct mds_coord_session_row *row), (R, row))
COORD_FWD(session_get,
	(C, const uint8_t *session_id, struct mds_coord_session_row *row),
	(R, session_id, row))
COORD_FWD(session_del, (C, const uint8_t *session_id), (R, session_id))
COORD_FWD(session_scan_client,
	(C, uint64_t clientid, mds_coord_session_scan_cb cb, void *ctx),
	(R, clientid, cb, ctx))
COORD_FWD(slot_put,
	(C, const uint8_t *session_id, uint32_t slot_id, uint32_t seq_id,
	 const void *reply, uint32_t reply_len),
	(R, session_id, slot_id, seq_id, reply, reply_len))
COORD_FWD(slot_get,
	(C, const uint8_t *session_id, uint32_t slot_id,
	 struct mds_coord_drc_slot_row *row),
	(R, session_id, slot_id, row))

#undef C
#undef T
#undef R

/* --- the in-doubt stand-ins ------------------------------------------ */

/* Signatures are fixed by the vtable slot types (catalogue_internal.h):
 * the out-parameters cannot be const even though a stand-in that
 * answers INDOUBT deliberately leaves them untouched. */
/* NOLINTBEGIN(readability-non-const-parameter) */

static enum mds_status fake_ns_create_indoubt(
	struct mds_catalogue *cat, struct mds_cat_txn *txn, uint64_t parent,
	const char *name, enum mds_file_type type, uint32_t mode, uint64_t uid,
	uint64_t gid, struct ds_prealloc_ctx *prealloc, struct mds_inode *out)
{
	(void)cat; (void)txn; (void)parent; (void)name; (void)mode;
	(void)uid; (void)gid; (void)out;
	fake_note("ns_create");
	fake_pop(prealloc, type);
	return fake_indoubt();
}

static enum mds_status fake_ns_create_with_layout_indoubt(
	struct mds_catalogue *cat, uint64_t parent, const char *name,
	enum mds_file_type type, uint32_t mode, uint64_t uid, uint64_t gid,
	struct ds_prealloc_ctx *prealloc, struct mds_inode *out,
	uint64_t layout_clientid, uint32_t layout_iomode, uint64_t layout_offset,
	uint64_t layout_length, const struct nfs4_stateid *layout_stateid,
	uint32_t layout_mds_id, bool *layout_ok,
	struct mds_ds_map_entry *layout_entry_out,
	uint32_t *layout_pop_stripe_unit_out)
{
	(void)cat; (void)parent; (void)name; (void)mode; (void)uid; (void)gid;
	(void)out; (void)layout_clientid; (void)layout_iomode;
	(void)layout_offset; (void)layout_length; (void)layout_stateid;
	(void)layout_mds_id; (void)layout_ok; (void)layout_entry_out;
	(void)layout_pop_stripe_unit_out;
	fake_note("ns_create_with_layout");
	fake_pop(prealloc, type);
	return fake_indoubt();
}

static enum mds_status fake_ns_create_wide_indoubt(
	struct mds_catalogue *cat, uint64_t parent, const char *name,
	const struct mds_inode *child, uint32_t sc, uint32_t su, uint32_t mc,
	const struct mds_ds_map_entry *entries, bool *safe_to_discard)
{
	(void)cat; (void)parent; (void)name; (void)child; (void)sc; (void)su;
	(void)mc; (void)entries; (void)safe_to_discard;
	fake_note("ns_create_wide");
	return fake_indoubt();
}

static enum mds_status fake_ns_remove_indoubt(struct mds_catalogue *cat,
					      struct mds_cat_txn *txn,
					      uint64_t parent, const char *name)
{
	(void)cat; (void)txn; (void)parent; (void)name;
	fake_note("ns_remove");
	return fake_indoubt();
}

static enum mds_status fake_ns_remove_known_gc_indoubt(
	struct mds_catalogue *cat, struct mds_cat_txn *txn, uint64_t parent,
	const char *name, const struct mds_inode *child, uint32_t sc,
	const struct mds_ds_map_entry *gc, uint32_t gc_n, uint32_t hint,
	bool *folded)
{
	(void)cat; (void)txn; (void)parent; (void)name; (void)child; (void)sc;
	(void)gc; (void)gc_n; (void)hint; (void)folded;
	fake_note("ns_remove_known_gc");
	return fake_indoubt();
}

static enum mds_status fake_remove_pending_enqueue_unlink_indoubt(
	struct mds_catalogue *cat, struct mds_cat_txn *txn, uint64_t dir,
	const char *name, uint64_t child, uint64_t gen, uint64_t *seq)
{
	(void)cat; (void)txn; (void)dir; (void)name; (void)child; (void)gen;
	(void)seq;
	fake_note("remove_pending_enqueue_unlink");
	return fake_indoubt();
}

static enum mds_status fake_ns_rename_flags_indoubt(
	struct mds_catalogue *cat, struct mds_cat_txn *txn, uint64_t sp,
	const char *sn, uint64_t dp, const char *dn, uint32_t flags)
{
	(void)cat; (void)txn; (void)sp; (void)sn; (void)dp; (void)dn; (void)flags;
	fake_note("ns_rename_flags");
	return fake_indoubt();
}

static enum mds_status fake_ns_link_indoubt(struct mds_catalogue *cat,
					    struct mds_cat_txn *txn,
					    uint64_t parent, const char *name,
					    uint64_t target)
{
	(void)cat; (void)txn; (void)parent; (void)name; (void)target;
	fake_note("ns_link");
	return fake_indoubt();
}

static enum mds_status fake_layoutget_fused_indoubt(
	struct mds_catalogue *cat, uint64_t fileid, uint32_t *sc, uint32_t *su,
	uint32_t *mc, struct mds_ds_map_entry **entries,
	const struct nfs4_stateid *stateid, uint64_t clientid, uint32_t iomode,
	uint64_t offset, uint64_t length, uint32_t mds_id)
{
	(void)cat; (void)fileid; (void)sc; (void)su; (void)mc; (void)entries;
	(void)stateid; (void)clientid; (void)iomode; (void)offset; (void)length;
	(void)mds_id;
	fake_note("layoutget_fused");
	return fake_indoubt();
}

/* NOLINTEND(readability-non-const-parameter) */

/* --- lifecycle of the proxy ------------------------------------------- */

static void fake_close(struct mds_catalogue *cat)
{
	(void)cat; /* the real handle is closed by fake_teardown */
}

static enum mds_status fake_probe(struct mds_catalogue *cat)
{
	(void)cat;
	return mds_catalogue_probe(g_fake.real);
}

#define AUTH_INSTALL(slot) do {                                           \
	if (g_fake.real_auth.slot != NULL) {                              \
		g_fake.auth.slot = w_##slot;                              \
	}                                                                 \
} while (0)
#define COORD_INSTALL(slot) do {                                          \
	if (g_fake.real_coord.slot != NULL) {                             \
		g_fake.coord.slot = w_##slot;                             \
	}                                                                 \
} while (0)

/*
 * Open a fresh memdb and build the proxy over it.  Every populated
 * memdb slot is forwarded through its counting wrapper; slots memdb
 * leaves NULL stay NULL (same slot matrix, same dispatcher fallbacks).
 * The caller then replaces the one slot under test.
 */
static struct mds_catalogue *fake_setup(void)
{
	size_t i;

	memset(&g_fake, 0, sizeof(g_fake));
	for (i = 0; i < sizeof(g_counts) / sizeof(g_counts[0]); i++) {
		g_counts[i].calls = 0;
	}
	g_fake.owner = pthread_self();
	g_fake.real = open_test_catalogue();
	if (g_fake.real == NULL) {
		return NULL;
	}
	g_fake.real_auth = *g_fake.real->auth_ops;
	g_fake.real_coord = *g_fake.real->coord_ops;
	g_fake.auth = g_fake.real_auth;
	g_fake.coord = g_fake.real_coord;

	AUTH_INSTALL(ns_create);              AUTH_INSTALL(ns_create_wide);
	AUTH_INSTALL(ns_remove);              AUTH_INSTALL(ns_remove_known_gc);
	AUTH_INSTALL(ns_parent_touch);        AUTH_INSTALL(remove_pending_enqueue);
	AUTH_INSTALL(remove_pending_enqueue_unlink);
	AUTH_INSTALL(remove_pending_peek_batch);
	AUTH_INSTALL(remove_pending_claim);   AUTH_INSTALL(remove_pending_complete);
	AUTH_INSTALL(remove_pending_bump_retry);
	AUTH_INSTALL(remove_pending_count);   AUTH_INSTALL(remove_pending_scan_all);
	AUTH_INSTALL(ns_rename);              AUTH_INSTALL(ns_rename_flags);
	AUTH_INSTALL(ns_link);                AUTH_INSTALL(ns_lookup);
	AUTH_INSTALL(ns_getattr);             AUTH_INSTALL(ns_setattr);
	AUTH_INSTALL(ns_readdir);             AUTH_INSTALL(dirent_name_for_child);
	AUTH_INSTALL(ns_readdir_plus_from);   AUTH_INSTALL(ns_nlink_adjust);
	AUTH_INSTALL(alloc_fileid);           AUTH_INSTALL(inode_put);
	AUTH_INSTALL(inode_del);              AUTH_INSTALL(dirent_put);
	AUTH_INSTALL(dirent_insert);          AUTH_INSTALL(dirent_del);
	AUTH_INSTALL(inline_get);             AUTH_INSTALL(inline_put);
	AUTH_INSTALL(inline_del);             AUTH_INSTALL(xattr_get);
	AUTH_INSTALL(xattr_put);              AUTH_INSTALL(xattr_del);
	AUTH_INSTALL(xattr_list);             AUTH_INSTALL(xattr_exists);
	AUTH_INSTALL(stripe_map_get);         AUTH_INSTALL(stripe_map_put);
	AUTH_INSTALL(stripe_map_del);         AUTH_INSTALL(stripe_map_scan);
	AUTH_INSTALL(ds_get);                 AUTH_INSTALL(ds_put);
	AUTH_INSTALL(ds_del);                 AUTH_INSTALL(ds_list);
	AUTH_INSTALL(ds_provision_get);       AUTH_INSTALL(ds_provision_put);
	AUTH_INSTALL(ds_provision_del);       AUTH_INSTALL(quota_rule_get);
	AUTH_INSTALL(quota_rule_put);         AUTH_INSTALL(quota_usage_get);
	AUTH_INSTALL(quota_usage_put);        AUTH_INSTALL(gc_enqueue);
	AUTH_INSTALL(gc_peek);                AUTH_INSTALL(gc_dequeue);
	AUTH_INSTALL(gc_count);               AUTH_INSTALL(gc_peek_batch);
	AUTH_INSTALL(shard_fileid_get);       AUTH_INSTALL(shard_fileid_put);
	AUTH_INSTALL(shard_fileid_del);       AUTH_INSTALL(ext_dirent_get);
	AUTH_INSTALL(ext_dirent_put);         AUTH_INSTALL(ext_dirent_del);
	AUTH_INSTALL(link_anchor_put);        AUTH_INSTALL(link_anchor_del);

	COORD_INSTALL(journal_put);           COORD_INSTALL(journal_get);
	COORD_INSTALL(journal_del);           COORD_INSTALL(journal_scan);
	COORD_INSTALL(layout_grant);          COORD_INSTALL(layout_grant_union);
	COORD_INSTALL(layout_return);         COORD_INSTALL(layout_get_by_stateid);
	COORD_INSTALL(layout_scan_for_file);  COORD_INSTALL(layout_del_all_for_client);
	COORD_INSTALL(ds_layout_idx_scan);    COORD_INSTALL(layout_iter_file);
	COORD_INSTALL(recovery_put);          COORD_INSTALL(recovery_del);
	COORD_INSTALL(recovery_get);          COORD_INSTALL(recovery_list);
	COORD_INSTALL(open_put);              COORD_INSTALL(open_get);
	COORD_INSTALL(open_del);              COORD_INSTALL(open_scan_file);
	COORD_INSTALL(open_scan_client);      COORD_INSTALL(lock_put);
	COORD_INSTALL(lock_del);              COORD_INSTALL(lock_test);
	COORD_INSTALL(lock_scan_file);        COORD_INSTALL(lock_scan_owner);
	COORD_INSTALL(lock_reap_client);      COORD_INSTALL(deleg_put);
	COORD_INSTALL(deleg_get);             COORD_INSTALL(deleg_del);
	COORD_INSTALL(deleg_scan_file);       COORD_INSTALL(deleg_scan_client);
	COORD_INSTALL(client_put);            COORD_INSTALL(client_get);
	COORD_INSTALL(client_del);            COORD_INSTALL(session_put);
	COORD_INSTALL(session_get);           COORD_INSTALL(session_del);
	COORD_INSTALL(session_scan_client);   COORD_INSTALL(slot_put);
	COORD_INSTALL(slot_get);

	g_fake.lifecycle.close = fake_close;
	g_fake.lifecycle.probe = fake_probe;

	memset(&g_fake.proxy, 0, sizeof(g_fake.proxy));
	g_fake.proxy.backend = g_fake.real->backend;
	g_fake.proxy.caps = g_fake.real->caps;
	g_fake.proxy.auth_ops = &g_fake.auth;
	g_fake.proxy.coord_ops = &g_fake.coord;
	g_fake.proxy.ops = &g_fake.lifecycle;
	g_fake.proxy.cluster_ops = NULL;    /* unreachable from compound ops */
	g_fake.proxy.backend_private = NULL; /* an unwrapped slot faults */
	return &g_fake.proxy;
}

static void fake_teardown(void)
{
	/* Teardown may legitimately call slots; stop judging first. */
	g_fake.indoubt_fired = 0;
	mds_catalogue_close(g_fake.real);
	g_fake.real = NULL;
}

/* --- fixture helpers --------------------------------------------------- */

static void seed_online_ds(struct mds_catalogue *cat, uint32_t ds_id)
{
	struct mds_ds_info info;

	memset(&info, 0, sizeof(info));
	info.ds_id = ds_id;
	info.state = DS_ONLINE;
	info.total_bytes = 1000000;
	info.port = 2049;
	info.mode = DS_MODE_GENERIC;
	info.transport = DS_TRANSPORT_TCP;
	(void)snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/export",
		       (unsigned)ds_id);
	if (mds_cat_ds_put(cat, NULL, &info) != MDS_OK) {
		(void)fprintf(stderr, "FATAL: seeding DS %u failed\n",
			      (unsigned)ds_id);
		abort();
	}
}

static bool name_exists(uint64_t parent, const char *name)
{
	struct mds_inode ino;

	return mds_cat_ns_lookup(g_fake.real, parent, name, &ino) == MDS_OK;
}

static uint64_t root_change(void)
{
	struct mds_inode ino;

	if (mds_cat_ns_getattr(g_fake.real, MDS_FILEID_ROOT, &ino) != MDS_OK) {
		return 0;
	}
	return ino.change;
}

static struct nfs4_op mk_sequence(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_SEQUENCE;
	return op;
}

static struct nfs4_op mk_putrootfh(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_PUTROOTFH;
	return op;
}

static struct nfs4_op mk_savefh(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_SAVEFH;
	return op;
}

static struct nfs4_op mk_getfh(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_GETFH;
	return op;
}

static struct nfs4_op mk_lookup(const char *name)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_LOOKUP;
	(void)snprintf(op.arg.lookup.name, sizeof(op.arg.lookup.name), "%s", name);
	return op;
}

static struct nfs4_op mk_create(const char *name, enum mds_file_type type,
				uint32_t mode)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_CREATE;
	(void)snprintf(op.arg.create.name, sizeof(op.arg.create.name), "%s", name);
	op.arg.create.type = type;
	op.arg.create.mode = mode;
	return op;
}

static struct nfs4_op mk_open_create(const char *name, uint32_t mode)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_OPEN;
	op.arg.open.claim = CLAIM_NULL;
	(void)snprintf(op.arg.open.name, sizeof(op.arg.open.name), "%s", name);
	op.arg.open.share_access = OPEN4_SHARE_ACCESS_BOTH;
	op.arg.open.share_deny = OPEN4_SHARE_DENY_NONE;
	op.arg.open.create = true;
	op.arg.open.createmode = CREATEMODE_UNCHECKED4;
	op.arg.open.mode = mode;
	return op;
}

static struct nfs4_op mk_remove(const char *name)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_REMOVE;
	(void)snprintf(op.arg.remove.name, sizeof(op.arg.remove.name), "%s", name);
	return op;
}

static struct nfs4_op mk_rename(const char *src, const char *dst)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_RENAME;
	(void)snprintf(op.arg.rename.src_name, sizeof(op.arg.rename.src_name),
		       "%s", src);
	(void)snprintf(op.arg.rename.dst_name, sizeof(op.arg.rename.dst_name),
		       "%s", dst);
	return op;
}

static struct nfs4_op mk_link(const char *name)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_LINK;
	(void)snprintf(op.arg.link.name, sizeof(op.arg.link.name), "%s", name);
	return op;
}

static struct nfs4_op mk_layoutget(void)
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

/* The shared postcondition of every in-doubt operation. */
#define ASSERT_INDOUBT_TERMINAL(nst) do {                                  \
	ASSERT_EQ((nst), NFS4ERR_IO);                                      \
	ASSERT_EQ(g_fake.indoubt_fired, 1);                                \
	if (g_fake.calls_after_indoubt != 0) {                             \
		fake_dump_after();                                         \
	}                                                                  \
	ASSERT_EQ(g_fake.calls_after_indoubt, 0);                          \
} while (0)

/* -----------------------------------------------------------------------
 * CREATE: one pop consumed, no retry, no compensating remove
 * ----------------------------------------------------------------------- */

static void test_create_indoubt(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct ds_prealloc_ctx *pa = NULL;
	struct compound_data cd;
	struct nfs4_op ops[3];
	struct nfs4_result res[3];
	uint64_t change_before;
	uint32_t n;

	ASSERT_TRUE(cat != NULL);
	seed_online_ds(g_fake.real, 1);
	ASSERT_EQ(ds_prealloc_init(cat, NULL, 1, &pa), 0);
	ASSERT_TRUE(pa != NULL);
	change_before = root_change();

	g_fake.auth.ns_create = fake_ns_create_indoubt;

	memset(res, 0, sizeof(res));
	compound_init(&cd);
	cd.cat = cat;
	cd.prealloc = pa;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_create("c_indoubt", MDS_FTYPE_REG, 0644);
	n = compound_process(&cd, ops, res, 3);

	ASSERT_EQ(n, 3);
	ASSERT_INDOUBT_TERMINAL(res[2].status);
	ASSERT_EQ(fake_calls("ns_create"), 1);
	/* Exactly one placement consumed, none popped again. */
	ASSERT_EQ(g_fake.pops, 1);
	/* Store untouched: no name, no parent bump. */
	ASSERT_TRUE(!name_exists(MDS_FILEID_ROOT, "c_indoubt"));
	ASSERT_EQ(root_change(), change_before);

	ds_prealloc_destroy(pa);
	fake_teardown();
}

/* -----------------------------------------------------------------------
 * OPEN(CREATE) + LAYOUTGET through ns_create_with_layout: the popped
 * bundle is not re-popped, plain ns_create is not tried, no pregrant is
 * revoked and the LAYOUTGET never runs.
 * ----------------------------------------------------------------------- */

static void test_open_create_with_layout_indoubt(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct ds_prealloc_ctx *pa = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[5];
	struct nfs4_result res[5];
	uint32_t n;

	ASSERT_TRUE(cat != NULL);
	seed_online_ds(g_fake.real, 1);
	ASSERT_EQ(ds_prealloc_init(cat, NULL, 1, &pa), 0);
	ASSERT_EQ(open_state_table_init(1, &ot), 0);

	g_fake.auth.ns_create_with_layout = fake_ns_create_with_layout_indoubt;
	ASSERT_TRUE(mds_cat_ns_create_with_layout_supported(cat));

	memset(res, 0, sizeof(res));
	compound_init(&cd);
	cd.cat = cat;
	cd.prealloc = pa;
	cd.ot = ot;
	cd.clientid = 0x100;
	cd.mds_id = 1;
	cd.cfg_serve_layouts = true;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_create("o_indoubt", 0644);
	ops[3] = mk_getfh();
	ops[4] = mk_layoutget();
	n = compound_process(&cd, ops, res, 5);

	/* The compound stops at the failed OPEN. */
	ASSERT_EQ(n, 3);
	ASSERT_INDOUBT_TERMINAL(res[2].status);
	ASSERT_EQ(fake_calls("ns_create_with_layout"), 1);
	ASSERT_EQ(fake_calls("ns_create"), 0);
	ASSERT_EQ(g_fake.pops, 1);
	ASSERT_TRUE(!cd.layout_pregranted);
	ASSERT_TRUE(!cd.stripe_cached);
	ASSERT_TRUE(!name_exists(MDS_FILEID_ROOT, "o_indoubt"));

	open_state_table_destroy(ot);
	ds_prealloc_destroy(pa);
	fake_teardown();
}

/* -----------------------------------------------------------------------
 * ns_create_wide: the dispatcher's safe_to_discard stays false when the
 * slot answers INDOUBT without deciding (the HPC wide create only GCs
 * its DS bundle when this is true).
 * ----------------------------------------------------------------------- */

static void test_create_wide_indoubt_keeps_bundle(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct mds_inode child;
	struct mds_ds_map_entry entries[2];
	bool safe_to_discard = true; /* poisoned: the dispatcher must clear it */
	enum mds_status st;

	ASSERT_TRUE(cat != NULL);
	g_fake.auth.ns_create_wide = fake_ns_create_wide_indoubt;

	memset(&child, 0, sizeof(child));
	child.fileid = 4242;
	child.type = MDS_FTYPE_REG;
	child.nlink = 1;
	child.parent_fileid = MDS_FILEID_ROOT;
	memset(entries, 0, sizeof(entries));
	entries[0].ds_id = 1;
	entries[1].ds_id = 2;

	st = mds_cat_ns_create_wide(cat, MDS_FILEID_ROOT, "w_indoubt", &child,
				    2, 65536, 1, entries, &safe_to_discard);

	ASSERT_EQ(st, MDS_ERR_INDOUBT);
	ASSERT_TRUE(!safe_to_discard);
	ASSERT_EQ(g_fake.indoubt_fired, 1);
	ASSERT_EQ(g_fake.calls_after_indoubt, 0);
	ASSERT_EQ(fake_calls("gc_enqueue"), 0);

	fake_teardown();
}

/* -----------------------------------------------------------------------
 * REMOVE, fused final unlink: no legacy split path, no GC rows, no quota
 * ----------------------------------------------------------------------- */

static void test_remove_fused_indoubt(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct mds_inode f;
	struct mds_ds_map_entry sme;
	struct compound_data cd;
	struct nfs4_op ops[3];
	struct nfs4_result res[3];
	uint32_t gc_rows = 0;
	uint32_t n;

	ASSERT_TRUE(cat != NULL);
	ASSERT_EQ(test_create_file(g_fake.real, MDS_FILEID_ROOT, "r_fused",
				   0644, &f), MDS_OK);
	memset(&sme, 0, sizeof(sme));
	sme.ds_id = 1;
	ASSERT_EQ(mds_cat_stripe_map_put(g_fake.real, NULL, f.fileid, 1, 65536, 1,
					 &sme), MDS_OK);

	g_fake.auth.ns_remove_known_gc = fake_ns_remove_known_gc_indoubt;

	memset(res, 0, sizeof(res));
	compound_init(&cd);
	cd.cat = cat;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_remove("r_fused");
	n = compound_process(&cd, ops, res, 3);

	ASSERT_EQ(n, 3);
	ASSERT_INDOUBT_TERMINAL(res[2].status);
	ASSERT_EQ(fake_calls("ns_remove_known_gc"), 1);
	ASSERT_EQ(fake_calls("ns_remove"), 0);
	ASSERT_EQ(fake_calls("gc_enqueue"), 0);
	ASSERT_EQ(fake_calls("stripe_map_del"), 0);
	ASSERT_EQ(fake_calls("inode_del"), 0);
	ASSERT_TRUE(name_exists(MDS_FILEID_ROOT, "r_fused"));
	ASSERT_EQ(mds_cat_gc_count(g_fake.real, &gc_rows), MDS_OK);
	ASSERT_EQ(gc_rows, 0);

	fake_teardown();
}

/* -----------------------------------------------------------------------
 * REMOVE, plain path (no stripe map): the same, through ns_remove
 * ----------------------------------------------------------------------- */

static void test_remove_plain_indoubt(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct mds_inode f;
	struct compound_data cd;
	struct nfs4_op ops[3];
	struct nfs4_result res[3];
	uint32_t n;

	ASSERT_TRUE(cat != NULL);
	ASSERT_EQ(test_create_file(g_fake.real, MDS_FILEID_ROOT, "r_plain",
				   0644, &f), MDS_OK);

	g_fake.auth.ns_remove = fake_ns_remove_indoubt;

	memset(res, 0, sizeof(res));
	compound_init(&cd);
	cd.cat = cat;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_remove("r_plain");
	n = compound_process(&cd, ops, res, 3);

	ASSERT_EQ(n, 3);
	ASSERT_INDOUBT_TERMINAL(res[2].status);
	ASSERT_EQ(fake_calls("ns_remove"), 1);
	ASSERT_EQ(fake_calls("gc_enqueue"), 0);
	ASSERT_TRUE(name_exists(MDS_FILEID_ROOT, "r_plain"));

	fake_teardown();
}

/* -----------------------------------------------------------------------
 * REMOVE, async manifest (delete-at-ack): the in-doubt manifest commit
 * must not fall back to the synchronous remove, must leave no tombstone
 * and no GC row.
 * ----------------------------------------------------------------------- */

static struct mds_config g_cfg_single;

static void test_remove_async_manifest_indoubt(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct remove_manifest *rm = NULL;
	struct parent_touch *pt = NULL;
	struct open_state_table *ot = NULL;
	struct parent_touch_stats pts;
	struct mds_inode f;
	struct compound_data cd;
	struct nfs4_op ops[3];
	struct nfs4_result res[3];
	uint32_t gc_rows = 0;
	uint32_t n;

	ASSERT_TRUE(cat != NULL);
	ASSERT_EQ(test_create_file(g_fake.real, MDS_FILEID_ROOT, "r_async",
				   0644, &f), MDS_OK);
	ASSERT_EQ(remove_manifest_init(cat, NULL, NULL, NULL, NULL, 1, 1, 128, 1,
				       8, 20, 60ULL * 1000000000ULL, &rm), 0);
	ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);
	ASSERT_EQ(open_state_table_init(1, &ot), 0);
	memset(&g_cfg_single, 0, sizeof(g_cfg_single));
	g_cfg_single.cluster_size = 1;

	g_fake.auth.remove_pending_enqueue_unlink =
		fake_remove_pending_enqueue_unlink_indoubt;

	memset(res, 0, sizeof(res));
	compound_init(&cd);
	cd.cat = cat;
	cd.rmf = rm;
	cd.pt = pt;
	cd.ot = ot;
	cd.cfg = &g_cfg_single;
	cd.mds_id = 1;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_remove("r_async");
	n = compound_process(&cd, ops, res, 3);

	ASSERT_EQ(n, 3);
	ASSERT_INDOUBT_TERMINAL(res[2].status);
	ASSERT_EQ(fake_calls("remove_pending_enqueue_unlink"), 1);
	/* Never the synchronous remove after an in-doubt manifest commit. */
	ASSERT_EQ(fake_calls("ns_remove"), 0);
	ASSERT_EQ(fake_calls("ns_remove_known_gc"), 0);
	ASSERT_EQ(fake_calls("gc_enqueue"), 0);
	/* Tombstone rolled back, prepared parent bump released. */
	ASSERT_EQ(remove_manifest_pending(rm), 0);
	ASSERT_TRUE(!remove_manifest_is_tombstoned(rm, MDS_FILEID_ROOT,
						  "r_async"));
	parent_touch_stats_get(pt, &pts);
	ASSERT_EQ(pts.pinned_count, 0);
	ASSERT_EQ(pts.submits, 0);
	/* Store untouched. */
	ASSERT_TRUE(name_exists(MDS_FILEID_ROOT, "r_async"));
	ASSERT_EQ(mds_cat_gc_count(g_fake.real, &gc_rows), MDS_OK);
	ASSERT_EQ(gc_rows, 0);

	g_fake.indoubt_fired = 0;
	remove_manifest_destroy(rm);
	parent_touch_destroy(pt);
	open_state_table_destroy(ot);
	fake_teardown();
}

/* -----------------------------------------------------------------------
 * RENAME: no plain-rename retry, no victim GC, no quota, no journal
 * ----------------------------------------------------------------------- */

static void test_rename_indoubt(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct mds_inode f;
	struct compound_data cd;
	struct nfs4_op ops[5];
	struct nfs4_result res[5];
	uint64_t change_before;
	uint32_t n;

	ASSERT_TRUE(cat != NULL);
	ASSERT_EQ(test_create_file(g_fake.real, MDS_FILEID_ROOT, "rn_src",
				   0644, &f), MDS_OK);
	change_before = root_change();

	g_fake.auth.ns_rename_flags = fake_ns_rename_flags_indoubt;

	memset(res, 0, sizeof(res));
	compound_init(&cd);
	cd.cat = cat;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_savefh();
	ops[3] = mk_rename("rn_src", "rn_dst");
	n = compound_process(&cd, ops, res, 4);

	ASSERT_EQ(n, 4);
	ASSERT_INDOUBT_TERMINAL(res[3].status);
	ASSERT_EQ(fake_calls("ns_rename_flags"), 1);
	ASSERT_EQ(fake_calls("ns_rename"), 0);
	ASSERT_EQ(fake_calls("gc_enqueue"), 0);
	ASSERT_EQ(fake_calls("stripe_map_del"), 0);
	ASSERT_EQ(fake_calls("journal_put"), 0);
	ASSERT_TRUE(name_exists(MDS_FILEID_ROOT, "rn_src"));
	ASSERT_TRUE(!name_exists(MDS_FILEID_ROOT, "rn_dst"));
	ASSERT_EQ(root_change(), change_before);

	fake_teardown();
}

/* -----------------------------------------------------------------------
 * LAYOUTGET, fused: no split-path grant, no revoke, no layout granted
 * ----------------------------------------------------------------------- */

static void test_layoutget_fused_indoubt(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct mds_inode f;
	struct compound_data cd;
	struct nfs4_op ops[4];
	struct nfs4_result res[4];
	bool has_layout = true;
	uint32_t n;

	ASSERT_TRUE(cat != NULL);
	ASSERT_EQ(test_create_file(g_fake.real, MDS_FILEID_ROOT, "lg_file",
				   0644, &f), MDS_OK);

	g_fake.coord.layoutget_fused = fake_layoutget_fused_indoubt;
	ASSERT_TRUE(mds_coord_layoutget_fused_supported(cat));

	memset(res, 0, sizeof(res));
	compound_init(&cd);
	cd.cat = cat;
	cd.clientid = 0x100;
	cd.mds_id = 1;
	cd.cfg_serve_layouts = true;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("lg_file");
	ops[3] = mk_layoutget();
	n = compound_process(&cd, ops, res, 4);

	ASSERT_EQ(n, 4);
	ASSERT_INDOUBT_TERMINAL(res[3].status);
	ASSERT_EQ(fake_calls("layoutget_fused"), 1);
	ASSERT_EQ(fake_calls("stripe_map_get"), 0);
	ASSERT_EQ(fake_calls("layout_grant"), 0);
	ASSERT_EQ(fake_calls("layout_grant_union"), 0);
	ASSERT_EQ(fake_calls("layout_return"), 0);
	ASSERT_EQ(mds_coord_layout_scan_for_file(g_fake.real, f.fileid,
						 &has_layout), MDS_OK);
	ASSERT_TRUE(!has_layout);

	fake_teardown();
}

/* -----------------------------------------------------------------------
 * LINK: no nlink compensation, no retry
 * ----------------------------------------------------------------------- */

static void test_link_indoubt(void)
{
	struct mds_catalogue *cat = fake_setup();
	struct mds_inode f;
	struct mds_inode after;
	struct compound_data cd;
	struct nfs4_op ops[6];
	struct nfs4_result res[6];
	uint32_t n;

	ASSERT_TRUE(cat != NULL);
	ASSERT_EQ(test_create_file(g_fake.real, MDS_FILEID_ROOT, "ln_target",
				   0644, &f), MDS_OK);

	g_fake.auth.ns_link = fake_ns_link_indoubt;

	memset(res, 0, sizeof(res));
	compound_init(&cd);
	cd.cat = cat;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("ln_target");
	ops[3] = mk_savefh();
	ops[4] = mk_putrootfh();
	ops[5] = mk_link("ln_second");
	n = compound_process(&cd, ops, res, 6);

	ASSERT_EQ(n, 6);
	ASSERT_INDOUBT_TERMINAL(res[5].status);
	ASSERT_EQ(fake_calls("ns_link"), 1);
	ASSERT_EQ(fake_calls("ns_nlink_adjust"), 0);
	ASSERT_EQ(fake_calls("inode_put"), 0);
	ASSERT_TRUE(!name_exists(MDS_FILEID_ROOT, "ln_second"));
	ASSERT_EQ(mds_cat_ns_getattr(g_fake.real, f.fileid, &after), MDS_OK);
	ASSERT_EQ(after.nlink, 1);

	fake_teardown();
}

/* ----------------------------------------------------------------------- */

int main(void)
{
	(void)fprintf(stdout, "test_indoubt_mapping:\n");

	RUN_TEST(test_every_status_has_a_name);
	RUN_TEST(test_every_status_has_an_nfs_mapping);
	RUN_TEST(test_create_indoubt);
	RUN_TEST(test_open_create_with_layout_indoubt);
	RUN_TEST(test_create_wide_indoubt_keeps_bundle);
	RUN_TEST(test_remove_fused_indoubt);
	RUN_TEST(test_remove_plain_indoubt);
	RUN_TEST(test_remove_async_manifest_indoubt);
	RUN_TEST(test_rename_indoubt);
	RUN_TEST(test_layoutget_fused_indoubt);
	RUN_TEST(test_link_indoubt);

	(void)fprintf(stdout, "\n%d/%d tests passed\n", tests_passed, tests_run);
	/* The LAYOUTGET paths above populate the process-global seqid
	 * table; release it so the leak check sees no reachable blocks. */
	layout_seqid_table_destroy();
	return (tests_failed == 0 && tests_passed == tests_run) ? 0 : 1;
}

/* NOLINTEND(readability-function-cognitive-complexity) */
