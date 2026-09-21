/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * slot_matrix_expected.h -- Expected vtable slot presence per backend.
 *
 * One row per slot of the four catalogue vtables (catalogue_internal.h):
 *
 *     X(slot_name, expect_memdb, expect_rondb, expect_fdb)
 *
 * "expect" is the TARGET state of the backend, not whatever the tree
 * happens to populate today: a slot that silently goes NULL changes
 * behaviour through a dispatcher fallback with no other test failing,
 * and a slot that appears without a decision widens the surface a
 * third backend has to match.  Changing a row is therefore a reviewed
 * decision, which is the point of keeping the table in one file.
 *
 * memdb column: the Phase 4 target for the reference backend -- every
 * authority slot it has today (including the cookie cursor
 * ns_readdir_plus_from), every coordination slot RonDB populates plus
 * lock_test, lock_scan_owner and layout_grant_union, all eight cluster
 * slots, no bootstrap, no native handle.  layoutget_fused stays absent:
 * it is a fused fast path with an explicit dispatcher fallback and
 * tests/unit/test_catalogue.c pins mds_coord_layoutget_fused_supported()
 * false for memdb; flipping that is a one-row change here.
 *
 * rondb column: rondb_authority_ops / rondb_coordination_ops in
 * src/catalogue/catalogue_rondb.c (both authority tables populate the
 * same slot set), all eight cluster slots of rondb_cluster_ops,
 * bootstrap and backend_handle present.
 *
 * The optional changefeed slots (image_feed_start / image_feed_stop)
 * are expected on RonDB only: memdb and fdb have no changefeed, so
 * image mode is unavailable there by design.
 *
 * fdb column: the Phase 5 backend (src/catalogue/catalogue_fdb*.c) --
 * every namespace authority slot the RonDB table has except the fused
 * ns_create_with_layout, plus close / probe / bootstrap, plus the
 * extended authority slots of catalogue_fdb_ext.c (inline, xattr,
 * stripe map, DS registry and provisioning, quota, GC, remove_pending,
 * shard, ext_dirent, link_anchor -- the memdb set); every coordination
 * slot including layoutget_fused, lock_test and lock_scan_owner
 * (catalogue_fdb_coord.c) and all eight cluster slots
 * (catalogue_fdb_cluster.c).  backend_client_stats is present (the
 * transaction runner's counters -- network waits, attempts, commits,
 * retries -- in the RonDB shim's shape, so the round-trip accounting is
 * measured the same way on both stores).  prealloc_pool_* stay absent
 * like on memdb, the changefeed slots like on memdb (no changefeed),
 * backend_handle because there is no native handle to expose;
 * ns_readdir_plus stays absent by design (the cookie cursor
 * ns_readdir_plus_from is the fused path, as on memdb).
 */

#ifndef CATALOGUE_CONFORMANCE_SLOT_MATRIX_EXPECTED_H
#define CATALOGUE_CONFORMANCE_SLOT_MATRIX_EXPECTED_H

#include "catalogue_internal.h"

/* --- struct mds_authority_ops ---------------------------------------- */
#define CONFORMANCE_AUTH_SLOTS(X)                                  \
    X(ns_create,                       true,  true,  true)             \
    X(ns_create_wide,                  true,  true,  true)             \
    X(ns_create_with_layout,           false, true,  false)            \
    X(ns_remove,                       true,  true,  true)             \
    X(ns_remove_known,                 false, true,  true)             \
    X(ns_remove_known_gc,              true,  true,  true)             \
    X(ns_parent_touch,                 true,  true,  true)             \
    X(remove_pending_enqueue,          true,  true,  true)             \
    X(remove_pending_enqueue_unlink,   true,  true,  true)             \
    X(remove_pending_peek_batch,       true,  true,  true)             \
    X(remove_pending_claim,            true,  true,  true)             \
    X(remove_pending_complete,         true,  true,  true)             \
    X(remove_pending_bump_retry,       true,  true,  true)             \
    X(remove_pending_count,            true,  true,  true)             \
    X(remove_pending_scan_all,         true,  true,  true)             \
    X(ns_rename,                       true,  true,  true)             \
    X(ns_rename_flags,                 true,  true,  true)             \
    X(ns_link,                         true,  true,  true)             \
    X(ns_lookup,                       true,  true,  true)             \
    X(ns_getattr,                      true,  true,  true)             \
    X(ns_setattr,                      true,  true,  true)             \
    X(ns_readdir,                      true,  true,  true)             \
    X(dirent_name_for_child,           true,  true,  true)             \
    X(ns_readdir_plus,                 false, true,  false)            \
    X(ns_readdir_plus_from,            true,  true,  true)             \
    X(ns_nlink_adjust,                 true,  true,  true)             \
    X(alloc_fileid,                    true,  true,  true)             \
    X(inode_put,                       true,  true,  true)             \
    X(inode_del,                       true,  true,  true)             \
    X(dirent_put,                      true,  true,  true)             \
    X(dirent_insert,                   true,  true,  true)             \
    X(dirent_del,                      true,  true,  true)             \
    X(inline_get,                      true,  true,  true)             \
    X(inline_put,                      true,  true,  true)             \
    X(inline_del,                      true,  true,  true)             \
    X(xattr_get,                       true,  true,  true)             \
    X(xattr_put,                       true,  true,  true)             \
    X(xattr_del,                       true,  true,  true)             \
    X(xattr_list,                      true,  true,  true)             \
    X(xattr_exists,                    true,  true,  true)             \
    X(stripe_map_get,                  true,  true,  true)             \
    X(stripe_map_put,                  true,  true,  true)             \
    X(stripe_map_del,                  true,  true,  true)             \
    X(stripe_map_scan,                 true,  true,  true)             \
    X(ds_get,                          true,  true,  true)             \
    X(ds_put,                          true,  true,  true)             \
    X(ds_del,                          true,  true,  true)             \
    X(ds_list,                         true,  true,  true)             \
    X(ds_provision_get,                true,  true,  true)             \
    X(ds_provision_put,                true,  true,  true)             \
    X(ds_provision_del,                true,  true,  true)             \
    X(quota_rule_get,                  true,  true,  true)             \
    X(quota_rule_put,                  true,  true,  true)             \
    X(quota_usage_get,                 true,  true,  true)             \
    X(quota_usage_put,                 true,  true,  true)             \
    X(gc_enqueue,                      true,  true,  true)             \
    X(gc_peek,                         true,  true,  true)             \
    X(gc_dequeue,                      true,  true,  true)             \
    X(gc_count,                        true,  true,  true)             \
    X(gc_peek_batch,                   true,  true,  true)             \
    X(prealloc_pool_insert,            false, true,  false)            \
    X(prealloc_pool_delete,            false, true,  false)            \
    X(prealloc_pool_scan,              false, true,  false)            \
    X(shard_fileid_get,                true,  false, true)             \
    X(shard_fileid_put,                true,  false, true)             \
    X(shard_fileid_del,                true,  false, true)             \
    X(ext_dirent_get,                  true,  false, true)             \
    X(ext_dirent_put,                  true,  false, true)             \
    X(ext_dirent_del,                  true,  false, true)             \
    X(link_anchor_put,                 true,  false, true)             \
    X(link_anchor_del,                 true,  false, true)             \
    X(backend_client_stats,            false, true,  true)

/* --- struct mds_coordination_ops ------------------------------------- */
#define CONFORMANCE_COORD_SLOTS(X)                                 \
    X(journal_put,                     true,  true,  true)             \
    X(journal_get,                     true,  true,  true)             \
    X(journal_del,                     true,  true,  true)             \
    X(journal_scan,                    true,  true,  true)             \
    X(layout_grant,                    true,  true,  true)             \
    X(layout_grant_union,              true,  true,  true)             \
    X(layoutget_fused,                 false, true,  true)             \
    X(layout_return,                   true,  true,  true)             \
    X(layout_get_by_stateid,           true,  true,  true)             \
    X(layout_scan_for_file,            true,  true,  true)             \
    X(layout_del_all_for_client,       true,  true,  true)             \
    X(ds_layout_idx_scan,              true,  true,  true)             \
    X(layout_iter_file,                true,  true,  true)             \
    X(recovery_put,                    true,  true,  true)             \
    X(recovery_del,                    true,  true,  true)             \
    X(recovery_get,                    true,  true,  true)             \
    X(recovery_list,                   true,  true,  true)             \
    X(open_put,                        true,  true,  true)             \
    X(open_get,                        true,  true,  true)             \
    X(open_del,                        true,  true,  true)             \
    X(open_scan_file,                  true,  true,  true)             \
    X(open_scan_client,                true,  true,  true)             \
    X(lock_put,                        true,  true,  true)             \
    X(lock_del,                        true,  true,  true)             \
    X(lock_test,                       true,  false, true)             \
    X(lock_scan_file,                  true,  true,  true)             \
    X(lock_scan_owner,                 true,  false, true)             \
    X(lock_reap_client,                true,  true,  true)             \
    X(deleg_put,                       true,  true,  true)             \
    X(deleg_get,                       true,  true,  true)             \
    X(deleg_del,                       true,  true,  true)             \
    X(deleg_scan_file,                 true,  true,  true)             \
    X(deleg_scan_client,               true,  true,  true)             \
    X(client_put,                      true,  true,  true)             \
    X(client_get,                      true,  true,  true)             \
    X(client_del,                      true,  true,  true)             \
    X(session_put,                     true,  true,  true)             \
    X(session_get,                     true,  true,  true)             \
    X(session_del,                     true,  true,  true)             \
    X(session_scan_client,             true,  true,  true)             \
    X(slot_put,                        true,  true,  true)             \
    X(slot_get,                        true,  true,  true)

/* --- struct mds_cluster_ops ------------------------------------------ */
#define CONFORMANCE_CLUSTER_SLOTS(X)                               \
    X(node_register,                   true,  true,  true)             \
    X(node_heartbeat,                  true,  true,  true)             \
    X(node_deregister,                 true,  true,  true)             \
    X(node_list,                       true,  true,  true)             \
    X(node_scan_stale,                 true,  true,  true)             \
    X(partition_list,                  true,  true,  true)             \
    X(partition_put,                   true,  true,  true)            \
    X(partition_cas,                   true,  true,  true)

/* --- struct mds_catalogue_ops (lifecycle) ---------------------------- */
#define CONFORMANCE_LIFECYCLE_SLOTS(X)                             \
    X(close,                           true,  true,  true)             \
    X(probe,                           true,  true,  true)             \
    X(bootstrap,                       false, true,  true)             \
    X(backend_handle,                  false, true,  false)            \
    X(image_feed_start,                false, true,  false)            \
    X(image_feed_stop,                 false, true,  false)

/* --- Capability bits (struct mds_catalogue.caps, exact value) -------- */
#define CONFORMANCE_CAPS_MEMDB  (MDS_CAT_CAP_SHARED_AUTHORITY)
#define CONFORMANCE_CAPS_RONDB  (MDS_CAT_CAP_SHARED_AUTHORITY | \
                                 MDS_CAT_CAP_MULTI_PROCESS)
#define CONFORMANCE_CAPS_FDB    (MDS_CAT_CAP_SHARED_AUTHORITY | \
                                 MDS_CAT_CAP_MULTI_PROCESS)

/* --- Public predicates derived from the tables above ----------------- */
#define CONFORMANCE_CLUSTER_SUPPORTED_MEMDB       false
#define CONFORMANCE_CLUSTER_SUPPORTED_RONDB       true
#define CONFORMANCE_CLUSTER_SUPPORTED_FDB         true
#define CONFORMANCE_SHARED_STATE_SUPPORTED_MEMDB  true
#define CONFORMANCE_SHARED_STATE_SUPPORTED_RONDB  true
#define CONFORMANCE_SHARED_STATE_SUPPORTED_FDB    true

#endif /* CATALOGUE_CONFORMANCE_SLOT_MATRIX_EXPECTED_H */
