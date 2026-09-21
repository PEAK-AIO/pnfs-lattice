# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

enable_testing()

# --- Unit tests ---

add_executable(test_layout       unit/test_layout.c)
add_executable(test_xdr_layout_form unit/test_xdr_layout_form.c)
add_executable(test_rename_2pc   unit/test_rename_2pc.c)
add_executable(test_subtree_map  unit/test_subtree_map.c)
add_executable(test_inode_cache  unit/test_inode_cache.c)
add_executable(test_dirent_cache unit/test_dirent_cache.c)
add_executable(test_parent_touch unit/test_parent_touch.c)
add_executable(test_remove_manifest unit/test_remove_manifest.c)
add_executable(test_compound     unit/test_compound.c)
add_executable(test_cluster_transport unit/test_cluster_transport.c)
add_executable(test_compound_cluster  unit/test_compound_cluster.c)
add_executable(test_session      unit/test_session.c)
add_executable(test_open_state   unit/test_open_state.c)
add_executable(test_lock_state   unit/test_lock_state.c)
add_executable(test_migration    unit/test_migration.c)
if(ENABLE_REPLICATION)
    add_executable(test_repl_health unit/test_repl_health.c)
endif()
add_executable(test_inline_data  unit/test_inline_data.c)
add_executable(test_commit_queue unit/test_commit_queue.c)
add_executable(test_nfsv42       unit/test_nfsv42.c)
add_executable(test_xdr_codec    unit/test_xdr_codec.c)
add_executable(test_nfs4_cb      unit/test_nfs4_cb.c)
add_executable(test_ds_health    unit/test_ds_health.c)
add_executable(test_ds_layout_idx unit/test_ds_layout_idx.c)
add_executable(test_layout_recall unit/test_layout_recall.c)
# Gated tests: only built when the corresponding ENABLE_<NAME> module is
# the real implementation.  The community / stub build skips them since
# the stubs return no-ops and the tests would assert real behaviour.
if(ENABLE_RESILVER)
    add_executable(test_resilver  unit/test_resilver.c)
endif()
if(ENABLE_REBALANCE)
    add_executable(test_rebalance unit/test_rebalance.c)
endif()
add_executable(test_failover_unit unit/test_failover.c)
add_executable(test_subtree_split unit/test_subtree_split.c)
add_executable(test_cluster_membership unit/test_cluster_membership.c)
add_executable(test_drain          unit/test_drain.c)
if(ENABLE_TIERING)
    add_executable(test_tiering   unit/test_tiering.c)
endif()
if(ENABLE_QUOTA)
    add_executable(test_quota     unit/test_quota.c)
endif()
add_executable(test_backup        unit/test_backup.c)
add_executable(test_rolling_upgrade unit/test_rolling_upgrade.c)
add_executable(test_mds_metrics    unit/test_mds_metrics.c)
add_executable(test_placement_policy unit/test_placement_policy.c)
add_executable(test_mds_tls        unit/test_mds_tls.c)
add_executable(test_mds_gss        unit/test_mds_gss.c)
add_executable(test_config          unit/test_config.c)
add_executable(test_log             unit/test_log.c)
if(ENABLE_DS_PREALLOC)
    add_executable(test_ds_prealloc       unit/test_ds_prealloc.c)
    add_executable(test_ds_prealloc_batch unit/test_ds_prealloc_batch.c)
endif()
add_executable(test_open_exclusive  unit/test_open_exclusive.c)
add_executable(test_mds_shard unit/test_mds_shard.c)
add_executable(test_mds_admin       unit/test_mds_admin.c
                                    ${PROJECT_SOURCE_DIR}/src/tools/admin_util.c)
add_executable(test_find_query      unit/test_find_query.c
                                    ${PROJECT_SOURCE_DIR}/src/tools/find_query.c)
add_executable(test_apid_http       unit/test_apid_http.c
                                    ${PROJECT_SOURCE_DIR}/src/tools/apid_http.c
                                    ${PROJECT_SOURCE_DIR}/src/tools/find_query.c)
add_executable(test_catalogue       unit/test_catalogue.c)
add_executable(test_catalog_delta   unit/test_catalog_delta.c)
add_executable(test_catalog_image   unit/test_catalog_image.c)
add_executable(test_rondb_schema    unit/test_rondb_schema.c)
add_executable(test_rondb_lock_shim unit/test_rondb_lock_shim.c)
add_executable(test_rondb_changefeed unit/test_rondb_changefeed.c)
add_executable(test_coord_shared_state unit/test_coord_shared_state.c)
add_executable(test_dispatch_null_slots unit/test_dispatch_null_slots.c)
add_executable(test_dir_delegation  unit/test_dir_delegation.c)
add_executable(test_delegation      unit/test_delegation.c)
add_executable(test_ds_capacity     unit/test_ds_capacity.c)
add_executable(test_ds_io_limits    unit/test_ds_io_limits.c)
if(ENABLE_DS_GC)
    add_executable(test_ds_gc       unit/test_ds_gc.c)
endif()
add_executable(test_hpc_shared      unit/test_hpc_shared.c)
if(ENABLE_LAYOUT_CACHE)
    add_executable(test_layout_cache unit/test_layout_cache.c)
endif()
if(ENABLE_LAYOUT_COMMIT_AGGREGATOR)
    add_executable(test_layout_commit_aggregator
                                    unit/test_layout_commit_aggregator.c)
endif()
add_executable(test_mountd_compat   unit/test_mountd_compat.c)

foreach(test_target
    test_layout
    test_rename_2pc
    test_subtree_map
    test_inode_cache
    test_dirent_cache
    test_parent_touch
    test_remove_manifest
    test_compound
    test_cluster_transport
    test_compound_cluster
    test_session
    test_open_state
    test_lock_state
    test_migration
    test_repl_health
    test_inline_data
    test_commit_queue
    test_nfsv42
    test_failover_unit
    test_subtree_split
    test_cluster_membership
    test_drain
    test_config
    test_log
    test_ds_prealloc
    test_ds_prealloc_batch
    test_open_exclusive
    test_mds_shard
    test_ds_health
    test_ds_layout_idx
    test_resilver
    test_rebalance
    test_tiering
    test_quota
    test_backup
    test_rolling_upgrade
    test_mds_admin
    test_find_query
    test_apid_http
    test_mds_metrics
    test_placement_policy
    test_mds_tls
    test_mds_gss
    test_catalogue
    test_catalog_delta
    test_catalog_image
    test_rondb_schema
    test_rondb_lock_shim
    test_rondb_changefeed
    test_coord_shared_state
    test_dispatch_null_slots
    test_dir_delegation
    test_delegation
    test_ds_capacity
    test_ds_io_limits
    test_ds_gc
    test_hpc_shared
    test_layout_cache
    test_layout_commit_aggregator
    test_mountd_compat
)
    # Skip iterations for targets that were not declared (gated tests
    # like test_resilver disappear when ENABLE_<NAME>=OFF).  This is
    # the canonical pattern for every gated test in this file.
    if(NOT TARGET ${test_target})
        continue()
    endif()
    target_include_directories(${test_target} PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src/tools
        ${PROJECT_SOURCE_DIR}/src/catalogue
        ${PROJECT_SOURCE_DIR}/tests
    )
    target_link_libraries(${test_target} PRIVATE
        pnfs_mds_core
        pnfs_cluster
        pnfs_fsal_obj
        pnfs_common
        pnfs_mds_core
        pnfs_cluster
        pnfs_mds_core
        rondb_schema
        pthread
    )
    add_test(NAME ${test_target} COMMAND ${test_target})
    set_tests_properties(${test_target} PROPERTIES LABELS "unit")
endforeach()

# Catalogue test needs compound_internal.h from src/mds
target_include_directories(test_catalogue PRIVATE
    ${PROJECT_SOURCE_DIR}/src/mds
)

# Shard test pulls in compound.h -> nfs4_cb -> ntirpc
target_link_libraries(test_mds_shard PRIVATE ${NTIRPC_LIB})


# Layout test needs ntirpc for XDR encode verification
target_link_libraries(test_layout PRIVATE ${NTIRPC_LIB})

# Phase C / Step 6 -- wire-form XDR test needs ntirpc + the
# pnfs_mds_core static library that owns encode_res_layoutget.
target_include_directories(test_xdr_layout_form PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
# pnfs_cluster (subtree_map.c, cluster_membership.c) calls back into
# the RonDB C wrappers that live in pnfs_mds_core, so the core archive
# must be scanned again after pnfs_cluster -- same ordering the main
# unit-test loop above uses.
target_link_libraries(test_xdr_layout_form PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    rondb_schema
    ${NTIRPC_LIB}
    pthread
)
add_test(NAME test_xdr_layout_form COMMAND test_xdr_layout_form)
set_tests_properties(test_xdr_layout_form PROPERTIES LABELS "unit")

# XDR codec test needs ntirpc (provided transitively by pnfs_mds_core)
target_link_libraries(test_xdr_codec PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    
    ${NTIRPC_LIB}
    pthread
)
add_test(NAME test_xdr_codec COMMAND test_xdr_codec)
set_tests_properties(test_xdr_codec PROPERTIES LABELS "unit")
# GSS test needs ntirpc + gssapi
target_link_libraries(test_mds_gss PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    
    ${NTIRPC_LIB}
    gssapi_krb5
    pthread
)

# nfs4_cb test needs ntirpc (CB_COMPOUND encoding uses XDR)
target_link_libraries(test_nfs4_cb PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    
    ${NTIRPC_LIB}
    pthread
)
add_test(NAME test_nfs4_cb COMMAND test_nfs4_cb)
set_tests_properties(test_nfs4_cb PROPERTIES LABELS "unit")

add_executable(test_proxy_io     unit/test_proxy_io.c)
target_include_directories(test_proxy_io PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/tools
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
target_link_libraries(test_proxy_io PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    
    pthread
)
add_test(NAME test_proxy_io COMMAND test_proxy_io)
set_tests_properties(test_proxy_io PROPERTIES LABELS "unit")

# TCP-based replication tests need longer timeout
# due to sleep-based synchronisation in the test harness.

# RPC server test: rpc_server.c is already in pnfs_mds_core.
add_executable(test_rpc_server   unit/test_rpc_server.c)
target_include_directories(test_rpc_server PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/tools
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
target_link_libraries(test_rpc_server PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    ${NTIRPC_LIB}
    pthread
)
add_test(NAME test_rpc_server COMMAND test_rpc_server)
set_tests_properties(test_rpc_server PROPERTIES LABELS "unit")

# Daemon integration test -- Item 52: lifecycle, compound, multi-listener
# add_executable(test_daemon_integration unit/test_daemon_integration.c
#                                        ${PROJECT_SOURCE_DIR}/src/mds/rpc_server.c)
# target_include_directories(test_daemon_integration PRIVATE ...)
# target_link_libraries(test_daemon_integration PRIVATE ...)
# add_test(NAME test_daemon_integration COMMAND test_daemon_integration)
# set_tests_properties(test_daemon_integration PROPERTIES LABELS "unit")

# --- Integration tests ---

add_executable(test_cluster          integration/test_cluster.c)
add_executable(test_failover         integration/test_failover.c)
add_executable(test_client_lifecycle integration/test_client_lifecycle.c)
add_executable(bench_create_layout_fusion
    integration/bench_create_layout_fusion.c)
add_executable(bench_mk_rm_scale
    integration/bench_mk_rm_scale.c)

foreach(test_target test_cluster test_failover test_client_lifecycle
                    bench_create_layout_fusion bench_mk_rm_scale)
    target_include_directories(${test_target} PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src/tools
        ${PROJECT_SOURCE_DIR}/src/catalogue
        ${PROJECT_SOURCE_DIR}/tests
    )
    target_link_libraries(${test_target} PRIVATE
        pnfs_mds_core
        pnfs_cluster
        pnfs_fsal_obj
        pnfs_common
        pnfs_mds_core
        pnfs_cluster
        pnfs_mds_core
        
        pthread
    )
    add_test(NAME ${test_target} COMMAND ${test_target})
    set_tests_properties(${test_target} PROPERTIES LABELS "integration")
endforeach()
# set_tests_properties(test_failover PROPERTIES TIMEOUT 120)
target_link_libraries(test_layout_recall PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    ${NTIRPC_LIB}
    pthread
)
target_include_directories(test_layout_recall PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/src/mds
    ${PROJECT_SOURCE_DIR}/tests
)
add_test(NAME test_layout_recall COMMAND test_layout_recall)
set_tests_properties(test_layout_recall PROPERTIES LABELS "unit")

# -------------------------------------------------------------------
# Patch 0005 -- per-DS-stripe lease keying unit tests (standalone).
#
# These tests build the implementation files directly so they avoid
# pulling in the full MDS / NDB stack and run in well under a second.
# -------------------------------------------------------------------

add_executable(test_lease_stripe_map
    unit/test_lease_stripe_map.c
    ${PROJECT_SOURCE_DIR}/src/fsal_obj/lease_stripe_map.c)
target_include_directories(test_lease_stripe_map PRIVATE
    ${PROJECT_SOURCE_DIR}/include)
add_test(NAME test_lease_stripe_map COMMAND test_lease_stripe_map)
set_tests_properties(test_lease_stripe_map PROPERTIES LABELS "unit")

add_executable(test_lease_table
    unit/test_lease_table.c
    ${PROJECT_SOURCE_DIR}/src/fsal_obj/lease_stripe_map.c
    ${PROJECT_SOURCE_DIR}/src/fsal_obj/lease_table.c)
target_compile_definitions(test_lease_table PRIVATE _POSIX_C_SOURCE=200809L)
target_include_directories(test_lease_table PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/fsal_obj)
target_link_libraries(test_lease_table PRIVATE pthread)
add_test(NAME test_lease_table COMMAND test_lease_table)
set_tests_properties(test_lease_table PROPERTIES LABELS "unit")

# -------------------------------------------------------------------
# cluster track (begin) -- backend-neutral cluster services extraction.
#
# test_cluster_extract drives subtree_map / cluster_membership /
# failover_watchdog / the image-feed dispatchers through fabricated
# struct mds_cluster_ops and exercises the backend registration table
# and the config parser's use of it.  It needs no backend beyond the
# memdb built into pnfs_mds_core.
# -------------------------------------------------------------------
add_executable(test_cluster_extract unit/test_cluster_extract.c)
target_include_directories(test_cluster_extract PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
target_link_libraries(test_cluster_extract PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    rondb_schema
    pthread
)
add_test(NAME test_cluster_extract COMMAND test_cluster_extract)
set_tests_properties(test_cluster_extract PROPERTIES LABELS "unit")
# cluster track (end)

# -------------------------------------------------------------------
# memdb track -- in-memory backend contract tests (concurrency,
# re-entrancy, isolation, capacity atomicity, cookies, layout union,
# lock_test, cluster contract).  Appended block; the backend itself is
# part of pnfs_mds_core.
# -------------------------------------------------------------------

add_executable(test_memdb_backend unit/test_memdb_backend.c)
target_include_directories(test_memdb_backend PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
target_link_libraries(test_memdb_backend PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    rondb_schema
    pthread
)
add_test(NAME test_memdb_backend COMMAND test_memdb_backend)
set_tests_properties(test_memdb_backend PROPERTIES LABELS "unit")
# End of memdb track block.

# -------------------------------------------------------------------
# harness track (Phase 6a/6b) -- backend-parameterised conformance
# suite and the catalogue tests that open their backend through it.
#
# The four catalogue unit tests select the backend from
# CATALOGUE_TEST_BACKEND (default memdb) via conformance_open(); an
# unavailable backend exits 77, which ctest maps to "Not Run".
# -------------------------------------------------------------------
add_subdirectory(catalogue_conformance)

foreach(harness_test test_catalogue test_rename_2pc test_open_state
                     test_layout_recall)
    target_sources(${harness_test} PRIVATE
        $<TARGET_OBJECTS:conformance_harness>)
    target_include_directories(${harness_test} PRIVATE
        ${PROJECT_SOURCE_DIR}/tests/catalogue_conformance)
    set_tests_properties(${harness_test} PROPERTIES SKIP_RETURN_CODE 77)
endforeach()

# Unprivileged daemon smoke: starts pnfs-mds on catalogue_backend=memdb
# on a high port and probes it.  Exits 77 (skipped) when the daemon
# refuses the memdb backend or no probe client is available.
add_test(NAME daemon_smoke
    COMMAND ${PROJECT_SOURCE_DIR}/tests/integration/daemon_smoke.sh
            ${CMAKE_BINARY_DIR})
set_tests_properties(daemon_smoke PROPERTIES
    LABELS "integration"
    SKIP_RETURN_CODE 77
    TIMEOUT 180)
# --- end harness track ---

# -------------------------------------------------------------------
# rondb-fix track -- backend-neutral pins for the namespace / recovery
# semantics the RonDB shim decides inside its transactions (RMDIR
# emptiness, one-step LINK, recovery-row ownership), run on the memdb
# reference backend so both backends stay at parity.  Appended block.
# -------------------------------------------------------------------
add_executable(test_rondb_ns_semantics unit/test_rondb_ns_semantics.c)
target_include_directories(test_rondb_ns_semantics PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
target_link_libraries(test_rondb_ns_semantics PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    rondb_schema
    pthread
)
add_test(NAME test_rondb_ns_semantics COMMAND test_rondb_ns_semantics)
set_tests_properties(test_rondb_ns_semantics PROPERTIES LABELS "unit")
# End of rondb-fix track block.

# -------------------------------------------------------------------
# Proxy SEEK_HOLE regression (mds_proxy_seek over fully written stripe
# units).  Same fixture shape and link line as test_proxy_io.
# -------------------------------------------------------------------
add_executable(test_proxy_seek_hole unit/test_proxy_seek_hole.c)
target_include_directories(test_proxy_seek_hole PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/tools
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
target_link_libraries(test_proxy_seek_hole PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    pthread
)
add_test(NAME test_proxy_seek_hole COMMAND test_proxy_seek_hole)
set_tests_properties(test_proxy_seek_hole PROPERTIES LABELS "unit")

# -------------------------------------------------------------------
# fdb track (Phase 5) -- FoundationDB backend foundation.
#
# test_fdb_codec compiles the pure-C codec straight into the binary so
# the value formats and the key registry are checked in every tree,
# ENABLE_FDB or not.  test_fdb_txn links the backend through
# pnfs_mds_core (HAVE_FDB is PUBLIC there); without ENABLE_FDB, or
# without a readable cluster file / reachable fdbserver, it exits 77,
# registered as SKIP_RETURN_CODE.  Appended block.
# -------------------------------------------------------------------
add_executable(test_fdb_codec unit/test_fdb_codec.c
                              ${PROJECT_SOURCE_DIR}/src/catalogue/fdb_codec.c)
target_include_directories(test_fdb_codec PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
)
add_test(NAME test_fdb_codec COMMAND test_fdb_codec)
set_tests_properties(test_fdb_codec PROPERTIES LABELS "unit")

add_executable(test_fdb_txn unit/test_fdb_txn.c)
target_include_directories(test_fdb_txn PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
if(ENABLE_FDB)
    target_include_directories(test_fdb_txn SYSTEM PRIVATE ${FDB_INCLUDE_DIR})
endif()
target_link_libraries(test_fdb_txn PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    rondb_schema
    pthread
)
add_test(NAME test_fdb_txn COMMAND test_fdb_txn)
set_tests_properties(test_fdb_txn PROPERTIES LABELS "unit" SKIP_RETURN_CODE 77)
# End of fdb track block.

# -------------------------------------------------------------------
# indoubt track (Phase 5 caller audit) -- MDS_ERR_INDOUBT.
#
# test_indoubt_mapping drives every enum mds_status value through the
# name and NFS mappings, then runs CREATE / OPEN(CREATE)+LAYOUTGET /
# REMOVE (fused, plain, async manifest) / RENAME / LAYOUTGET / LINK
# through compound_process against a counting proxy over the memdb
# backend whose slot under test answers MDS_ERR_INDOUBT: NFS4ERR_IO,
# no follow-up slot call, one placement pop, store unchanged.  Needs
# no external service.  Appended block.
# -------------------------------------------------------------------
add_executable(test_indoubt_mapping unit/test_indoubt_mapping.c)
target_include_directories(test_indoubt_mapping PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
target_link_libraries(test_indoubt_mapping PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    rondb_schema
    pthread
)
add_test(NAME test_indoubt_mapping COMMAND test_indoubt_mapping)
set_tests_properties(test_indoubt_mapping PROPERTIES LABELS "unit")
# End of indoubt track block.

# -------------------------------------------------------------------
# Lab instruments (not ctests), run by hand against a live cluster.
# lab_cluster_drive drives the public mds_cluster_* /
# mds_coord_recovery_* / mds_cat_ns_* dispatchers on whichever live
# store the tree has (RonDB via RONDB_CONF, FoundationDB via
# FDB_CLUSTER_FILE + FDB_KEY_PREFIX; --backend selects), so it is built
# whenever one of them is compiled in.  The NDB-API instruments stay
# RonDB-only: lab_nr_hb_set writes one registry row's last_heartbeat_ns
# through the NDB API for the mixed clock-domain (rolling upgrade)
# experiment; lab_dirent_raw shows the raw stored key bytes of one
# directory's dirent rows and reads each back by primary key (dangling
# / phantom dirent diagnosis).
# -------------------------------------------------------------------
if(ENABLE_RONDB OR ENABLE_FDB)
    add_executable(lab_cluster_drive integration/lab_cluster_drive.c)
    target_include_directories(lab_cluster_drive PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src/catalogue
        ${PROJECT_SOURCE_DIR}/tests
    )
    target_link_libraries(lab_cluster_drive PRIVATE
        pnfs_mds_core
        pnfs_cluster
        pnfs_fsal_obj
        pnfs_common
        pnfs_mds_core
        pnfs_cluster
        pnfs_mds_core
        rondb_schema
        pthread
    )
endif()

if(ENABLE_RONDB)
    add_executable(lab_nr_hb_set integration/lab_nr_hb_set.cpp)
    target_include_directories(lab_nr_hb_set PRIVATE ${RONDB_INCLUDE_DIR})
    target_compile_features(lab_nr_hb_set PRIVATE cxx_std_11)
    target_link_libraries(lab_nr_hb_set PRIVATE ${RONDB_LIBRARY} pthread)

    add_executable(lab_dirent_raw integration/lab_dirent_raw.cpp)
    target_include_directories(lab_dirent_raw PRIVATE ${RONDB_INCLUDE_DIR})
    target_compile_features(lab_dirent_raw PRIVATE cxx_std_11)
    target_link_libraries(lab_dirent_raw PRIVATE ${RONDB_LIBRARY} pthread)
endif()

# -------------------------------------------------------------------
# fdb-ext track (Phase 5) -- FoundationDB extended authority slots
# (src/catalogue/catalogue_fdb_ext.c: inline, xattr, stripe map, DS
# registry / provisioning, quota, GC, delete manifest, shard,
# ext_dirent, link_anchor).  Same shape as test_fdb_txn: links the
# backend through pnfs_mds_core and exits 77 without ENABLE_FDB or a
# reachable cluster.  Appended block.
# -------------------------------------------------------------------
add_executable(test_fdb_ext unit/test_fdb_ext.c)
target_include_directories(test_fdb_ext PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
if(ENABLE_FDB)
    target_include_directories(test_fdb_ext SYSTEM PRIVATE ${FDB_INCLUDE_DIR})
endif()
target_link_libraries(test_fdb_ext PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    rondb_schema
    pthread
)
add_test(NAME test_fdb_ext COMMAND test_fdb_ext)
set_tests_properties(test_fdb_ext PROPERTIES LABELS "unit" SKIP_RETURN_CODE 77)
# End of fdb-ext track block.

# -------------------------------------------------------------------
# fdb-coord track -- FoundationDB coordination and cluster slots
# (catalogue_fdb_coord.c, catalogue_fdb_cluster.c).  Same link line
# and skip rule as test_fdb_txn: exits 77 without ENABLE_FDB or a
# reachable local cluster.  Appended block.
# -------------------------------------------------------------------
add_executable(test_fdb_coord unit/test_fdb_coord.c)
target_include_directories(test_fdb_coord PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
)
if(ENABLE_FDB)
    target_include_directories(test_fdb_coord SYSTEM PRIVATE ${FDB_INCLUDE_DIR})
endif()
target_link_libraries(test_fdb_coord PRIVATE
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    rondb_schema
    pthread
)
add_test(NAME test_fdb_coord COMMAND test_fdb_coord)
set_tests_properties(test_fdb_coord PROPERTIES LABELS "unit" SKIP_RETURN_CODE 77)
# End of fdb-coord track block.

# -------------------------------------------------------------------
# Sanitizer suppressions for the FoundationDB client (ENABLE_FDB).
#
# pnfs_mds_core links libfdb_c PUBLIC, so every test that links the
# core archive -- and the daemon that daemon_smoke starts -- carries
# the uninstrumented client, which trips LeakSanitizer (allocations
# kept for the life of the network thread) and ThreadSanitizer (future
# completion it cannot see) on its own account.  tests/lsan-fdb.supp
# and tests/tsan-fdb.supp scope both down to this tree's code; ctest
# hands them over through the environment so `ctest -L unit` is green
# in an -fsanitize=address / -fsanitize=thread configure without any
# manual export.  Only the sanitizer runtimes read the variables, so a
# plain build is unaffected.  The tests that do not link the core
# archive (test_fdb_codec, test_lease_*) are left alone; the
# conformance suite sets the same property in its own CMakeLists.
# Must stay after the last add_test() of this directory: the TESTS
# directory property holds the tests registered so far.
# -------------------------------------------------------------------
if(ENABLE_FDB)
    set(PNFS_FDB_SANITIZER_ENV
        "LSAN_OPTIONS=suppressions=${PROJECT_SOURCE_DIR}/tests/lsan-fdb.supp"
        "TSAN_OPTIONS=suppressions=${PROJECT_SOURCE_DIR}/tests/tsan-fdb.supp")
    get_directory_property(pnfs_registered_tests TESTS)
    foreach(registered_test IN LISTS pnfs_registered_tests)
        if(NOT TARGET ${registered_test})
            continue()
        endif()
        get_target_property(registered_libs ${registered_test} LINK_LIBRARIES)
        if(registered_libs AND "pnfs_mds_core" IN_LIST registered_libs)
            set_property(TEST ${registered_test} APPEND PROPERTY
                ENVIRONMENT ${PNFS_FDB_SANITIZER_ENV})
        endif()
    endforeach()
    set_property(TEST daemon_smoke APPEND PROPERTY
        ENVIRONMENT ${PNFS_FDB_SANITIZER_ENV})
endif()
