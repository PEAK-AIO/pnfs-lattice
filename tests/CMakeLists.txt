# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

enable_testing()

# In-memory catalogue backend for unit tests (no external dependencies).
add_library(test_memdb STATIC catalogue_memdb.c)
target_include_directories(test_memdb PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
)
target_link_libraries(test_memdb PRIVATE pthread)

# --- Unit tests ---

add_executable(test_layout       unit/test_layout.c)
add_executable(test_xdr_layout_form unit/test_xdr_layout_form.c)
add_executable(test_rename_2pc   unit/test_rename_2pc.c)
add_executable(test_subtree_map  unit/test_subtree_map.c)
add_executable(test_inode_cache  unit/test_inode_cache.c)
add_executable(test_compound     unit/test_compound.c)
add_executable(test_cluster_transport unit/test_cluster_transport.c)
add_executable(test_compound_cluster  unit/test_compound_cluster.c)
add_executable(test_session      unit/test_session.c)
add_executable(test_open_state   unit/test_open_state.c)
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
if(ENABLE_DS_PREALLOC)
    add_executable(test_ds_prealloc       unit/test_ds_prealloc.c)
    add_executable(test_ds_prealloc_batch unit/test_ds_prealloc_batch.c)
endif()
add_executable(test_open_exclusive  unit/test_open_exclusive.c)
add_executable(test_mds_shard unit/test_mds_shard.c)
add_executable(test_mds_admin       unit/test_mds_admin.c
                                    ${PROJECT_SOURCE_DIR}/src/tools/admin_util.c)
add_executable(test_catalogue       unit/test_catalogue.c)
add_executable(test_catalog_delta   unit/test_catalog_delta.c)
add_executable(test_catalog_image   unit/test_catalog_image.c)
add_executable(test_rondb_schema    unit/test_rondb_schema.c)
add_executable(test_rondb_lock_shim unit/test_rondb_lock_shim.c)
add_executable(test_rondb_changefeed unit/test_rondb_changefeed.c)
add_executable(test_coord_shared_state unit/test_coord_shared_state.c)
add_executable(test_dir_delegation  unit/test_dir_delegation.c)
add_executable(test_delegation      unit/test_delegation.c)
add_executable(test_ds_capacity     unit/test_ds_capacity.c)
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
    test_compound
    test_cluster_transport
    test_compound_cluster
    test_session
    test_open_state
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
    test_dir_delegation
    test_delegation
    test_ds_capacity
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
        test_memdb
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
target_link_libraries(test_xdr_layout_form PRIVATE
    test_memdb
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    rondb_schema
    ${NTIRPC_LIB}
    pthread
)
add_test(NAME test_xdr_layout_form COMMAND test_xdr_layout_form)
set_tests_properties(test_xdr_layout_form PROPERTIES LABELS "unit")

# XDR codec test needs ntirpc (provided transitively by pnfs_mds_core)
target_link_libraries(test_xdr_codec PRIVATE
    test_memdb
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
    test_memdb
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
    test_memdb
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
    test_memdb
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
    test_memdb
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

foreach(test_target test_cluster test_failover test_client_lifecycle
                    bench_create_layout_fusion)
    target_include_directories(${test_target} PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src/tools
        ${PROJECT_SOURCE_DIR}/src/catalogue
        ${PROJECT_SOURCE_DIR}/tests
    )
    target_link_libraries(${test_target} PRIVATE
        test_memdb
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
    test_memdb
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
