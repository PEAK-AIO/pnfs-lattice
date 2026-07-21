# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

# Core MDS modules -- shared between the daemon and unit tests.
add_library(pnfs_mds_core STATIC
    compound.c
    compound_access.c
    compound_session.c
    compound_namespace.c
    compound_data_io.c
    compound_layout.c
    compound_nfsv42.c
    session.c
    inode_cache.c
    dirent_cache.c
    delegation.c
    dir_delegation.c
    hpc_shared.c
    # layout_cache.c moved to src/modules/layout_cache/ (gated by ENABLE_LAYOUT_CACHE)
    # layout_commit_aggregator.c moved to src/modules/layout_commit_aggregator/ (gated by ENABLE_LAYOUT_COMMIT_AGGREGATOR)
    grace.c
    proxy_io.c
    open_state.c
    lock_state.c
    xdr_codec.c
    xdr_ops_core.c
    xdr_ops_layout.c
    xdr_ops_nfsv42.c
    xdr_ops_dir_deleg.c
    copy_offload.c
    commit_queue.c
    # quota.c moved to src/modules/quota/ (gated by ENABLE_QUOTA)
    nfs4_cb.c
    ds_health.c
    # ds_prealloc.c moved to src/modules/ds_prealloc/ (gated by ENABLE_DS_PREALLOC)
    ds_prepare.c
    ds_nfs_rpc.c
    mountd_compat.c
    ds_cache.c
    ds_capacity.c
    # ds_gc.c moved to src/modules/ds_gc/ (gated by ENABLE_DS_GC)
    layout_recall.c
    # resilver.c moved to src/modules/resilver/ (gated by ENABLE_RESILVER)
    # rebalance.c moved to src/modules/rebalance/ (gated by ENABLE_REBALANCE)
    mover_util.c
    io_tracker.c
    # tiering.c moved to src/modules/tiering/ (gated by ENABLE_TIERING)
    backup.c
    mds_gss.c
    mds_shard.c
    rpc_server.c
    ${PROJECT_SOURCE_DIR}/src/catalogue/catalogue_dispatch.c
    ${PROJECT_SOURCE_DIR}/src/catalogue/catalogue_factory.c
    ${PROJECT_SOURCE_DIR}/src/catalogue/catalog_delta.c
    ${PROJECT_SOURCE_DIR}/src/catalogue/catalog_image.c
)

# Pure-C RonDB serialisation helpers.  Built unconditionally because
# they have no NDB dependency and are needed by unit tests even when
# ENABLE_RONDB=OFF.
add_library(rondb_schema STATIC
    ${PROJECT_SOURCE_DIR}/src/catalogue/rondb_schema.c
)
target_include_directories(rondb_schema PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)
target_include_directories(rondb_schema PRIVATE
    ${PROJECT_SOURCE_DIR}/src/catalogue
)

# Conditional backend sources.
if(ENABLE_RONDB)
    add_library(rondb_shim STATIC
        ${PROJECT_SOURCE_DIR}/src/catalogue/catalogue_rondb_shim.cpp
    )
    target_include_directories(rondb_shim PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${RONDB_INCLUDE_DIR}
    )
    target_compile_features(rondb_shim PRIVATE cxx_std_11)
    target_compile_definitions(rondb_shim PRIVATE HAVE_RONDB=1)
    target_link_libraries(rondb_shim PUBLIC rondb_schema)
    # Link shim + C wrapper into core.
    target_sources(pnfs_mds_core PRIVATE
        ${PROJECT_SOURCE_DIR}/src/catalogue/catalogue_rondb.c
    )
    target_link_libraries(pnfs_mds_core PUBLIC rondb_shim)
    if(RONDB_LIBRARY)
        target_link_libraries(rondb_shim PUBLIC ${RONDB_LIBRARY})
    endif()
endif()

target_include_directories(pnfs_mds_core
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PROJECT_SOURCE_DIR}/src/catalogue
)

target_include_directories(pnfs_mds_core SYSTEM
    PUBLIC ${NTIRPC_INCLUDE_DIR}/ntirpc
)

# Suppress pedantic errors from ntirpc system headers (zero-size array)
target_compile_options(pnfs_mds_core PRIVATE -Wno-error=pedantic)

target_link_libraries(pnfs_mds_core
    PUBLIC
        pnfs_common
        pnfs_fsal_obj
        pnfs_cluster
        OpenSSL::Crypto
        gssapi_krb5
        ${NTIRPC_LIB}
)

# The daemon executable.  rpc_server.c is built into pnfs_mds_core
# (so unit tests get rpc_conn_get_fd, etc. transitively); main.c is
# the only remaining executable-only source.  metrics_http.c moved
# to src/modules/observability/ and is now contributed to
# pnfs_mds_core via the module CMakeLists (gated by
# ENABLE_OBSERVABILITY).
add_executable(pnfs-mds
    main.c
)

target_link_libraries(pnfs-mds
    PRIVATE
        pnfs_mds_core
        pnfs_cluster
        pnfs_fsal_obj
        ${NTIRPC_LIB}
        
        pthread
)

if(ENABLE_RONDB)
    set_target_properties(pnfs-mds PROPERTIES LINKER_LANGUAGE CXX)
endif()

# Install targets -- used by 'cmake --install' or 'make install'.
install(TARGETS pnfs-mds RUNTIME DESTINATION bin)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
        DESTINATION include/pnfs-mds
        FILES_MATCHING PATTERN "*.h")
