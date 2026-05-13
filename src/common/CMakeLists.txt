# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

add_library(pnfs_common STATIC
    log.c
    threadpool.c
    heartbeat.c
    config.c
    error.c
    mds_histogram.c
    mds_metrics.c
    mds_op_metrics.c
    mds_tls.c
)

# rondb_stubs.c provides fallback symbols (catalogue_rondb_layoutget_fused,
# catalogue_rondb_ns_create_with_layout, rondb_shim_fileid_batch_alloc,
# rondb_shim_bench_create) for builds that did NOT find RonDB headers.
# When ENABLE_RONDB is on, catalogue_rondb.c and catalogue_rondb_shim.cpp
# provide the real implementations; linking both produces multiple-definition
# linker errors.  Compile the stubs only when the real backend is absent.
if(NOT ENABLE_RONDB)
    target_sources(pnfs_common PRIVATE rondb_stubs.c)
endif()

target_include_directories(pnfs_common PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(pnfs_common
    PUBLIC
        pthread
        OpenSSL::SSL
        OpenSSL::Crypto
)
