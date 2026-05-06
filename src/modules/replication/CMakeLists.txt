# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# replication -- cluster health monitor for replicated MDS pairs.
#
# Enterprise build (ENABLE_REPLICATION=ON, the default): real impl
# linked into pnfs_mds_core.
#
# Community build (ENABLE_REPLICATION=OFF): no-op stubs that mirror
# the "no standby configured" path of the real implementation.
# health_repl_is_ok() returns true, health_writes_blocked() returns
# false; the daemon never refuses writes due to replication state.

# health.c originally lived in src/cluster/ (pnfs_cluster library) and
# is called from cluster_transport.c, so the module sources attach to
# pnfs_cluster rather than pnfs_mds_core.  This is the only difference
# from the resilver/rebalance/tiering pattern; everything else (option
# name, stub semantics, README convention) is identical.
if(ENABLE_REPLICATION)
    target_sources(pnfs_cluster PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/health.c)
else()
    target_sources(pnfs_cluster PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/health_stub.c)
    message(STATUS "replication: disabled (community build) -- using stubs")
endif()
