# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# rebalance -- admin-triggered mirror relocation between data servers.
#
# Enterprise build (ENABLE_REBALANCE=ON, the default): links the real
# implementation in rebalance.c.
#
# Community build (ENABLE_REBALANCE=OFF): links rebalance_stub.c, which
# turns every rebalance_* call into a safe no-op (NULL worker handle,
# rebalance_start returns MDS_ERR_INVAL).

if(ENABLE_REBALANCE)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/rebalance.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/rebalance_stub.c)
    message(STATUS "rebalance: disabled (community build) -- using stubs")
endif()
