# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# tiering -- DS storage tiering (promote/demote based on I/O scores).
#
# Enterprise build (ENABLE_TIERING=ON, the default): real implementation.
# Community build (ENABLE_TIERING=OFF): no-op stubs; tiering_start
# returns MDS_ERR_INVAL.

if(ENABLE_TIERING)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tiering.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tiering_stub.c)
    message(STATUS "tiering: disabled (community build) -- using stubs")
endif()
