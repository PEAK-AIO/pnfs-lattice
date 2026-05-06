# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# layout_commit_aggregator -- batches LAYOUTCOMMIT updates per fileid,
# flushes them periodically to amortise NDB writes.

if(ENABLE_LAYOUT_COMMIT_AGGREGATOR)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/layout_commit_aggregator.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/layout_commit_aggregator_stub.c)
    message(STATUS "layout_commit_aggregator: disabled (community build) -- using stubs")
endif()
