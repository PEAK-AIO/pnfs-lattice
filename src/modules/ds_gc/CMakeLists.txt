# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# ds_gc -- background drainer for the catalogue GC queue.

if(ENABLE_DS_GC)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/ds_gc.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/ds_gc_stub.c)
    message(STATUS "ds_gc: disabled (community build) -- using stubs")
endif()
