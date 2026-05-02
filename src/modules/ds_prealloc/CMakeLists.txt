# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# ds_prealloc — DS placement pre-allocation pool for fast CREATE.

if(ENABLE_DS_PREALLOC)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/ds_prealloc.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/ds_prealloc_stub.c)
    message(STATUS "ds_prealloc: disabled (community build) — using stubs")
endif()
