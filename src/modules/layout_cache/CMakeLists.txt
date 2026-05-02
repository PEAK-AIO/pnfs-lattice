# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# layout_cache — sharded LRU cache of stripe maps to skip RonDB
# round trips on hot LAYOUTGET paths.
#
# Enterprise build (ENABLE_LAYOUT_CACHE=ON, the default): real LRU.
# Community build (ENABLE_LAYOUT_CACHE=OFF): every lookup misses,
# every insert is dropped — correct, just no caching.

if(ENABLE_LAYOUT_CACHE)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/layout_cache.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/layout_cache_stub.c)
    message(STATUS "layout_cache: disabled (community build) — using stubs")
endif()
