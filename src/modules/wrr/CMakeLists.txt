# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# wrr -- weighted-round-robin + capacity-derived placement kernels.
#
# Note: the wrr module attaches its sources to pnfs_fsal_obj because
# placement.c (the consumer) lives there, not in pnfs_mds_core.  The
# module API surface (include/wrr.h) is two functions called from
# placement_select_ex().

if(ENABLE_WRR)
    target_sources(pnfs_fsal_obj PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/wrr.c)
else()
    target_sources(pnfs_fsal_obj PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/wrr_stub.c)
    message(STATUS "wrr: disabled (community build) -- using stubs (degrades WEIGHTED_RR/CAPACITY to RR)")
endif()
