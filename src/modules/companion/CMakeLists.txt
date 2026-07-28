# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# companion -- supervisor for operator-declared helper processes.

if(ENABLE_COMPANION)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/companion.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/companion_stub.c)
    message(STATUS "companion: disabled -- using stubs")
endif()
