# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# resilver — background resilvering for degraded mirrored files.
#
# Enterprise build (ENABLE_RESILVER=ON, the default): links the real
# implementation in resilver.c.
#
# Community build (ENABLE_RESILVER=OFF): links resilver_stub.c, which
# turns every resilver_* call into a safe no-op.  Callers that
# null-check the worker handle (cluster_transport handlers, mds-admin
# RPC paths) short-circuit cleanly.

if(ENABLE_RESILVER)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/resilver.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/resilver_stub.c)
    message(STATUS "resilver: disabled (community build) — using stubs")
endif()
