# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# quota -- user/group quota enforcement.
#
# Enterprise build (ENABLE_QUOTA=ON, the default): real impl.
# Community build (ENABLE_QUOTA=OFF): all checks return MDS_OK,
# all updates are no-ops, all space queries return UINT64_MAX.
#
# 34 call sites in compound paths; the stub semantics are
# deliberately the same as the documented behaviour when no rules
# are configured ("unlimited"), so the daemon keeps serving
# correctly without enforcement.

if(ENABLE_QUOTA)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/quota.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/quota_stub.c)
    message(STATUS "quota: disabled (community build) -- using stubs (all checks allow)")
endif()
