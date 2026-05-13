# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# observability -- Prometheus /metrics HTTP endpoint.
#
# Enterprise build (ENABLE_OBSERVABILITY=ON, the default): real impl
# linked into pnfs_mds_core (was previously linked into the pnfs-mds
# executable directly; the move into pnfs_mds_core means tests can
# also reference the symbols without re-listing the source).
#
# Community build (ENABLE_OBSERVABILITY=OFF): no-op stubs.
# metrics_http_start returns 0 with a NULL ctx; no thread or port.

if(ENABLE_OBSERVABILITY)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/metrics_http.c)
else()
    # Community build: ship a real, minimal HTTP /metrics endpoint
    # so operators can scrape Prometheus without the enterprise
    # observability module.  The legacy no-op stub is still
    # available at metrics_http_stub.c for builds that genuinely
    # want zero networking surface.
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/metrics_http_lite.c)
    message(STATUS "observability: community build -- using metrics_http_lite")
endif()
