# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# Optional / community-vs-enterprise gated modules.
#
# Each subdirectory below corresponds to one feature that can be
# compiled in (real implementation) or compiled out (no-op stubs)
# via an option(ENABLE_<NAME>) declared in the top-level CMakeLists.
#
# Convention for each module:
#   src/modules/<name>/CMakeLists.txt
#       if(ENABLE_<NAME>)
#           target_sources(pnfs_mds_core PRIVATE <name>.c)
#       else()
#           target_sources(pnfs_mds_core PRIVATE <name>_stub.c)
#       endif()
#   src/modules/<name>/<name>.c       -- real implementation
#   src/modules/<name>/<name>_stub.c  -- no-op stubs (community build)
#
# The header (include/<name>.h) stays unchanged so call sites do not
# move and the API surface is identical between enterprise and
# community builds -- only the implementation differs.
#
# This file must be included AFTER add_subdirectory(src/cluster) and
# add_subdirectory(src/mds) so that the pnfs_cluster and pnfs_mds_core
# targets already exist when each module attempts target_sources() on
# them.  Most modules attach to pnfs_mds_core; the `replication`
# module attaches to pnfs_cluster (its original home).

add_subdirectory(resilver)
add_subdirectory(rebalance)
add_subdirectory(tiering)
add_subdirectory(observability)
add_subdirectory(replication)
add_subdirectory(layout_cache)
add_subdirectory(layout_commit_aggregator)
add_subdirectory(ds_gc)
add_subdirectory(ds_prealloc)
add_subdirectory(wrr)
add_subdirectory(quota)
