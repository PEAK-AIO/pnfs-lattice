# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

add_library(pnfs_cluster STATIC
    grpc_service.c
    rename_2pc.c
    hardlink_2pc.c
    cluster_transport.c
    migration.c
    subtree_map.c
    failover.c
    failover_rondb.c
    # health.c moved to src/modules/replication/ (gated by ENABLE_REPLICATION)
    subtree_split.c
    cluster_membership.c
    cluster_drain.c
)

target_include_directories(pnfs_cluster PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(pnfs_cluster
    PUBLIC
        pnfs_common
        
        pthread
)
