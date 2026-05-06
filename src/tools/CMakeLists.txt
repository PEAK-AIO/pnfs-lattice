# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

add_executable(mds-admin
    mds_admin.c
    admin_util.c
)

target_include_directories(mds-admin PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(mds-admin
    PRIVATE
        pnfs_mds_core
        pnfs_cluster
        pnfs_fsal_obj
        pnfs_common
        pnfs_mds_core
        pnfs_cluster
        
        pthread
)

# Backend probe tool -- validates catalogue backend connectivity.
add_executable(mds-backend-probe
    mds_backend_probe.c
)

target_include_directories(mds-backend-probe PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(mds-backend-probe
    PRIVATE
        pnfs_mds_core
        pnfs_cluster
        pnfs_fsal_obj
        pnfs_common
        pnfs_mds_core
        
        pthread
)

# RonDB micro-benchmark: raw ns_create without NFS/XDR overhead.
add_executable(bench-rondb-create
    bench_rondb_create.c
)

target_include_directories(bench-rondb-create PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
)

target_link_libraries(bench-rondb-create
    PRIVATE
        pnfs_mds_core
        pnfs_cluster
        pnfs_fsal_obj
        pnfs_common
        pnfs_mds_core
        pnfs_cluster
        ${NTIRPC_LIB}
        
        pthread
)

if(ENABLE_RONDB)
    set_target_properties(mds-admin mds-backend-probe bench-rondb-create
        PROPERTIES LINKER_LANGUAGE CXX)
endif()
