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

# Backend probe tool -- validates RonDB catalogue connectivity and
# drives the RonDB bootstrap/cleanup.  RonDB-specific, so it is built
# only when the RonDB backend is.
if(ENABLE_RONDB)
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
endif()

# Metadata search CLI -- find-style predicates answered by one backend
# table scan instead of an NFS namespace walk.
add_executable(mds-find
    mds_find.c
    find_query.c
    admin_util.c
)

target_include_directories(mds-find PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(mds-find
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

# Read-only HTTP query service -- lets clients search the namespace
# without an NFS mount and without any metadata-backend access.
add_executable(mds-apid
    mds_apid.c
    apid_http.c
    find_query.c
    admin_util.c
)

target_include_directories(mds-apid PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(mds-apid
    PRIVATE
        pnfs_mds_core
        pnfs_cluster
        pnfs_fsal_obj
        pnfs_common
        pnfs_mds_core
        pnfs_cluster
        ${NTIRPC_LIB}
        OpenSSL::SSL
        OpenSSL::Crypto

        pthread
)

# RonDB micro-benchmark: raw ns_create without NFS/XDR overhead.
# RonDB-specific (opens the backend directly), built only with it.
if(ENABLE_RONDB)
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
endif()

# mds-find / mds-apid stay backend-neutral executables: their RonDB
# table scans are compiled out in find_query.c without HAVE_RONDB
# (the query core then reports MDS_ERR_NOSUPPORT for scan predicates
# and still serves exact-fileid lookups through the dispatcher).

if(ENABLE_RONDB)
    set_target_properties(mds-admin mds-backend-probe bench-rondb-create
                          mds-find mds-apid
        PROPERTIES LINKER_LANGUAGE CXX)
endif()

# The metadata search surface: the CLI, the read-only HTTP service, and
# the client-side wrapper that talks to it.  The wrapper is installed
# alongside them so the same package can be used on an NFS client that
# has no backend access.
install(TARGETS mds-find mds-apid RUNTIME DESTINATION bin)
install(PROGRAMS ${PROJECT_SOURCE_DIR}/scripts/lattice-find
        DESTINATION bin)
