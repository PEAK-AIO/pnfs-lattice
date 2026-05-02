# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# FindRonDB.cmake — locate libndbclient (RonDB / MySQL NDB Cluster)
#
# Sets:
#   RONDB_FOUND
#   RONDB_INCLUDE_DIR
#   RONDB_LIBRARY
#   RonDB_ROOT (optional cache hint)

set(RonDB_ROOT "${RonDB_ROOT}" CACHE PATH "RonDB installation prefix")
if(NOT RonDB_ROOT AND DEFINED RONDB_ROOT AND NOT "${RONDB_ROOT}" STREQUAL "")
    set(RonDB_ROOT "${RONDB_ROOT}")
endif()

file(GLOB _RONDB_GLOB_HINTS LIST_DIRECTORIES true
    "/opt/rondb*"
    "/usr/local/rondb*"
)

set(_RONDB_HINTS
    ${RonDB_ROOT}
    ${RONDB_ROOT}
    $ENV{RONDB_ROOT}
    /opt/rondb
    /opt/rondb/current
    /usr/local/rondb
    /usr/local/rondb/current
    ${_RONDB_GLOB_HINTS}
)

list(REMOVE_DUPLICATES _RONDB_HINTS)

find_path(RONDB_INCLUDE_DIR
    NAMES ndbapi/NdbApi.hpp
    HINTS ${_RONDB_HINTS}
    PATH_SUFFIXES
        include/storage/ndb
        include/mysql/storage/ndb
        include
        include/mysql
    PATHS
        /usr/include
        /usr/include/mysql
        /usr/local/include
        /usr/local/include/mysql
)

find_library(RONDB_LIBRARY
    NAMES ndbclient ndbclient_static
    HINTS ${_RONDB_HINTS}
    PATH_SUFFIXES
        lib
        lib64
        lib/mysql
        lib64/mysql
    PATHS
        /usr/lib /usr/lib64
        /usr/local/lib
        /usr/local/lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RonDB
    DEFAULT_MSG
    RONDB_LIBRARY
    RONDB_INCLUDE_DIR
)

if(RONDB_FOUND)
    mark_as_advanced(RonDB_ROOT RONDB_INCLUDE_DIR RONDB_LIBRARY)
endif()

unset(_RONDB_GLOB_HINTS)

unset(_RONDB_HINTS)
