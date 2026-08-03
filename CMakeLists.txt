# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

cmake_minimum_required(VERSION 3.20)

# Cluster build nodes often have a full /tmp.  GCC writes intermediate .s
# files there during CMake's compiler sanity check (before project()).
# Redirect compiler temps into the out-of-tree build directory unless the
# caller opted out with PNFS_KEEP_TMPDIR=1 in the environment.
if(NOT DEFINED ENV{PNFS_KEEP_TMPDIR})
    if(CMAKE_BINARY_DIR)
        get_filename_component(_PNFS_BUILD_TMP "${CMAKE_BINARY_DIR}/tmp" ABSOLUTE)
    else()
        get_filename_component(_PNFS_BUILD_TMP
            "${CMAKE_CURRENT_SOURCE_DIR}/build/tmp" ABSOLUTE)
    endif()
    file(MAKE_DIRECTORY "${_PNFS_BUILD_TMP}")
    if(NOT DEFINED ENV{TMPDIR}
            OR "$ENV{TMPDIR}" STREQUAL "/tmp"
            OR "$ENV{TMPDIR}" MATCHES "^/tmp/")
        set(ENV{TMPDIR} "${_PNFS_BUILD_TMP}")
        set(ENV{TEMP} "${_PNFS_BUILD_TMP}")
        set(ENV{TMP} "${_PNFS_BUILD_TMP}")
        message(STATUS "TMPDIR=${_PNFS_BUILD_TMP} (redirected from system /tmp)")
    endif()
endif()

project(pnfs-mds
    VERSION 0.1.0
    DESCRIPTION "pNFS Active-Active MDS with Flex File Layouts"
    LANGUAGES C
)

# Standard GNU install directory layout: gives us ${CMAKE_INSTALL_MANDIR}
# (typically `share/man`) for the man pages installed below.
include(GNUInstallDirs)

# ---------------------------------------------------------------------------
# Build options
# ---------------------------------------------------------------------------
option(ENABLE_EBPF       "Build eBPF replication monitoring programs" ON)
option(ENABLE_ADMIN_CLI  "Build the mds-admin CLI tool"               ON)
option(ENABLE_TESTS      "Build unit and integration tests"           ON)
option(ENABLE_RONDB      "Build RonDB catalogue backend"              ON)
# Compile-time kill-switch for per-op latency / phase / cat-op
# histograms.  OFF removes every observation site from the binary
# (defines MDS_OP_METRICS_DISABLE), leaving only the dispatcher
# metrics in threadpool.c.  Default ON: runtime toggling via
# `metrics_op_enabled = false` in mds.conf is usually preferable.
option(ENABLE_OP_METRICS "Compile per-op latency / phase histograms"  ON)
if(NOT ENABLE_OP_METRICS)
    add_compile_definitions(MDS_OP_METRICS_DISABLE=1)
endif()

# ---------------------------------------------------------------------------
# Community-edition module gates.
#
# These modules are STUB-ONLY in the community edition: the real .c
# implementations live in the enterprise tree and are not shipped
# here.  The defaults are forced OFF so the build always selects the
# stub variant.  Flipping any of these to ON will fail at build time
# because the corresponding <name>.c file does not exist; that is
# intentional -- it is the build-system's way of saying "this module
# is enterprise-only".
# ---------------------------------------------------------------------------
option(ENABLE_RESILVER   "Enable resilver module (enterprise-only)" OFF)
option(ENABLE_REBALANCE  "Enable rebalance module (enterprise-only)" OFF)
option(ENABLE_TIERING    "Enable tiering module (enterprise-only)" OFF)
option(ENABLE_OBSERVABILITY "Enable observability module (enterprise-only)" OFF)
option(ENABLE_REPLICATION   "Enable replication module (enterprise-only)" OFF)
option(ENABLE_LAYOUT_CACHE  "Enable layout cache module (enterprise-only)" ON)
option(ENABLE_LAYOUT_COMMIT_AGGREGATOR "Enable LAYOUTCOMMIT aggregator module (enterprise-only)" OFF)
option(ENABLE_DS_GC         "Enable ds_gc module (enterprise-only)" ON)
option(ENABLE_DS_PREALLOC   "Enable ds_prealloc module (enterprise-only)" OFF)
option(ENABLE_WRR           "Enable wrr module (enterprise-only)" OFF)
option(ENABLE_QUOTA         "Enable quota module (enterprise-only)" OFF)

if(ENABLE_RONDB)
    enable_language(CXX)
endif()

# ---------------------------------------------------------------------------
# Version string
#
# include/pnfs_mds.h defines a default PNFS_MDS_VERSION (built from the
# PNFS_MDS_VERSION_* integer macros) behind an #ifndef guard.  Package builds
# (deb/rpm) compile from a tree with no .git and pin the human-facing version
# string by passing -DPNFS_MDS_VERSION_OVERRIDE=<str> (e.g. v0.1.0-community).
# When set, define PNFS_MDS_VERSION globally so the daemon and the tools --
# notably `mds-admin --version` -- report the packaged version.  The header's
# #ifndef guard lets this build-time definition win without a redefinition.
# ---------------------------------------------------------------------------
if(DEFINED PNFS_MDS_VERSION_OVERRIDE AND NOT PNFS_MDS_VERSION_OVERRIDE STREQUAL "")
    add_compile_definitions(PNFS_MDS_VERSION="${PNFS_MDS_VERSION_OVERRIDE}")
    message(STATUS "pnfs-mds version override: ${PNFS_MDS_VERSION_OVERRIDE}")
endif()

# ---------------------------------------------------------------------------
# Git commit
#
# Record the short commit the build is cut from so `mds-admin --version` can
# report it.  Package builds (deb/rpm) stage a tree with no .git and pass
# -DPNFS_MDS_GIT_COMMIT_OVERRIDE=<sha>; in-tree dev builds fall back to
# `git rev-parse`.  Either way PNFS_MDS_GIT_COMMIT is defined globally.
# ---------------------------------------------------------------------------
if(DEFINED PNFS_MDS_GIT_COMMIT_OVERRIDE AND NOT PNFS_MDS_GIT_COMMIT_OVERRIDE STREQUAL "")
    set(PNFS_MDS_GIT_COMMIT_STR "${PNFS_MDS_GIT_COMMIT_OVERRIDE}")
else()
    set(PNFS_MDS_GIT_COMMIT_STR "unknown")
    find_package(Git QUIET)
    if(GIT_EXECUTABLE)
        execute_process(
            COMMAND "${GIT_EXECUTABLE}" rev-parse --short HEAD
            WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
            OUTPUT_VARIABLE PNFS_MDS_GIT_SHA
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
            RESULT_VARIABLE PNFS_MDS_GIT_RC)
        if(PNFS_MDS_GIT_RC EQUAL 0 AND NOT PNFS_MDS_GIT_SHA STREQUAL "")
            set(PNFS_MDS_GIT_COMMIT_STR "${PNFS_MDS_GIT_SHA}")
        endif()
    endif()
endif()
add_compile_definitions(PNFS_MDS_GIT_COMMIT="${PNFS_MDS_GIT_COMMIT_STR}")
message(STATUS "pnfs-mds git commit: ${PNFS_MDS_GIT_COMMIT_STR}")

# ---------------------------------------------------------------------------
# Toolchain gate -- GCC >= 11.1 is mandatory.
#
# GCC 11.x ships with Rocky/RHEL 9, GCC 14.x with Rocky/RHEL 10.
# The gate was lowered from 13.1 to 11.1 to cover both Rocky 9 and 10.
# GCC 12, 14, and 15 are fully supported.
# ---------------------------------------------------------------------------
if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_C_COMPILER_VERSION VERSION_LESS "11.1")
        message(FATAL_ERROR
            "pnfs-mds requires GCC >= 11.1 (found ${CMAKE_C_COMPILER_VERSION}). "
            "See docs/architecture.md section 20 for details.")
    endif()
else()
    message(WARNING
        "pnfs-mds is only tested with GCC >= 11.1 "
        "(found ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION})")
endif()

# ---------------------------------------------------------------------------
# Global compiler settings -- C11, strict warnings, K&R style assumed
# ---------------------------------------------------------------------------
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

add_compile_options(
    -Wall -Wextra -Wpedantic -Werror
    -Wshadow
    $<$<COMPILE_LANGUAGE:C>:-Wstrict-prototypes>
    $<$<COMPILE_LANGUAGE:C>:-Wmissing-prototypes>
    $<$<COMPILE_LANGUAGE:C>:-Wold-style-definition>
    -Wformat=2 -Wformat-security
    # -Wdeclaration-after-statement  # disabled: project targets C11, code uses C99 mixed decls
    -Wno-unused-parameter          # too noisy during scaffolding phase
    $<$<COMPILE_LANGUAGE:C>:-Wno-error=missing-prototypes>
    -Wno-error=unused-result
    -Wno-error=unused-variable
    -Wno-error=unused-function
    -Wno-error=uninitialized
    -fstack-protector-strong
    -D_GNU_SOURCE
    -UNDEBUG # always define "assert()" in assert.h, even for Release builds
)

# GCC-specific flags that clang does not understand.
if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    add_compile_options(
        -Wno-error=maybe-uninitialized
        -Wno-error=unused-but-set-variable
    )
    # -Wuse-after-free was added in GCC 12; older versions reject the flag.
    if(NOT CMAKE_C_COMPILER_VERSION VERSION_LESS "12.0")
        add_compile_options(-Wno-error=use-after-free)
    endif()
endif()

# ---------------------------------------------------------------------------
# Global include paths
# ---------------------------------------------------------------------------
include_directories(
    ${PROJECT_SOURCE_DIR}/include
)

# ---------------------------------------------------------------------------
# Required dependencies
# ---------------------------------------------------------------------------
list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# LMDB removed -- RonDB is the only supported backend.

# pthreads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

# libntirpc -- NFS RPC + optional RDMA control plane.
#
# On Debian/Ubuntu: apt install libntirpc-dev
# On Rocky/RHEL 10 (not in repos): run deps/build-ntirpc.sh
# See docs/INSTALL_ROCKY.md for details.
find_library(NTIRPC_LIB ntirpc)
find_path(NTIRPC_INCLUDE_DIR ntirpc/rpc/rpc.h)

if(NOT NTIRPC_LIB OR NOT NTIRPC_INCLUDE_DIR)
    message(FATAL_ERROR
        "libntirpc not found.\n"
        "  Debian/Ubuntu:  sudo apt install libntirpc-dev\n"
        "  Rocky/RHEL 10:  run deps/build-ntirpc.sh   (builds from source)\n"
        "  See docs/INSTALL_ROCKY.md for the full dependency list.")
endif()
message(STATUS "libntirpc: ${NTIRPC_LIB}")

# gRPC + protobuf -- inter-MDS communication (optional until implemented)
# grpc_service.c is currently stubbed; require when Phase 7 lands.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(GRPC QUIET grpc)
    pkg_check_modules(PROTOBUF QUIET protobuf)
endif()
if(PkgConfig_FOUND AND GRPC_FOUND AND PROTOBUF_FOUND)
    add_compile_definitions(HAVE_GRPC=1)
    message(STATUS "gRPC found -- inter-MDS gRPC service enabled")
else()
    message(WARNING
        "pkg-config or gRPC/protobuf not found -- inter-MDS gRPC service "
        "disabled (stubs only)")
endif()


# RonDB -- optional distributed catalogue backend
if(ENABLE_RONDB)
    find_package(RonDB)
    if(RONDB_FOUND)
        add_compile_definitions(HAVE_RONDB=1)  # Global: needed by catalogue_rondb.c
        message(STATUS "RonDB found -- RonDB catalogue backend enabled")
    else()
        message(WARNING "RonDB not found -- RonDB backend disabled")
        set(ENABLE_RONDB OFF)
    endif()
endif()

# libbpf -- eBPF replication monitoring (optional)
if(ENABLE_EBPF)
    if(PkgConfig_FOUND)
        pkg_check_modules(LIBBPF libbpf)
    endif()
    if(NOT PkgConfig_FOUND OR NOT LIBBPF_FOUND)
        message(WARNING
            "pkg-config or libbpf not found -- eBPF monitoring disabled")
        set(ENABLE_EBPF OFF)
    endif()
endif()

# ---------------------------------------------------------------------------
# Subdirectories
# ---------------------------------------------------------------------------
add_subdirectory(src/common)
add_subdirectory(src/cluster)
add_subdirectory(src/fsal_obj)
add_subdirectory(src/mds)

# Optional modules -- must come AFTER src/mds so pnfs_mds_core exists
# before each module attempts target_sources() on it.
add_subdirectory(src/modules)

if(ENABLE_EBPF)
    add_subdirectory(src/bpf)
endif()

if(ENABLE_ADMIN_CLI)
    add_subdirectory(src/tools)
endif()

if(ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# QA targets (see scripts/qa-check.sh and CONTRIBUTING.md section 10)
# ---------------------------------------------------------------------------

# Full QA pipeline (all gates)
add_custom_target(qa
    COMMAND ${PROJECT_SOURCE_DIR}/scripts/qa-check.sh --continue
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Running full QA gate pipeline"
    USES_TERMINAL
)

# Quick QA (skip valgrind and integration)
add_custom_target(qa-quick
    COMMAND ${PROJECT_SOURCE_DIR}/scripts/qa-check.sh --quick --continue
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Running quick QA gates (no valgrind)"
    USES_TERMINAL
)

# Style check only
add_custom_target(check-style
    COMMAND ${PROJECT_SOURCE_DIR}/scripts/check-style.sh
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Running code style checks"
    USES_TERMINAL
)

# cppcheck only
find_program(CPPCHECK_BIN cppcheck)
if(CPPCHECK_BIN)
    add_custom_target(cppcheck
        COMMAND ${CPPCHECK_BIN}
            --enable=all
            --std=c11
            --error-exitcode=1
            --suppress=missingIncludeSystem
            --suppress=unusedFunction
            --suppress=unmatchedSuppression
            -I ${PROJECT_SOURCE_DIR}/include
            ${PROJECT_SOURCE_DIR}/src
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMENT "Running cppcheck static analysis"
        USES_TERMINAL
    )
endif()

# clang-tidy only (requires compile_commands.json)
find_program(CLANG_TIDY_BIN clang-tidy)
if(CLANG_TIDY_BIN)
    add_custom_target(clang-tidy
        COMMAND find ${PROJECT_SOURCE_DIR}/src -name "*.c" -exec
            ${CLANG_TIDY_BIN} -p ${CMAKE_BINARY_DIR}
            --warnings-as-errors=* {} +
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMENT "Running clang-tidy analysis"
        USES_TERMINAL
    )
endif()

# Valgrind on all test binaries
find_program(VALGRIND_BIN valgrind)
if(VALGRIND_BIN AND ENABLE_TESTS)
    add_custom_target(valgrind-tests
        COMMAND ${PROJECT_SOURCE_DIR}/scripts/qa-check.sh --gate valgrind
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMENT "Running unit tests under valgrind"
        USES_TERMINAL
    )
endif()

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
install(DIRECTORY include/ DESTINATION include/pnfs-mds)

# Man pages.  mds-admin.8 documents the CLI; mds.conf.5 documents the
# INI file parsed by mds_config_load().  Installed to the standard
# ${mandir}/man8 and ${mandir}/man5 paths so `man mds-admin` works
# after `make install` (followed by mandb on Debian/Ubuntu).
install(FILES docs/man/mds-admin.8
        DESTINATION ${CMAKE_INSTALL_MANDIR}/man8)
install(FILES docs/man/mds.conf.5
        DESTINATION ${CMAKE_INSTALL_MANDIR}/man5)
