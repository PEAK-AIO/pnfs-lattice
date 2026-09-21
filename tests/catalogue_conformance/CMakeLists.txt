# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# Backend-parameterised catalogue conformance suite (Phase 6a / 6b).
#
# Every executable links the harness object and pnfs_mds_core, which
# carries the in-memory backend.  The link order (core, cluster, fsal,
# common, core, cluster, core) mirrors the unit-test loop in
# tests/CMakeLists.txt: pnfs_cluster calls back into pnfs_mds_core so
# the core archive is scanned again after it.
#
# Backend selection at run time: CATALOGUE_TEST_BACKEND=memdb|rondb|fdb
# (default memdb); rondb additionally needs RONDB_CONF.  A backend the
# binary or environment cannot provide exits 77, registered below as
# SKIP_RETURN_CODE so ctest reports "Not Run" instead of pass or fail.
#
# Labels:
#   unit         C6 token semantics, C7 lifecycle, slot matrix and
#                hard-link cookie -- seconds each, green on memdb, run
#                with every `ctest -L unit`.
#   conformance  slot matrix and hard-link cookie (both also unit) plus
#                the 6b contract suite, which runs for minutes and is
#                therefore not a unit test.  Rows / sub-tests the
#                selected backend is documented not to meet are
#                classified in the test output.
#
# Sanitizer run (C7 open/close + concurrent handles under ASan/UBSan),
# as a second configure of the same tree:
#
#   ./scripts/cmake-fresh.sh build-asan -DCMAKE_BUILD_TYPE=Debug \
#       -DENABLE_RONDB=OFF \
#       -DCMAKE_C_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
#       -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
#   cmake --build build-asan --target conformance_lifecycle \
#       conformance_token_semantics -j5
#   ASAN_OPTIONS=detect_leaks=1 ./build-asan/tests/catalogue_conformance/conformance_lifecycle

add_library(conformance_harness OBJECT harness.c)
target_include_directories(conformance_harness PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/catalogue
    ${PROJECT_SOURCE_DIR}/tests
    ${CMAKE_CURRENT_SOURCE_DIR}
)
# The fdb teardown sweeps the run prefix with bare fdb_c calls.
if(ENABLE_FDB)
    target_include_directories(conformance_harness SYSTEM PRIVATE ${FDB_INCLUDE_DIR})
endif()

set(CONFORMANCE_LINK_LIBS
    pnfs_mds_core
    pnfs_cluster
    pnfs_fsal_obj
    pnfs_common
    pnfs_mds_core
    pnfs_cluster
    pnfs_mds_core
    rondb_schema
    pthread
)

# conformance_add(<target> <source> <labels>) -- <labels> is a CMake
# list ("unit;conformance" registers under both).
#
# With ENABLE_FDB every binary carries libfdb_c (PUBLIC in
# pnfs_mds_core); the LeakSanitizer / ThreadSanitizer suppressions for
# the uninstrumented client (tests/lsan-fdb.supp, tests/tsan-fdb.supp)
# reach the test through ctest's environment, exactly as
# tests/CMakeLists.txt does for the unit tests, so a sanitizer configure
# runs green without manual exports.
function(conformance_add target source label)
    add_executable(${target} ${source} $<TARGET_OBJECTS:conformance_harness>)
    target_include_directories(${target} PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src/catalogue
        ${PROJECT_SOURCE_DIR}/tests
        ${CMAKE_CURRENT_SOURCE_DIR}
    )
    target_link_libraries(${target} PRIVATE ${CONFORMANCE_LINK_LIBS})
    add_test(NAME ${target} COMMAND ${target})
    set_tests_properties(${target} PROPERTIES
        LABELS "${label}"
        SKIP_RETURN_CODE 77
    )
    if(ENABLE_FDB)
        set_property(TEST ${target} APPEND PROPERTY ENVIRONMENT
            "LSAN_OPTIONS=suppressions=${PROJECT_SOURCE_DIR}/tests/lsan-fdb.supp"
            "TSAN_OPTIONS=suppressions=${PROJECT_SOURCE_DIR}/tests/tsan-fdb.supp")
    endif()
endfunction()

conformance_add(conformance_token_semantics test_token_semantics.c unit)
conformance_add(conformance_lifecycle       test_lifecycle.c       unit)
conformance_add(conformance_slot_matrix     slot_matrix.c          "unit;conformance")
conformance_add(conformance_hardlink_cookie test_hardlink_cookie.c "unit;conformance")
conformance_add(conformance_contract        test_contract.c        conformance)

# The contract binary carries its own 600 s watchdog; give ctest room
# above it so the binary, not ctest, reports the deadlock.
set_tests_properties(conformance_contract PROPERTIES TIMEOUT 900)

# Phase 6c: FoundationDB fault injection and lifecycle.  fdb only (77
# on any other backend or without ENABLE_FDB).  The buggify, restart
# and lifecycle sub-tests re-execute the binary as child processes so
# the client network options they need never touch the harness process
# (see the header of test_fault.c).  The binary carries a 1500 s
# watchdog that also kills a stuck child; ctest sits above it.
conformance_add(conformance_fault test_fault.c conformance)
if(ENABLE_FDB)
    target_include_directories(conformance_fault SYSTEM PRIVATE ${FDB_INCLUDE_DIR})
endif()
set_tests_properties(conformance_fault PROPERTIES TIMEOUT 1800)
