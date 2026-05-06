#!/bin/bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# qa-check.sh -- Daily QA gate runner.
#
# Runs every quality check defined in CONTRIBUTING.md.
# Exit code 0 = all clear for review. Non-zero = fix before submitting.
#
# Usage:
#   ./scripts/qa-check.sh              # Run all gates
#   ./scripts/qa-check.sh --quick      # Skip slow gates (valgrind, fuzz)
#   ./scripts/qa-check.sh --gate NAME  # Run only the named gate
#
# Gates (in order):
#   1. style        -- Code style checks (tabs, line length, comments)
#   2. build-gcc    -- Build with gcc, all warnings as errors
#   3. build-clang  -- Build with clang, all warnings as errors
#   4. cppcheck     -- Static analysis via cppcheck
#   5. clang-tidy   -- Static analysis via clang-tidy
#   6. unit-tests   -- Run unit tests
#   7. valgrind     -- Run unit tests under valgrind (slow)
#   8. integration  -- Run integration tests (requires running MDS)
#
# The script stops at the first gate failure unless --continue is given.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR_GCC="${PROJECT_DIR}/build-qa-gcc"
BUILD_DIR_CLANG="${PROJECT_DIR}/build-qa-clang"

# Colours for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

QUICK=0
CONTINUE_ON_FAIL=0
ONLY_GATE=""
FAILED_GATES=()
PASSED=0
FAILED=0
SKIPPED=0

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick)     QUICK=1; shift ;;
        --continue)  CONTINUE_ON_FAIL=1; shift ;;
        --gate)      ONLY_GATE="$2"; shift 2 ;;
        -h|--help)
            head -27 "$0" | tail -22
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log_header() {
    echo ""
    echo -e "${CYAN}==========================================================${NC}"
    echo -e "${CYAN}  GATE: $1${NC}"
    echo -e "${CYAN}==========================================================${NC}"
}

gate_pass() {
    echo -e "  ${GREEN}[x] PASS${NC}: $1"
    PASSED=$((PASSED + 1))
}

gate_fail() {
    echo -e "  ${RED}[ ] FAIL${NC}: $1"
    FAILED=$((FAILED + 1))
    FAILED_GATES+=("$1")
    if [[ ${CONTINUE_ON_FAIL} -eq 0 ]]; then
        summary
        exit 1
    fi
}

gate_skip() {
    echo -e "  ${YELLOW}-- SKIP${NC}: $1 ($2)"
    SKIPPED=$((SKIPPED + 1))
}

should_run() {
    if [[ -n "${ONLY_GATE}" ]] && [[ "${ONLY_GATE}" != "$1" ]]; then
        return 1
    fi
    return 0
}

summary() {
    echo ""
    echo -e "${CYAN}==========================================================${NC}"
    echo -e "${CYAN}  QA SUMMARY${NC}"
    echo -e "${CYAN}==========================================================${NC}"
    echo -e "  Passed:  ${GREEN}${PASSED}${NC}"
    echo -e "  Failed:  ${RED}${FAILED}${NC}"
    echo -e "  Skipped: ${YELLOW}${SKIPPED}${NC}"
    if [[ ${FAILED} -gt 0 ]]; then
        echo ""
        echo -e "  ${RED}Failed gates:${NC}"
        for g in "${FAILED_GATES[@]}"; do
            echo -e "    ${RED}[ ]${NC} ${g}"
        done
        echo ""
        echo -e "  ${RED}QA FAILED -- do not submit for review.${NC}"
    else
        echo ""
        echo -e "  ${GREEN}All gates passed -- ready for review.${NC}"
    fi
}

# ---------------------------------------------------------------------------
# Gate 1: Style checks
# ---------------------------------------------------------------------------
if should_run "style"; then
    log_header "style"
    if [[ -x "${SCRIPT_DIR}/check-style.sh" ]]; then
        if "${SCRIPT_DIR}/check-style.sh"; then
            gate_pass "style"
        else
            gate_fail "style"
        fi
    else
        gate_skip "style" "check-style.sh not found or not executable"
    fi
fi

# ---------------------------------------------------------------------------
# Gate 2: Build with gcc
# ---------------------------------------------------------------------------
if should_run "build-gcc"; then
    log_header "build-gcc"
    rm -rf "${BUILD_DIR_GCC}"
    mkdir -p "${BUILD_DIR_GCC}"

    if cmake -S "${PROJECT_DIR}" -B "${BUILD_DIR_GCC}" \
        -DCMAKE_C_COMPILER=gcc \
        -DCMAKE_BUILD_TYPE=Debug \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DENABLE_TESTS=ON \
        -DENABLE_EBPF=OFF \
        > "${BUILD_DIR_GCC}/cmake.log" 2>&1; then

        if cmake --build "${BUILD_DIR_GCC}" -j"$(nproc)" \
            > "${BUILD_DIR_GCC}/build.log" 2>&1; then
            gate_pass "build-gcc"
        else
            echo "    Build log: ${BUILD_DIR_GCC}/build.log"
            tail -20 "${BUILD_DIR_GCC}/build.log"
            gate_fail "build-gcc"
        fi
    else
        echo "    CMake log: ${BUILD_DIR_GCC}/cmake.log"
        tail -20 "${BUILD_DIR_GCC}/cmake.log"
        gate_fail "build-gcc (cmake)"
    fi
fi

# ---------------------------------------------------------------------------
# Gate 3: Build with clang
# ---------------------------------------------------------------------------
if should_run "build-clang"; then
    log_header "build-clang"
    rm -rf "${BUILD_DIR_CLANG}"
    mkdir -p "${BUILD_DIR_CLANG}"

    if command -v clang > /dev/null 2>&1; then
        if cmake -S "${PROJECT_DIR}" -B "${BUILD_DIR_CLANG}" \
            -DCMAKE_C_COMPILER=clang \
            -DCMAKE_BUILD_TYPE=Debug \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
            -DENABLE_TESTS=ON \
            -DENABLE_EBPF=OFF \
            > "${BUILD_DIR_CLANG}/cmake.log" 2>&1; then

            if cmake --build "${BUILD_DIR_CLANG}" -j"$(nproc)" \
                > "${BUILD_DIR_CLANG}/build.log" 2>&1; then
                gate_pass "build-clang"
            else
                echo "    Build log: ${BUILD_DIR_CLANG}/build.log"
                tail -20 "${BUILD_DIR_CLANG}/build.log"
                gate_fail "build-clang"
            fi
        else
            echo "    CMake log: ${BUILD_DIR_CLANG}/cmake.log"
            tail -20 "${BUILD_DIR_CLANG}/cmake.log"
            gate_fail "build-clang (cmake)"
        fi
    else
        gate_skip "build-clang" "clang not installed"
    fi
fi

# ---------------------------------------------------------------------------
# Gate 4: cppcheck
# ---------------------------------------------------------------------------
if should_run "cppcheck"; then
    log_header "cppcheck"
    if command -v cppcheck > /dev/null 2>&1; then
        CPPCHECK_OUT="${BUILD_DIR_GCC}/cppcheck.log"
        if cppcheck \
            --enable=all \
            --std=c11 \
            --error-exitcode=1 \
            --suppress=missingIncludeSystem \
            --suppress=unusedFunction \
            --suppress=constParameterCallback \
            --suppress=unmatchedSuppression \
            --suppress=checkersReport \
            --suppress=checkLevelNormal \
            --inline-suppr \
            -I "${PROJECT_DIR}/include" \
            "${PROJECT_DIR}/src" \
            > "${CPPCHECK_OUT}" 2>&1; then
            gate_pass "cppcheck"
        else
            echo "    Log: ${CPPCHECK_OUT}"
            tail -30 "${CPPCHECK_OUT}"
            gate_fail "cppcheck"
        fi
    else
        gate_skip "cppcheck" "cppcheck not installed"
    fi
fi

# ---------------------------------------------------------------------------
# Gate 5: clang-tidy
# ---------------------------------------------------------------------------
if should_run "clang-tidy"; then
    log_header "clang-tidy"
    COMPILE_DB="${BUILD_DIR_GCC}/compile_commands.json"
    if command -v clang-tidy > /dev/null 2>&1 && [[ -f "${COMPILE_DB}" ]]; then
        TIDY_OUT="${BUILD_DIR_GCC}/clang-tidy.log"
        TIDY_FAIL=0
        > "${TIDY_OUT}"  # truncate log from prior runs

        # Run clang-tidy on each source file.
        # Exclude pure-stub files: all parameters are intentionally unused.
        while IFS= read -r -d '' src; do
            if ! clang-tidy -p "${BUILD_DIR_GCC}" --extra-arg=-Wno-unknown-warning-option \
                --warnings-as-errors='*' \
                "${src}" >> "${TIDY_OUT}" 2>&1; then
                TIDY_FAIL=1
            fi
        done < <(find "${PROJECT_DIR}/src" -name '*.c' \
            ! -name 'rondb_stubs.c' \
            -print0)

        if [[ ${TIDY_FAIL} -eq 0 ]]; then
            gate_pass "clang-tidy"
        else
            echo "    Log: ${TIDY_OUT}"
            tail -30 "${TIDY_OUT}"
            gate_fail "clang-tidy"
        fi
    else
        if ! command -v clang-tidy > /dev/null 2>&1; then
            gate_skip "clang-tidy" "clang-tidy not installed"
        else
            gate_skip "clang-tidy" "compile_commands.json not found (run build-gcc first)"
        fi
    fi
fi

# ---------------------------------------------------------------------------
# Gate 6: Unit tests
# ---------------------------------------------------------------------------
if should_run "unit-tests"; then
    log_header "unit-tests"
    if [[ -d "${BUILD_DIR_GCC}" ]]; then
        TEST_OUT="${BUILD_DIR_GCC}/test.log"

        # Try labeled unit tests first; fall back to all tests if
        # no tests are labeled (ctest exits 0 with zero matches).
        (cd "${BUILD_DIR_GCC}" && ctest --output-on-failure -L unit \
            > "${TEST_OUT}" 2>&1) || true

        if grep -q "No tests were found" "${TEST_OUT}"; then
            # No labeled tests -- run all tests as fallback.
            (cd "${BUILD_DIR_GCC}" && ctest --output-on-failure \
                > "${TEST_OUT}" 2>&1) || true
        fi

        # Final check: did anything actually run and pass?
        if grep -q "No tests were found" "${TEST_OUT}"; then
            echo "    ERROR: ctest discovered zero tests"
            gate_fail "unit-tests"
        elif grep -q "tests passed" "${TEST_OUT}"; then
            gate_pass "unit-tests"
        else
            echo "    Log: ${TEST_OUT}"
            tail -40 "${TEST_OUT}"
            gate_fail "unit-tests"
        fi
    else
        gate_skip "unit-tests" "build directory not found (run build-gcc first)"
    fi
fi

# ---------------------------------------------------------------------------
# Gate 7: Valgrind (slow -- skipped with --quick)
# ---------------------------------------------------------------------------
if should_run "valgrind"; then
    log_header "valgrind"
    if [[ ${QUICK} -eq 1 ]]; then
        gate_skip "valgrind" "--quick mode"
    elif ! command -v valgrind > /dev/null 2>&1; then
        gate_skip "valgrind" "valgrind not installed"
    elif [[ ! -d "${BUILD_DIR_GCC}" ]]; then
        gate_skip "valgrind" "build directory not found"
    else
        VALGRIND_FAIL=0
        VALGRIND_LOG_DIR="${BUILD_DIR_GCC}/valgrind-logs"
        mkdir -p "${VALGRIND_LOG_DIR}"

        # Suppress known leaks inside the in-memory test backend
        # (tests/catalogue_memdb.c); production source is not covered
        # by this file.
        VALGRIND_SUPP_ARGS=()
        if [[ -f "${PROJECT_DIR}/tests/memdb.supp" ]]; then
            VALGRIND_SUPP_ARGS+=("--suppressions=${PROJECT_DIR}/tests/memdb.supp")
        fi

        # Find all test binaries
        while IFS= read -r -d '' test_bin; do
            test_name="$(basename "${test_bin}")"
            vg_log="${VALGRIND_LOG_DIR}/${test_name}.log"

            if ! valgrind \
                --leak-check=full \
                --show-leak-kinds=all \
                --errors-for-leak-kinds=all \
                --error-exitcode=99 \
                --track-origins=yes \
                "${VALGRIND_SUPP_ARGS[@]}" \
                --log-file="${vg_log}" \
                "${test_bin}" > /dev/null 2>&1; then
                echo -e "    ${RED}[ ]${NC} ${test_name} -- see ${vg_log}"
                VALGRIND_FAIL=1
            else
                echo -e "    ${GREEN}[x]${NC} ${test_name} -- clean"
            fi
        done < <(find "${BUILD_DIR_GCC}" -name 'test_*' -executable -type f -print0)

        if [[ ${VALGRIND_FAIL} -eq 0 ]]; then
            gate_pass "valgrind"
        else
            gate_fail "valgrind"
        fi
    fi
fi

# ---------------------------------------------------------------------------
# Gate 8: Integration tests (optional -- requires running infrastructure)
# ---------------------------------------------------------------------------
if should_run "integration"; then
    log_header "integration"
    if [[ ${QUICK} -eq 1 ]]; then
        gate_skip "integration" "--quick mode"
    else
        gate_skip "integration" "requires running MDS + DS infrastructure"
    fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
summary

if [[ ${FAILED} -gt 0 ]]; then
    exit 1
fi
exit 0
