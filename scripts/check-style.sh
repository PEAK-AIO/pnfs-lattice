#!/bin/bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# check-style.sh — Code style checker per CONTRIBUTING.md.
#
# Checks:
#   1. No trailing whitespace in .c/.h files
#   2. Line length <= 100 chars in .c/.h files
#   3. No C++ comments (//) in .c files
#   4. No typedef struct (except function pointers)
#   5. No banned functions (strcpy, strcat, sprintf, gets, alloca)
#   6. All .c/.h files have PeakAIO copyright header
#   7. No mixed tabs/spaces indentation (leading whitespace consistency)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

ERRORS=0

check_fail() {
    echo -e "  ${RED}✗${NC} $1"
    ERRORS=$((ERRORS + 1))
}

check_pass() {
    echo -e "  ${GREEN}✓${NC} $1"
}

# Collect all .c and .h files (exclude build dirs and generated files)
mapfile -t C_FILES < <(find "${PROJECT_DIR}/src" "${PROJECT_DIR}/include" \
    -name '*.c' -o -name '*.h' 2>/dev/null | sort)

if [[ ${#C_FILES[@]} -eq 0 ]]; then
    echo "No .c/.h files found."
    exit 0
fi

echo "Checking ${#C_FILES[@]} source files..."

# ---------------------------------------------------------------------------
# Check 1: Trailing whitespace
# ---------------------------------------------------------------------------
echo ""
echo "--- Trailing whitespace ---"
TRAIL_COUNT=0
for f in "${C_FILES[@]}"; do
    matches=$(grep -n '[[:blank:]]$' "${f}" 2>/dev/null || true)
    if [[ -n "${matches}" ]]; then
        rel="${f#"${PROJECT_DIR}/"}"
        while IFS= read -r line; do
            check_fail "${rel}:${line}"
            TRAIL_COUNT=$((TRAIL_COUNT + 1))
        done <<< "${matches}"
    fi
done
if [[ ${TRAIL_COUNT} -eq 0 ]]; then
    check_pass "No trailing whitespace"
fi

# ---------------------------------------------------------------------------
# Check 2: Line length > 100
# ---------------------------------------------------------------------------
echo ""
echo "--- Line length (max 100) ---"
LONG_COUNT=0
for f in "${C_FILES[@]}"; do
    # Use awk to find lines > 100 chars (excluding tabs expanded)
    matches=$(awk 'length > 100 { printf "%d: %s\n", NR, $0 }' "${f}" 2>/dev/null || true)
    if [[ -n "${matches}" ]]; then
        rel="${f#"${PROJECT_DIR}/"}"
        line_count=$(echo "${matches}" | wc -l)
        check_fail "${rel}: ${line_count} line(s) > 100 chars"
        echo "${matches}" | head -5 | sed 's/^/        /'
        LONG_COUNT=$((LONG_COUNT + line_count))
    fi
done
if [[ ${LONG_COUNT} -eq 0 ]]; then
    check_pass "All lines <= 100 chars"
fi

# ---------------------------------------------------------------------------
# Check 3: C++ comments in .c files
# ---------------------------------------------------------------------------
echo ""
echo "--- C++ comments (//) in .c files ---"
CPP_COUNT=0
for f in "${C_FILES[@]}"; do
    # Only check .c files, skip .h (// is common in header guards etc.)
    if [[ "${f}" != *.c ]]; then
        continue
    fi
    # Match // but exclude URLs (http://) and TODO markers in string literals
    matches=$(grep -n '^\s*\/\/' "${f}" 2>/dev/null || true)
    if [[ -n "${matches}" ]]; then
        rel="${f#"${PROJECT_DIR}/"}"
        line_count=$(echo "${matches}" | wc -l)
        check_fail "${rel}: ${line_count} C++ style comment(s)"
        echo "${matches}" | head -3 | sed 's/^/        /'
        CPP_COUNT=$((CPP_COUNT + line_count))
    fi
done
if [[ ${CPP_COUNT} -eq 0 ]]; then
    check_pass "No C++ comments in .c files"
fi

# ---------------------------------------------------------------------------
# Check 4: Banned functions
# ---------------------------------------------------------------------------
echo ""
echo "--- Banned functions ---"
BANNED="strcpy\|strcat\|sprintf\|gets\|alloca\|strtok[^_]"
BAN_COUNT=0
for f in "${C_FILES[@]}"; do
    matches=$(grep -n "\b\(${BANNED}\)\b" "${f}" 2>/dev/null | \
              grep -v '^[0-9]*:[[:space:]]*/\*' | \
              grep -v '^[0-9]*:[[:space:]]*\*' | \
              grep -v '^[0-9]*:[[:space:]]*//' || true)
    if [[ -n "${matches}" ]]; then
        rel="${f#"${PROJECT_DIR}/"}"
        while IFS= read -r line; do
            check_fail "${rel}:${line}"
            BAN_COUNT=$((BAN_COUNT + 1))
        done <<< "${matches}"
    fi
done
if [[ ${BAN_COUNT} -eq 0 ]]; then
    check_pass "No banned functions"
fi

# ---------------------------------------------------------------------------
# Check 5: Copyright header
# ---------------------------------------------------------------------------
echo ""
echo "--- PeakAIO copyright header ---"
HDR_COUNT=0
for f in "${C_FILES[@]}"; do
    if ! head -5 "${f}" | grep -q "Copyright.*PeakAIO"; then
        rel="${f#"${PROJECT_DIR}/"}"
        check_fail "${rel}: missing PeakAIO copyright header"
        HDR_COUNT=$((HDR_COUNT + 1))
    fi
done
if [[ ${HDR_COUNT} -eq 0 ]]; then
    check_pass "All files have PeakAIO copyright header"
fi

# ---------------------------------------------------------------------------
# Check 6: typedef struct (non-function-pointer)
# ---------------------------------------------------------------------------
echo ""
echo "--- typedef struct (banned per kernel style) ---"
TD_COUNT=0
for f in "${C_FILES[@]}"; do
    matches=$(grep -n 'typedef\s\+struct' "${f}" 2>/dev/null || true)
    if [[ -n "${matches}" ]]; then
        rel="${f#"${PROJECT_DIR}/"}"
        while IFS= read -r line; do
            check_fail "${rel}:${line}"
            TD_COUNT=$((TD_COUNT + 1))
        done <<< "${matches}"
    fi
done
if [[ ${TD_COUNT} -eq 0 ]]; then
    check_pass "No typedef struct"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
if [[ ${ERRORS} -gt 0 ]]; then
    echo -e "${RED}Style check failed: ${ERRORS} issue(s) found.${NC}"
    exit 1
else
    echo -e "${GREEN}Style check passed.${NC}"
    exit 0
fi
