#!/usr/bin/env bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# Create a source tarball for offline MDS build hosts.
# Excludes CMake build trees so the target never inherits a stale
# CMakeCache.txt from another machine or checkout path.
#
# Usage (from anywhere):
#   ./scripts/package-source.sh [output.tar.gz]
#
# Default output: lattice.tar.gz in the parent of the repo directory.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="$(basename "${ROOT}")"
PARENT="$(dirname "${ROOT}")"
OUT="${1:-${PARENT}/lattice.tar.gz}"

if [[ "${OUT}" != /* ]]; then
  OUT="${PWD}/${OUT}"
fi

cd "${PARENT}"

echo "packaging ${NAME} -> ${OUT}" >&2
tar -czvf "${OUT}" \
  --exclude="${NAME}/build" \
  --exclude="${NAME}/build-*" \
  --exclude="${NAME}/.git" \
  --exclude="${NAME}/.cache" \
  --exclude="${NAME}/**/__pycache__" \
  "${NAME}"

echo "done: ${OUT}" >&2
