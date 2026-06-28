# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# Shared build environment for pnfs-lattice scripts.
# Source from bash build wrappers; do not execute directly.
#
# GCC and CMake write compiler scratch files to $TMPDIR. On many
# cluster build nodes /tmp is tiny and fills up ("No space left on
# device" while closing cc*.s). Default to a directory under the
# out-of-tree build dir on the user's filesystem instead.
#
# Usage:
#   BUILD_DIR=build source scripts/build-env.sh
#
# Set PNFS_KEEP_TMPDIR=1 to leave TMPDIR untouched.

if [[ -n "${_PNFS_BUILD_ENV_SOURCED:-}" ]]; then
	return 0 2>/dev/null || exit 0
fi
_PNFS_BUILD_ENV_SOURCED=1

_build_env_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${ROOT:-${_build_env_root}}"
BUILD_DIR="${BUILD_DIR:-build}"
BUILD_TMP="${ROOT}/${BUILD_DIR}/tmp"

pnfs_tmpdir_is_system_tmp() {
	[[ -z "${1:-}" || "${1}" == /tmp || "${1}" == /tmp/* ]]
}

if [[ -z "${PNFS_KEEP_TMPDIR:-}" ]] && pnfs_tmpdir_is_system_tmp "${TMPDIR:-}"; then
	mkdir -p "${BUILD_TMP}"
	export TMPDIR="${BUILD_TMP}"
	echo "build-env: TMPDIR=${TMPDIR} (redirected from system /tmp)" >&2
fi
export TEMP="${TMPDIR}"
export TMP="${TMPDIR}"
