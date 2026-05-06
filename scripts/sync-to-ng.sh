#!/bin/bash
# sync-to-ng.sh -- Mirror main to PEAK-AIO/pnfs-ng with Warp artefacts stripped.
#
# pmds (this repo) tracks some files that must never land in the
# public pnfs-ng mirror:
#
#   .warp/                 -- Warp agent working directory
#   AGENTS.md              -- project rules for the Warp agent
#   scripts/sync-to-ng.sh  -- this script (pnfs-ng has no use for it)
#   scripts/git-hooks/     -- the ng-specific pre-push guardrail
#
# The script:
#   1. Clones the current main branch into a scratch directory.
#   2. Rewrites history with git-filter-repo, removing every path in
#      the strip list above from every commit.
#   3. Force-pushes the rewritten main to the pnfs-ng remote.
#
# The scratch directory is deleted on exit via trap.  Nothing is
# written back to the working clone.
#
# Usage:
#   scripts/sync-to-ng.sh            # sync current HEAD of main
#   NG_URL=... scripts/sync-to-ng.sh # override the target remote
#
# Requires: git-filter-repo (pipx install git-filter-repo or apt
# install git-filter-repo), git, mktemp.

set -euo pipefail

NG_URL="${NG_URL:-https://github.com/PEAK-AIO/pnfs-ng.git}"
REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

# Pre-flight checks -- fail loudly before touching anything.
if ! command -v git-filter-repo >/dev/null 2>&1; then
    printf 'error: git-filter-repo not found in PATH.\n' >&2
    printf 'install: pipx install git-filter-repo\n' >&2
    exit 1
fi

# In a plain checkout .git is a directory; in a worktree it's a
# file pointing at the real gitdir.  Accept either shape -- both
# satisfy `git rev-parse --show-toplevel` above.
if [ -z "${REPO_ROOT}" ] || [ ! -e "${REPO_ROOT}/.git" ]; then
    printf 'error: could not locate the pmds repo root.\n' >&2
    exit 1
fi

# Refuse to run if main has uncommitted changes -- we want to mirror
# committed state only.
if ! git -C "${REPO_ROOT}" diff --quiet HEAD -- 2>/dev/null; then
    printf 'error: working tree has uncommitted changes; commit or stash first.\n' >&2
    exit 1
fi

SCRATCH="$(mktemp -d -t pnfs-ng-sync-XXXXXX)"
trap 'rm -rf -- "${SCRATCH}"' EXIT

# Clone a fresh copy of main so filter-repo's rewrite never touches
# the working repo.  --single-branch keeps the clone minimal; we
# intentionally pull local objects via file:// so the clone is quick
# and deterministic regardless of remote fetch state.
git clone --quiet --no-local --single-branch --branch main \
    "${REPO_ROOT}" "${SCRATCH}/work"

cd "${SCRATCH}/work"

# Strip Warp-only artefacts from every commit.  --invert-paths means
# "keep everything except the listed paths".  filter-repo treats a
# bare directory argument as a path prefix, so .warp strips the
# whole subtree.
git filter-repo --force \
    --invert-paths \
    --path .warp \
    --path AGENTS.md \
    --path scripts/sync-to-ng.sh \
    --path scripts/git-hooks

# Attach the ng remote and force-push the rewritten history.  ng is
# a derived mirror and MUST be force-pushed; force is scoped to this
# scratch clone only, so the working repo's remotes are unaffected.
git remote add ng "${NG_URL}"
git push --force ng main:main
git push --force ng --tags

printf 'sync-to-ng: main mirrored to %s\n' "${NG_URL}"
