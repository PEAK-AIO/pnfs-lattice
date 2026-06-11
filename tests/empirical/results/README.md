# Lattice empirical pNFSv4.2 harness — per-patch results

This directory holds the captured runs of `tests/empirical/run_all.sh` and
`tests/empirical/phase_d_pynfs.sh` used to validate each lease/grant
convergence patch on the `dev` branch. NFS protocol scope is **NFSv4.2
flex-files only**. Mount is taken against the storage NIC (`10.0.0.11`),
not the management NIC (`192.168.100.11`).

## Invocation

```
RESULT_DIR=tests/empirical/results/<tag> \
MDS_HOST=10.0.0.11 \
MOUNT=/mnt/pnfs_empirical \
bash tests/empirical/run_all.sh
```

## Headline numbers

| run | source | delegation | phase A | phase B | phase C | phase D pynfs |
|---|---|---|---|---|---|---|
| `baseline_lattice_main` | `lattice-main@071019a` | on | n/a | n/a | n/a | 179 pass / 2 fail / 0 unexp |
| `0003` | `dev@90f2b05` (auto-widen-lease-on-4k) | off | 8/8 pass | 2/2 pass | 4/4 pass | 171 pass / 8 expected (deleg off) / 2 lattice-divergence / 0 unexp |
| `0004` | `dev@c578d06` (configurable-grant-cap) | off | 8/8 pass | 2/2 pass | 4/4 pass | 171 pass / 8 expected (deleg off) / 2 lattice-divergence / 0 unexp |
| `0004_deleg` | `dev@c578d06` | on | n/a | n/a | n/a | 179 pass / 2 lattice-divergence / 0 unexp |
| `0005` | `dev@251d558` (per-DS-stripe lease keying) | on | 8/8 pass | 2/2 pass | 4/4 pass | 179 pass / 2 lattice-divergence / 0 unexp |
| `0006` | `dev@c0ff63c` (HPC-gated activation) | on | 8/8 pass | 2/2 pass | 4/4 pass | 179 pass / 2 lattice-divergence / 0 unexp |
| `0007` | `dev@HEAD` (contention-aware grant narrowing) | on | 8/8 pass | 2/2 pass | 4/4 pass | 179 pass / 2 lattice-divergence / 0 unexp |

The `_deleg` re-run of 0004 establishes byte-for-byte pynfs parity with
the `baseline_lattice_main` run: 179 pass, 2 fail, 0 errors, 0
unexpected fails.

## Pre-existing lattice divergences (NOT introduced by 0001–0004)

Both fail on `lattice-main@071019a` and on every patched build:

- `st_rename.testStaleRename` — `CLOSE after RENAME deletes target` returns
  `NFS4ERR_STALE` instead of `NFS4_OK`.
- `st_compound.testUndefined` — an unknown opcode returns `NFS4ERR_BADXDR`
  instead of `NFS4ERR_OP_ILLEGAL`.

These are tracked as future cleanup work; they are outside the scope of
the lease/grant convergence series.

## Delegation gating

`file_delegations_enabled` is off by default in the lab `mds.conf` as a
Phase-4 perf-investigation control. With it `false`, eight `st_delegation`
pynfs tests fail by construction (the server intentionally does not grant
delegations). Setting `file_delegations_enabled = true` and restarting the
daemon lifts those eight failures with no other changes — see the
`0004_deleg` and `baseline_lattice_main` runs.

## Harness defaults

`tests/empirical/lib_common.sh` defaults to `vers=4.2` to match the
production protocol scope. Prior `vers=4.1` defaults are not supported
by this codebase.

(c) PEAK:AIO Mark Klarzynski
