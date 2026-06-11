# Empirical NFSv4.1 / pNFS tests

Re-runnable wire-level conformance tests for the pNFS MDS.  These
complement the in-tree unit and integration tests by exercising the
real client-server wire flow against a live MDS.

## Phases

| Phase | What it does | Pass criteria |
|---|---|---|
| **A** -- Linux default | Mount the MDS with the default Linux NFSv4.1 client, run a six-size write workload, capture the wire | At least 1 LAYOUTGET reply, `FF_FLAGS_STRIPE_LEASE` count on the wire = 0 |
| **B** -- Flex Files | Run the pynfs flex-files category (`flex` flag — FFLA1, FFLG2, ...) against the MDS root export | pynfs `FAIL` count = 0 |
| **C** -- strict N-to-1 | Two parallel dd writers on the same client into disjoint byte ranges of one file, capture the wire | At least 1 LAYOUTGET reply, `CB_LAYOUTRECALL` count = 0 |
| **D** -- full pynfs | Broad pynfs 4.1 regression (`all deleg xattr writedelegations backchannel_ctl`) | Unexpected `FAIL` count = 0; config-gated delegation failures and documented pre-existing lattice divergences are reported but do not count |

### Phase D failure categories

Phase D classifies each pynfs failure into one of three buckets:

1. **`fail_expected`** -- substring-matches `PYNFS_KNOWN_FAILURES`.  Default targets the delegation suite (`st_delegation,st_deleg,delegation`); these fail when `file_delegations_enabled=false`.  Tolerated up to `PYNFS_KNOWN_FAILURES_BUDGET` (default 12).
2. **`fail_diverg`** -- substring-matches `PYNFS_LATTICE_DIVERGENCES`.  These are pre-existing lattice bugs unrelated to the convergence work that **pmds passes**.  Default contents:
   * `testStaleRename` (RNM21) -- `CLOSE` after `RENAME` deletes the target returns `NFS4ERR_STALE`; the RFC and pmds return `NFS4_OK`.
   * `testUndefined` (COMP5) -- `OP_SEQUENCE` in an undefined-op slot returns `NFS4ERR_BADXDR`; the RFC and pmds return `NFS4ERR_OP_ILLEGAL`.
   The harness reports the divergence list every run so it is not forgotten, but does not count it against the unexpected-FAIL assertion.  Fix these in a separate change to remove the entries here.
3. **`fail_unexpected`** -- every other failure.  Assertion fails the run when this count is > 0.

## How to run

Against the default MDS (`192.168.100.11`):

```sh
sudo -v        # cache sudo creds (mount needs root)
bash tests/empirical/run_all.sh
```

Against another MDS:

```sh
MDS_HOST=10.0.0.42 bash tests/empirical/run_all.sh
```

Override individual settings:

```sh
MOUNT=/mnt/custom \
TEST_IFACE=ens6 \
RESULT_DIR=/tmp/my_run \
bash tests/empirical/run_all.sh
```

Run a single phase:

```sh
RESULT_DIR=/tmp/just_a bash tests/empirical/phase_a_linux_default.sh
```

## Requirements

On the client host (typically a Linux test box):

* `mount.nfs` (nfs-utils)
* `tcpdump`, `tshark` (Wireshark CLI)
* `dd`, standard coreutils
* `python3` (for Phase B and the report aggregator)
* A clone of pynfs at `/home/peak/pynfs` (override with `PYNFS_ROOT=`)
* Passwordless `sudo` for `mount`, `umount`, `tcpdump`

## Result tree

Each run writes to `$RESULT_DIR` (default
`/tmp/lattice_empirical_<utc-timestamp>`):

```
phase_a_linux_default.pcap          -- raw capture
phase_a_linux_default_decode.txt    -- tshark -V decode of LAYOUTGET PDUs
phase_a_linux_default_summary.json  -- pass/fail counters
phase_b_flex_files.log              -- pynfs stdout/stderr
phase_b_flex_files_pynfs.json       -- pynfs result JSON
phase_b_flex_files_summary.json     -- pass/fail counters
phase_c_strict_n_to_1.pcap          -- raw capture
phase_c_strict_n_to_1_decode.txt    -- tshark -V decode
phase_c_strict_n_to_1_summary.json  -- pass/fail counters
overall_summary.json                -- aggregate
PASS_REPORT.md                      -- human-readable verdict
run_all.log                         -- combined stdout
```

## Exit codes

* `0` -- every phase reported `fail == 0` and emitted a summary.
* `1` -- at least one phase reported `fail >= 1`, did not emit a summary,
  or could not start (missing tool, mount failure, etc.).

## Adding a new phase

1. Create `tests/empirical/phase_<x>_<name>.sh`.
2. Source `lib_common.sh` at the top.
3. Use `pass` / `fail` / `assert_eq` / `assert_le` for accounting.
4. Call `write_summary_json` before exit.
5. Add a `run_phase` line to `run_all.sh`.
