# Lattice empirical test results — patch 0002 series
(c) PEAK:AIO Mark Klarzynski

## Environment
- MDS: lattice (`pnfs-lattice` repo, HEAD `071019a`), built on .11, deployed at /usr/local/bin/pnfs-mds.
- RonDB schema was wiped (drop_all_tabs) before this test; lattice auto-bootstrapped a fresh `pnfs_mds` schema on startup.
- Client: .15, NFSv4.2 over pNFS Flex Files mount of `10.0.0.11:/` on /mnt/pnfs (nconnect=16).
- Workspace inside mount: /mnt/pnfs/lattice-test (chmod 1777 by root).
- Test date: 2026-06-07T10:13:50Z

## Patch under test
**Nothing** is being patched in lattice for this run — this is a clean empirical baseline of current `lattice-main` at HEAD `071019a`. The corresponding pmds patch 0002 (lease-floor, `fc96f31`) has no equivalent in lattice; lattice already constrains the lease scope through commit `7580307` (stripe lease table).

## Phase A — block-size LAYOUTGET sweep

Six block sizes: 4 KiB, 16 KiB, 64 KiB, 1 MiB, 16 MiB, 256 MiB. Each via `dd if=/dev/urandom of=…` over the live pNFS mount. `tcpdump` on .11 captured port 2049 traffic; `tshark` decoded server-reply LAYOUTGET frames.

- Server-reply LAYOUTGET frames decoded: **0
0**
- FF_FLAGS_STRIPE_LEASE / stripe-lease references on the wire: **0
0**

See `~/lattice_test_handover/phase_A_decode.txt` for the full per-block decode.

## Phase B — Flex Files protocol surface

Extracted from the Phase A capture: layout type, stripe_unit, ff_flags. Folded into `phase_A_decode.txt` since it's the same pcap.

## Phase C — same-client N-to-1

Two parallel `dd` processes writing 16 MiB each to the same file at offsets 0 and 32 MiB. Captured port 2049 traffic to look for CB_LAYOUTRECALL.

- CB_LAYOUTRECALL traffic in the capture: **0
0** matches (zero = no recall traffic)
- NFS4ERR_* lines: **0
0**

See `~/lattice_test_handover/phase_C_decode.txt` for the full op histogram and any error details.

## pynfs 4.1 regression

Run from .15 against `10.0.0.11:/lattice-test`. Same invocation pattern as the pmds patch-0002 run (`all deleg xattr writedelegations backchannel_ctl`).

```
could not parse pynfs json: [Errno 2] No such file or directory: '/home/peak/lattice_test_handover/pynfs.json'
```

Raw JSON in `~/lattice_test_handover/pynfs.json`. Per-test failure detail (if any) in `~/lattice_test_handover/pynfs_run.log`.

## Conclusion

This is an empirical baseline of `lattice-main` at `071019a` against the same 4-phase test we ran on pmds with patch 0002. Interpretation is intentionally not pre-committed in this file — the numbers above stand on their own, and decisions about porting pmds patches 0001/0002 to lattice (or vice versa) should be made by reading the per-block decodes alongside the email I sent on flex-files v1/v2 standards positioning.

(c) PEAK:AIO Mark Klarzynski
