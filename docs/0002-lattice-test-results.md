# Lattice empirical test results - patch 0002 series
(c) PEAK:AIO Mark Klarzynski

## Environment
- MDS: lattice ('pnfs-lattice' repo, HEAD '071019a'), built and deployed on .11.
- RonDB schema dropped (per-table ndb_drop_table -d pnfs_mds); lattice auto-bootstrapped fresh on start.
- Client: .15, NFSv4.2 pNFS mount of '10.0.0.11:/' at /mnt/pnfs, nconnect=16.
- Workspace inside mount: /mnt/pnfs/lattice-test (chmod 1777).
- Test date: 2026-06-07T10:27:09Z
- Patch under test: NONE - this is a baseline of current lattice-main. The pmds patch 0002 (lease-floor) has no direct analogue in lattice; lattice constrains lease scope via commit 7580307 (stripe lease table).

## Phase A: block-size LAYOUTGET sweep
Six sizes: 4K, 16K, 64K, 1M, 16M, 256M. tcpdump on .11, tshark decode.
- Server-reply LAYOUTGET frames decoded: 286
- FF_FLAGS_STRIPE_LEASE / stripe-lease wire references: 0
Full decode: ~/lattice_test_handover/phase_A_decode.txt

## Phase B: Flex Files protocol surface
Extracted from Phase A pcap (layout type, stripe_unit, ff_flags).

## Phase C: same-client N-to-1
Two parallel dd writers to same file, disjoint offsets 0 and 32MiB.
- CB_LAYOUTRECALL refs: 2
- NFS4ERR_ lines: 0
Full decode: ~/lattice_test_handover/phase_C_decode.txt

## pynfs 4.1 regression
Target: 10.0.0.11:/lattice-test. Suite spec: 'all deleg xattr writedelegations backchannel_ctl'.
```
could not parse: [Errno 2] No such file or directory: '/home/peak/lattice_test_handover/pynfs.json'
```
Raw JSON: ~/lattice_test_handover/pynfs.json. Run log: pynfs_run.log.

## Conclusion
Empirical baseline only. Interpretation deliberately not pre-committed; numbers above plus per-block decodes inform the decision on porting pmds 0001/0002 to lattice (or vice versa).

(c) PEAK:AIO Mark Klarzynski
