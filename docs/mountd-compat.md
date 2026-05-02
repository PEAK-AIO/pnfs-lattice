# `showmount -e` compatibility responder
## Why
Beta operators reflexively run `showmount -e <host>` as a sanity check that an NFS server is exporting something. The pNFS-MDS speaks NFSv4.1 / pNFS only, so `showmount -e <mds>` returns *RPC: Program not registered*. That looks like a broken server and produces a stream of "is the MDS up?" tickets. We want to satisfy the operator reflex without exposing real DS exports or implementing the NFSv3 MOUNT protocol.
## Defaults
**Enabled by default** since v0.1.0+mountd-compat.  A fresh install
binds UDP+TCP **20048** on `0.0.0.0`, registers `100005/3` with the
local rpcbind, and advertises a single synthetic export `/`.
Upgraders see one new listening port + one new rpcbind entry on first
restart; the wire-protocol guarantees (no NFSv3 mount) hold whether
the shim is on or off.  See the **Upgrade behaviour** section below.
To suppress the listener entirely on a given host, set
`mountd_compat_enabled = false` in `mds.conf`.
## What this is
A tiny ONC-RPC responder, started on demand by the daemon, that:
1. Listens on UDP and TCP on a configured port (default **20048**, the IANA mountd port).
2. Implements ONC-RPC program **100005** (mountd) version **3** and answers exactly three procedures:
   * **NULL** (proc 0) — empty success.
   * **EXPORT** (proc 5) — a synthetic `exportlist` built from `mountd_compat_exports` in `mds.conf`. Each entry is `{ dirpath, groups = empty (= world-readable) }`.
   * **DUMP** (proc 2) — empty mountlist. (We don't track NFSv3 mounts because there are none.)
3. Rejects every other procedure with `MSG_ACCEPTED / PROC_UNAVAIL`. Specifically, **MNT (proc 1) — the only procedure that returns a usable v3 file handle — is always rejected.** A client that runs `mount -t nfs -o vers=3 <mds>:/ /mnt/x` against a daemon with this shim enabled gets a clean mount error, not a partial success.
4. (Optional, default ON) Registers `100005/3 → port` with the local rpcbind via PMAPPROC_SET so `showmount -e` can discover the port via portmap on 111. Deregisters on shutdown.
## What this is NOT
* **Not** a passthrough or proxy of any DS's `showmount -e`. The responder never opens a socket to any DS for any reason.
* **Not** an NFSv3 mount server. The MOUNT procedure is rejected. Real client mounts must use NFSv4.1 / pNFS.
* **Not** a leak of DS topology. No DS hostnames, IPs, exports, or paths are advertised by this shim. The export strings are operator-controlled and have no relationship to any real filesystem path on the MDS or any DS.
## Configuration
See `docs/config-keys.md` and `mds.conf(5)` for the full reference. Defaults:
```ini
mountd_compat_enabled = true
mountd_compat_port = 20048
mountd_compat_bind_addr = 0.0.0.0
mountd_compat_register_rpcbind = true
mountd_compat_exports = /
```
A typical operator override that publishes friendlier paths:
```ini
mountd_compat_exports = /pnfs, /scratch
```
With the defaults, a customer immediately sees:
```text
$ showmount -e mds-01
Export list for mds-01:
/  *
```
After customising `mountd_compat_exports`:
```text
$ showmount -e mds-01
Export list for mds-01:
/pnfs     *
/scratch  *
```
To turn the shim off on a given host:
```ini
mountd_compat_enabled = false
```
## Code map
* `include/mountd_compat.h` — public API: `mountd_compat_start`, `mountd_compat_stop`, plus `mountd_compat_handle_packet` exposed for unit tests.
* `src/mds/mountd_compat.c` — implementation. Single core file, identical in community and enterprise builds. Always compiled; runtime-gated by `mountd_compat_enabled`.
* `tests/unit/test_mountd_compat.c` — packet-level XDR + dispatch tests (no sockets, no threads).
The dispatch logic is deliberately implemented as a pure function (`mountd_compat_handle_packet`) over byte buffers so it can be unit-tested at the wire level. The thread loop in `mountd_compat.c` only handles I/O: bind, accept, RFC 5531 record-marker reassembly, sendmsg.
## Wire-protocol behaviour summary
References: RFC 5531 (ONC-RPC), RFC 1813 Appendix I (mountd v3), RFC 1833 (rpcbind v2 / PMAPPROC_SET).
- Program != 100005 → `MSG_ACCEPTED / PROG_UNAVAIL`.
- Version != 3 → `MSG_ACCEPTED / PROG_MISMATCH (low=3, high=3)`.
- RPC version != 2 → `MSG_DENIED / RPC_MISMATCH (low=2, high=2)`.
- Auth flavor not in {AUTH_NONE, AUTH_UNIX} → `MSG_DENIED / AUTH_ERROR / AUTH_TOOWEAK`. AUTH_UNIX cred body is byte-skipped after a bounds-checked length read; the content is never dereferenced.
- Procedures NULL / EXPORT / DUMP → success.
- Any other procedure (MNT, UMNT, UMNTALL, anything ≥ 6) → `PROC_UNAVAIL`.
- Malformed call (truncated header, oversize cred/verf length, message-type != CALL) → silent drop per RFC 5531 §10.
## Threat model and security limits
The responder is exposed on the network on the configured port. The hardening posture is "answer everything fast, allocate nothing".
- **Max single packet/record: 8 KiB.** Anything larger is dropped (UDP) or causes the TCP connection to be closed.
- **Max simultaneous TCP connections: 16.** New accepts past the cap are closed immediately. Idle connections are reaped after 30 s of inactivity.
- **No allocations on the request path.** All buffers are stack-resident or owned by the per-connection state. Configuration is snapshotted at start; runtime mutations to `mds_config` do not affect the running responder.
- **Bounds-first parsing.** Cred and verifier lengths are validated against `MOUNTD_AUTH_BODY_MAX` (400 bytes per RFC 5531 §8.1) and against the remaining packet length before any data is read. The cred body is never interpreted; AUTH_UNIX clients with bizarre cred contents are accepted as long as the length fields are sane, because this responder is read-only.
- **Read-only data.** The responder produces the same canned reply for every well-formed call from every client; there is nothing client-controlled that can leak from the daemon's address space.
- **Single static export list.** Frozen at start; reconfigure requires a daemon restart. There is no way to mutate the export list at runtime via this shim.
## rpcbind requirement
`showmount -e <host>` always asks the host's portmapper (rpcbind on port 111) for the mountd port; it does not take a port argument. To support that flow, the responder optionally registers itself with the local rpcbind on startup. If rpcbind is not running on the host, the registration RPC is best-effort: it logs a `WARN:` line and continues serving. Clients that bypass rpcbind (e.g. `rpcinfo -p <host>` followed by a direct TCP connect) still work.
**If you disable rpcbind on the host, `showmount -e` cannot work, regardless of this shim.**
## What's deliberately out of scope
- **Real NFSv3 MOUNT support.** We rely on the protocol-level `PROC_UNAVAIL` to make accidental v3 mounts impossible. Adding real MOUNT support would require committing to NFSv3 namespace semantics that conflict with the NFSv4.1 / pNFS architecture.
- **DS-export discovery proxying.** Forbidden by design. The responder never reaches out to any DS.
- **mountd v1.** Only v3 is advertised. v1 calls get `PROG_MISMATCH`. Pre-1996 clients are not a target.
- **Authoritative `showmount -a`.** DUMP returns an empty mountlist. The MDS does not have an NFSv3 mount table to enumerate.
- **A self-hosted portmapper.** We require rpcbind on the host. Bundling our own port-111 listener would conflict with every other RPC service on the box.
## Upgrade behaviour
On a fresh install or first restart after upgrade, the daemon:
1. Binds UDP and TCP on `0.0.0.0:20048`.
2. Sends a one-shot `PMAPPROC_SET` to the local rpcbind so `100005/3` resolves to the bound port.
3. Logs `INFO: mountd_compat: listening on 0.0.0.0:20048 (udp+tcp), registered with rpcbind`.
What operators see that's new:
* `netstat -plant | grep 20048` shows the daemon listening.
* `rpcinfo -p <mds>` shows mountd v3 advertised.
* External port scanners (`nmap`, monitoring agents) detect a new RPC service on 20048.
Failure modes are soft:
* If port 20048 is already taken (e.g. the host is co-located with `nfs-utils` `rpc.mountd`), the daemon logs `WARN: mountd_compat: TCP bind ... failed` and continues serving NFSv4 normally.
* If rpcbind isn't running, the daemon logs `WARN: rpcbind registration failed; clients can't discover mountd via portmap` and continues serving.
Neither failure affects NFSv4.1 / pNFS service on port 2049.
## Operator workflow
The defaults already satisfy `showmount -e`.  To customise the
advertised path list:
```bash
sudo bash -c 'cat >> /etc/pnfs-mds/mds.conf' <<'EOF'

# showmount -e compatibility shim
mountd_compat_exports = /pnfs
EOF
sudo systemctl restart pnfs-mds
```
Verify from a client:
```bash
showmount -e <mds-host>
# expected:
# Export list for <mds-host>:
# /pnfs *
```
Confirm the MDS rejects v3 mounts (this guarantee holds whether the
shim is on or off):
```bash
mount -t nfs -o vers=3 <mds-host>:/ /mnt/x
# expected: mount.nfs: failed (mountd MNT returns PROC_UNAVAIL)
```
To disable the shim entirely on a given host (suppress the port and
the rpcbind entry):
```bash
sudo bash -c 'cat >> /etc/pnfs-mds/mds.conf' <<'EOF'

mountd_compat_enabled = false
EOF
sudo systemctl restart pnfs-mds
```
After a restart with `mountd_compat_enabled = false`, the responder
is gone and `showmount -e <mds>` returns *RPC: Program not registered*
again.
