# Companion processes
Operator guide for the `companion` module: helper programs that the
`pnfs-mds` daemon starts, supervises, and shuts down together with
itself.
## What it is for
Sites often run small helper programs next to a metadata server —
ingest feeders, exporters, index maintainers, cleanup jobs. Managing
them out of band means their lifetime is unrelated to the daemon they
cooperate with: they survive a daemon stop, they are invisible to
`mds-admin`, and nothing stops one from exhausting the resources the
daemon needs.
A companion fixes that. The daemon owns the process:
- It starts when the daemon starts and is terminated when the daemon
  stops.
- Failures are visible and, if you want, retried with backoff.
- Memory and CPU ceilings are applied to the child, not to the daemon.
- Its declared RonDB `[api]` slot usage is accounted for.
The feature is inert out of the box. With no `companion.<name>.*` keys
in `mds.conf`, no supervisor thread is created and nothing is spawned.
## Trust model — read this first
Companions can only be declared in `mds.conf`. That file is the
allowlist, and it is the *only* place an executable path or argument
can come from.
`mds-admin companion start|stop|restart` sends an action plus a
**name** to the daemon. It cannot send a path, an argument vector, a
resource limit, or a shell string. An operator with access to the
admin port can therefore start and stop the programs you approved, and
nothing else.
Consequences worth being explicit about:
- **Adding or changing a companion requires editing `mds.conf` and
  restarting the daemon.** There is no reload path and no way to run a
  one-off command. This is deliberate.
- Programs are executed with `execv()` against an absolute path. No
  shell is involved, `PATH` is never consulted, and each argument is a
  separate config key — so quoting, word splitting, and globbing simply
  do not exist here.
- A malformed declaration fails daemon startup rather than failing
  later at spawn time on a serving node.
## A first declaration
```ini
companion.indexer.exec           = /opt/site/bin/indexer
companion.indexer.arg[0]         = --source
companion.indexer.arg[1]         = /mnt/pnfs/incoming
companion.indexer.workdir        = /var/lib/site
companion.indexer.log_file       = /var/log/pnfs-companion-indexer.log
companion.indexer.restart        = on-failure
companion.indexer.max_restarts   = 5
companion.indexer.rlimit_as_mb   = 4096
companion.indexer.ndb_conns      = 1
```
That runs:
```
/opt/site/bin/indexer --source /mnt/pnfs/incoming
```
with `argv[0]` set to the exec path, `cwd` at `/var/lib/site`, stdout
and stderr appended to the log file, a 4 GiB address-space ceiling, and
one NDB API slot declared for budgeting.
Restart the daemon, then confirm:
```sh
mds-admin companion list
```
```
NAME                      STATE      PID      RESTARTS  LAST_EXIT  RSS_MB     NDB_CONNS
indexer                   running    48231    0         -          182        1
```
## Config keys
All keys are optional except `exec`. Out-of-range numbers log a `WARN`
and keep the default; structurally broken declarations abort startup
with an error.
### Per companion
- `companion.<name>.exec` — **required.** Absolute path to the
  executable. `PATH` is never searched.
- `companion.<name>.arg[N]` — one argument per key, `N` from 0 to 11.
  Indexes must be contiguous from 0; a gap aborts startup. Values are
  passed through verbatim.
- `companion.<name>.workdir` — absolute `chdir` target. Default `/`.
- `companion.<name>.log_file` — absolute path receiving stdout and
  stderr, opened append+create, mode 0640. Default: inherit the
  daemon's descriptors, which normally means the journal. The daemon
  does not create parent directories and does not rotate this file.
- `companion.<name>.enabled` — `true|false`. Default `true`. A
  disabled declaration is still listed (as `disabled`) but can never
  be started.
- `companion.<name>.autostart` — `true|false`. Default `true`. When
  false the companion is declared but idle until an admin request
  starts it.
- `companion.<name>.restart` — `never|on-failure|always`. Default
  `on-failure`. `on-failure` treats exit status 0 as "work finished"
  and leaves the companion `stopped`; anything else, including death by
  signal, is a failure.
- `companion.<name>.restart_backoff_ms` — first retry delay,
  100..600000. Default 1000. Doubles after each retry.
- `companion.<name>.restart_backoff_max_ms` — backoff ceiling,
  100..3600000. Default 60000. A value below the initial backoff is
  raised to match it, with a warning.
- `companion.<name>.max_restarts` — retries allowed per failure burst,
  0..1000. Default 5. `0` means never restart. Once exhausted the
  companion sits in `failed` until an operator starts it.
- `companion.<name>.restart_reset_sec` — uptime that clears the burst
  counter, 1..86400. Default 60. A companion that stays up this long
  gets a fresh restart budget after its next failure.
- `companion.<name>.stop_timeout_ms` — `SIGTERM`-to-`SIGKILL` grace,
  100..600000. Default 10000.
- `companion.<name>.start_delay_ms` — delay before the first spawn at
  daemon start, 0..600000. Default 0. Useful for staggering helpers
  that would otherwise all hit the catalogue at once. An explicit
  `mds-admin companion start` ignores this and spawns immediately.
- `companion.<name>.rlimit_as_mb` — `RLIMIT_AS` in MiB, 0..1048576.
  Default 0 (inherit).
- `companion.<name>.rlimit_nofile` — `RLIMIT_NOFILE`, 0..1048576.
  Default 0 (inherit).
- `companion.<name>.rlimit_cpu_sec` — `RLIMIT_CPU` in seconds,
  0..31536000. Default 0 (inherit). Exceeding it kills the child with
  `SIGKILL`.
- `companion.<name>.systemd_scope` — `true|false`. Default false. Runs
  the child inside a transient systemd scope so cgroup memory pressure
  kills only the companion. Requires `memory_max_mb`.
- `companion.<name>.memory_max_mb` — `MemoryMax` for the scope,
  0..1048576. Required when `systemd_scope = true`.
- `companion.<name>.ndb_conns` — declared RonDB API connections,
  0..64. Default 0. Advisory accounting only; see below.
Names may contain `A-Z`, `a-z`, `0-9`, `-`, and `_`, up to 63
characters. A `.` is not allowed because it separates the name from the
field in the key. At most 8 companions may be declared per node.
### Node-wide
- `companion_enabled` — master switch. Default `true`. Setting it
  `false` keeps declarations in place and starts nothing.
- `ndb_api_slots_total` — total `[api]` slots in the RonDB cluster,
  0..255. Default 0, meaning undeclared: free-slot reporting is
  suppressed until you set it.
- `ndb_api_slots_reserved` — slots to hold back, 0..255. Default 0.
  Use this for peer MDS daemons and transient tools this node cannot
  see.
- `companion_ndb_admission` — `advisory|enforce`. Default `advisory`.
## Resource fencing
Two independent mechanisms, and they compose:
`setrlimit` is applied in the child before `exec` whenever the
corresponding `rlimit_*` key is non-zero. It needs nothing installed
and works everywhere, but the failure mode is a bit blunt: an
allocation failure inside the child, or `SIGKILL` on CPU exhaustion.
A **systemd scope** is used when `systemd_scope = true` and
`systemd-run` is present. The child is launched as:
```
systemd-run --scope --collect -p MemoryMax=<N>M \
    -u pnfs-companion-<name>.scope -- <exec> <args...>
```
Under memory pressure the cgroup OOM killer targets the scope, so the
companion dies and the daemon does not.
If `systemd_scope = true` but `systemd-run` cannot be found, the daemon
logs a warning and starts the program **without** the scope rather than
refusing to run it. Any `rlimit_*` keys still apply. Note that when a
scope is in use the rlimits also constrain the `systemd-run` wrapper
itself, so do not set `rlimit_as_mb` unreasonably low.
## RonDB API slot budget
Each `[api]` section in the RonDB `config.ini` is one slot. Every MDS
connection pool entry consumes one, and so does every helper that opens
its own NDB connection. Run out and new connections fail with an
unhelpful `4009 "No data node(s) available"`.
Declare what you have and what each companion costs, and the daemon
will do the arithmetic:
```ini
ndb_api_slots_total    = 48
ndb_api_slots_reserved = 8
companion.indexer.ndb_conns = 1
companion.etl.ndb_conns     = 2
```
```sh
mds-admin companion budget
```
```
NDB API slots (declared accounting, this node only):
  total:                 48
  mds pool:              16
  reserved:              8
  companions declared:   3 (2 companion(s))
  companions running:    1 (1 running)
  free:                  23
  admission:             advisory
```
`free` is `total - mds pool - reserved - companions running`. It is
**signed**: an over-committed node reports a negative number rather
than silently clamping at zero.
### Admission control
With `companion_ndb_admission = advisory` (the default) a spawn that
would exceed the declared total logs a warning and proceeds. The
reasoning: a mis-declared budget should not stop a program you asked
for from running.
With `enforce`, that spawn is refused and the admin request returns an
error pointing at the budget. Use it once you trust your declarations.
### Two honest limitations
1. **`ndb_conns` is intent, not measurement.** The daemon cannot see
   how many connections a child actually opens. If a helper opens three
   and you declared one, the accounting is wrong and nothing detects
   it.
2. **The view is per node.** This daemon knows its own pool and its own
   companions. It cannot see peer MDS daemons, their companions, or
   someone running a diagnostic tool by hand. `ndb_api_slots_reserved`
   is the mechanism for carving out that headroom.
RonDB refusing a connection remains the only authoritative signal that
the cluster is genuinely out of slots. This budget is a planning aid.
### Finding the real slot count
Count `[api]` sections in the cluster `config.ini`, or ask the
management node:
```sh
ndb_mgm -c <mgm_host>:1186 -e show | grep -c "^id=[0-9].*type: api"
```
## Admin commands
All commands accept `--mds-host` and `--mds-port` for the admin
endpoint, and `--json` where output is structured.
```sh
mds-admin companion list [--json]
mds-admin companion status <name> [--json]
mds-admin companion start <name>
mds-admin companion stop <name>
mds-admin companion restart <name>
mds-admin companion budget [--json] [--total-api-slots N]
```
`start` on a `failed` companion clears its restart burst, so it is also
the recovery action after a companion has given up.
`stop` is deliberate: the restart policy is not applied, so a stopped
companion stays stopped. It sends `SIGTERM` to the child's process
group, waits `stop_timeout_ms`, then escalates to `SIGKILL`. The
command returns as soon as the stop is requested.
`budget --total-api-slots N` is a local what-if. It recomputes the free
figure against `N` for display only, without touching the daemon —
useful because the configured total otherwise needs a restart to
change.
### States
- `stopped` — declared and startable, not running.
- `starting` — spawn scheduled, waiting out `start_delay_ms`.
- `running` — child is alive.
- `backoff` — failed, waiting out the restart delay.
- `failed` — restart budget exhausted, or a failure under
  `restart = never`. No further automatic restarts.
- `disabled` — `enabled = false`.
## Process behaviour
Things worth knowing when writing or adopting a companion program:
- Each child gets **its own process group**. Signals from the
  supervisor go to the whole group, so a helper that forks workers is
  torn down as a unit.
- The child's signal mask is cleared before `exec`. The daemon blocks
  `SIGINT`/`SIGTERM` in its own threads; without this a companion would
  inherit that and ignore ordinary termination requests.
- Every inherited descriptor above stderr is closed, so companions
  cannot hold catalogue connections, listening sockets, TLS state, or
  the daemon log open.
- stdin is `/dev/null`. Companions are never interactive.
- Two exit codes are reserved for pre-exec failures: **127** means
  `execv` failed (usually a bad `exec` path), **126** means child setup
  failed (`workdir` or `log_file` not usable).
- Children are reaped with targeted `waitpid()` calls on pids the
  module created. The daemon installs no `SIGCHLD` handler.
### Shutdown ordering
On daemon shutdown, companions are terminated *after* the cluster
transport stops. That ordering is required for memory safety: an admin
connection thread may still be inside a companion handler, and stopping
the transport is what guarantees those threads have finished.
Practically, this means a companion may briefly outlive the RPC
listeners during shutdown. Do not write a companion that assumes the
daemon is still serving metadata when it receives `SIGTERM`.
## Building without the module
`-DENABLE_COMPANION=OFF` links stub implementations: nothing is
spawned, and admin requests report that the feature is unavailable.
`companion.<name>.*` keys are still parsed and validated — they simply
have no effect.
Note that `mds-admin companion budget` still reports the declared
total, this daemon's pool, and reserved headroom in that build, since
those come from config rather than from the supervisor.
## Troubleshooting
**Daemon refuses to start after adding a companion.** The declaration
is structurally invalid. The startup error names the key: a relative
`exec`, `workdir`, or `log_file`; a missing `exec`; a gap in `arg[N]`;
or `systemd_scope` without `memory_max_mb`.
**`companion list` shows `failed` with `LAST_EXIT` 127.** `execv`
failed — the `exec` path does not exist or is not executable.

**`failed` with `LAST_EXIT` 126.** Child setup failed before exec:
check that `workdir` exists and that `log_file`'s parent directory
exists and is writable by the daemon user.
**A companion flaps and then stops restarting.** It exhausted
`max_restarts` within `restart_reset_sec`. Fix the underlying problem,
then `mds-admin companion start <name>`.
**`start` returns "no companion named X is declared".** The name is not
in that node's `mds.conf`, or the daemon has not been restarted since
the key was added.
**Admin command reports the feature is not active.** Either the build
used `ENABLE_COMPANION=OFF`, `companion_enabled = false`, or no
companions are declared on that node.
