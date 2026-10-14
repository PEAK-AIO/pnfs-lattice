#!/usr/bin/env python3
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
"""Client-side metadata round-trip accounting against a mounted pNFS namespace.

Runs, from one client and one thread, N iterations of each operation
class in ONE directory of the mount -- create, getattr, lookup, remove --
one class after the other, and reports per class the client-observed
latency (mean / p50 / p99 / max) and, when an MDS metrics URL is given,
the MDS backend counters that the phase moved, divided by N: backend
round-trip waits, transactions started and transactions committed per
NFS operation (pnfs_mds_ndb_*_total on /metrics; the names say ndb, the
values come from whichever catalogue backend the daemon runs), plus the
catalogue operations the daemon issued per NFS operation
(pnfs_mds_cat_op_latency_seconds_count{cat_op=...} deltas / N), which is
what turns the per-slot transaction shapes into the per-NFS-op cost.

The classes are chosen so each is one NFS compound on a kernel client
mounted with `actimeo=0,lookupcache=none`:
  create   os.open(O_CREAT | O_EXCL)      OPEN(CREATE)          (CLOSE untimed)
  getattr  os.fstat(fd)                   GETATTR of ONE file opened once
                                          before the phase (N compounds on
                                          the same filehandle; an MDS inode
                                          cache may answer them without
                                          touching the store -- the deltas
                                          then show it)
  lookup   os.stat(path)                  LOOKUP + GETATTR, a different file
                                          every time
  remove   os.unlink(path)                REMOVE
Without those mount options the client answers getattr and lookup from
its caches and the numbers measure the client, not the server.

Usage:
  nfs_op_bench.py MOUNT_DIR [N] [--metrics URL] [--label TEXT]

The directory MOUNT_DIR/opbench-<pid> is created and removed again.
Exit status 0 when every operation succeeded, 1 otherwise.
"""
import argparse
import os
import statistics
import sys
import time
import urllib.request

COUNTERS = (
    "pnfs_mds_ndb_exec_roundtrips_total",
    "pnfs_mds_ndb_txn_started_total",
    "pnfs_mds_ndb_txn_committed_total",
)
CAT_OP_PREFIX = "pnfs_mds_cat_op_latency_seconds_count{cat_op=\""


def scrape(url, attempts=3):
    """Return the MDS counters of interest as {name: int}, or None.

    The daemon's scrape listener is single-threaded and closes each
    connection; a reset on a busy moment is retried a few times.
    """
    if not url:
        return None
    body = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                body = resp.read().decode("utf-8", "replace")
            break
        except (OSError, ValueError) as exc:
            print("metrics scrape failed (%d/%d): %s" % (attempt + 1, attempts, exc),
                  file=sys.stderr)
            time.sleep(0.2)
    if body is None:
        return None
    found = {}
    for line in body.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        if parts[0] in COUNTERS:
            found[parts[0]] = int(parts[1])
        elif parts[0].startswith(CAT_OP_PREFIX) and parts[0].endswith("\"}"):
            found["cat_op:" + parts[0][len(CAT_OP_PREFIX):-2]] = int(parts[1])
    return found if all(name in found for name in COUNTERS) else None


def percentile(sorted_samples, pct):
    """Nearest-rank percentile of an ascending list."""
    idx = (len(sorted_samples) * pct) // 100
    return sorted_samples[min(idx, len(sorted_samples) - 1)]


def timed(fn):
    """Run fn(); return (elapsed_us, ok)."""
    start = time.perf_counter_ns()
    try:
        fn()
        ok = True
    except OSError as exc:
        print("  op failed: %s" % exc, file=sys.stderr)
        ok = False
    return (time.perf_counter_ns() - start) // 1000, ok


def run_phase(name, count, op_fn, metrics_url):
    """Run op_fn(i) count times; return a result row.

    The counters are scraped once before and once after the phase, so
    the deltas divided by count are exact per-call attributions as long
    as the phase is the only load on the MDS (heartbeats and health
    probes add a few transactions per minute).
    """
    before = scrape(metrics_url)
    samples = []
    errors = 0
    for i in range(count):
        elapsed, ok = timed(lambda i=i: op_fn(i))
        if ok:
            samples.append(elapsed)
        else:
            errors += 1
    after = scrape(metrics_url)
    row = {"name": name, "ok": len(samples), "err": errors}
    if samples:
        samples.sort()
        row["mean"] = statistics.fmean(samples)
        row["p50"] = percentile(samples, 50)
        row["p99"] = percentile(samples, 99)
        row["max"] = samples[-1]
    if before is not None and after is not None and count > 0:
        row["waits"] = (after[COUNTERS[0]] - before[COUNTERS[0]]) / count
        row["txn"] = (after[COUNTERS[1]] - before[COUNTERS[1]]) / count
        row["commit"] = (after[COUNTERS[2]] - before[COUNTERS[2]]) / count
        row["cat_ops"] = {
            key[len("cat_op:"):]: (after[key] - before.get(key, 0)) / count
            for key in after if key.startswith("cat_op:") and
            after[key] != before.get(key, 0)
        }
    return row


def print_rows(rows, have_metrics):
    """Render the result table."""
    header = "%-8s %7s %5s %9s %8s %8s %8s" % ("op", "ok", "err", "mean_us",
                                                "p50_us", "p99_us", "max_us")
    if have_metrics:
        header += " %9s %8s %10s" % ("waits/op", "txn/op", "commit/op")
    print(header)
    for row in rows:
        line = "%-8s %7d %5d" % (row["name"], row["ok"], row["err"])
        if "mean" in row:
            line += " %9.1f %8d %8d %8d" % (row["mean"], row["p50"], row["p99"],
                                            row["max"])
        else:
            line += " %9s %8s %8s %8s" % ("-", "-", "-", "-")
        if have_metrics and "waits" in row:
            line += " %9.3f %8.3f %10.3f" % (row["waits"], row["txn"], row["commit"])
        print(line)
    if not have_metrics:
        return
    print("catalogue operations issued by the MDS per NFS operation (cat_op deltas / N):")
    for row in rows:
        ops = row.get("cat_ops", {})
        listed = sorted(ops.items(), key=lambda kv: (-kv[1], kv[0]))
        print("  %-8s %s" % (row["name"], ", ".join(
            "%s=%.2f" % (name, per_op) for name, per_op in listed) or "(none)"))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("mount_dir")
    parser.add_argument("count", nargs="?", type=int, default=2000)
    parser.add_argument("--metrics", default="",
                        help="MDS metrics URL, e.g. http://mds0:9090/metrics")
    parser.add_argument("--label", default="", help="free text echoed in the header")
    args = parser.parse_args()
    if args.count <= 0:
        parser.error("N must be positive")

    workdir = os.path.join(args.mount_dir, "opbench-%d" % os.getpid())
    os.mkdir(workdir)
    paths = [os.path.join(workdir, "f%06d" % i) for i in range(args.count)]

    def create(i):
        os.close(os.open(paths[i], os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))

    def lookup(i):
        os.stat(paths[i])

    def remove(i):
        os.unlink(paths[i])

    print("nfs_op_bench %s: %d x create/getattr/lookup/remove in %s%s" %
          (args.label, args.count, workdir,
           ", MDS counters from %s" % args.metrics if args.metrics else ""))
    rows = [run_phase("create", args.count, create, args.metrics)]
    # getattr: one descriptor opened once, outside the phase, so every
    # timed call is exactly one GETATTR compound on that filehandle.
    fd = os.open(paths[0], os.O_RDONLY)
    rows.append(run_phase("getattr", args.count, lambda _i: os.fstat(fd), args.metrics))
    os.close(fd)
    rows.append(run_phase("lookup", args.count, lookup, args.metrics))
    rows.append(run_phase("remove", args.count, remove, args.metrics))
    os.rmdir(workdir)

    print_rows(rows, bool(args.metrics))
    return 0 if all(row["err"] == 0 for row in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
