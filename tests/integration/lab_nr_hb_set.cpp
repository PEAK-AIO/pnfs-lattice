/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lab_nr_hb_set.cpp -- Lab instrument: overwrite or read one
 * mds_node_registry row's last_heartbeat_ns through the NDB API.
 *
 * The daemon and the dispatchers only ever stamp CLOCK_REALTIME
 * (mds_cluster.h), so a row in the OLD CLOCK_MONOTONIC domain -- what a
 * not-yet-upgraded primary writes during a rolling upgrade -- cannot be
 * produced through the public API.  This tool writes such a value
 * directly so the standby's watchdog can be observed classifying the
 * row as indeterminate (failover_watchdog.c) instead of stale.
 *
 * Not a ctest.  Usage:
 *   lab_nr_hb_set CONNECT_STRING DATABASE MDS_ID            -- print the row
 *   lab_nr_hb_set CONNECT_STRING DATABASE MDS_ID NEW_NS     -- set last_heartbeat_ns
 *
 * One primary-key operation per invocation (read or update), nothing
 * else in the row is touched; boot_epoch is printed so the operator can
 * see which incarnation the row belongs to.
 */

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <ndbapi/NdbApi.hpp>

namespace {

constexpr const char *k_table = "mds_node_registry";
constexpr const char *k_col_mds_id = "mds_id";
constexpr const char *k_col_boot_epoch = "boot_epoch";
constexpr const char *k_col_hb_ns = "last_heartbeat_ns";

int report(const NdbError &err, const char *what)
{
    std::fprintf(stderr, "%s: NDB error %d (%s)\n", what, err.code, err.message);
    return 1;
}

int read_row(Ndb *ndb, const NdbDictionary::Table *tbl, uint32_t mds_id)
{
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) {
        return report(ndb->getNdbError(), "startTransaction");
    }
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        NdbError err = tx->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "getNdbOperation");
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(k_col_mds_id, static_cast<Uint32>(mds_id));
    NdbRecAttr *a_epoch = op->getValue(k_col_boot_epoch, nullptr);
    NdbRecAttr *a_hb = op->getValue(k_col_hb_ns, nullptr);
    if (a_epoch == nullptr || a_hb == nullptr) {
        NdbError err = op->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "getValue");
    }
    if (tx->execute(NdbTransaction::Commit) == -1) {
        NdbError err = tx->getNdbError();
        ndb->closeTransaction(tx);
        if (err.code == 626) {
            std::printf("mds_id=%" PRIu32 ": no row\n", mds_id);
            return 0;
        }
        return report(err, "read");
    }
    std::printf("mds_id=%" PRIu32 " boot_epoch=%" PRIu64
                " last_heartbeat_ns=%" PRIu64 "\n",
                mds_id, static_cast<uint64_t>(a_epoch->u_64_value()),
                static_cast<uint64_t>(a_hb->u_64_value()));
    ndb->closeTransaction(tx);
    return 0;
}

int write_row(Ndb *ndb, const NdbDictionary::Table *tbl, uint32_t mds_id,
              uint64_t new_ns)
{
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) {
        return report(ndb->getNdbError(), "startTransaction");
    }
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        NdbError err = tx->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "getNdbOperation");
    }
    /* updateTuple: the row must exist -- this tool never creates one. */
    if (op->updateTuple() != 0 ||
        op->equal(k_col_mds_id, static_cast<Uint32>(mds_id)) != 0 ||
        op->setValue(k_col_hb_ns, static_cast<Uint64>(new_ns)) != 0) {
        NdbError err = op->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "define update");
    }
    if (tx->execute(NdbTransaction::Commit) == -1) {
        NdbError err = tx->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "update");
    }
    ndb->closeTransaction(tx);
    std::printf("mds_id=%" PRIu32 " last_heartbeat_ns set to %" PRIu64 "\n",
                mds_id, new_ns);
    return 0;
}

bool parse_u64(const char *s, uint64_t *out)
{
    char *end = nullptr;
    unsigned long long v = std::strtoull(s, &end, 10);

    if (s[0] == '\0' || end == nullptr || *end != '\0') {
        return false;
    }
    *out = static_cast<uint64_t>(v);
    return true;
}

/* Every NDB object lives inside this function so all destructors have
 * run before main() calls ndb_end(). */
int run(const char *connect_string, const char *database, uint32_t mds_id,
        bool do_write, uint64_t new_ns)
{
    Ndb_cluster_connection conn(connect_string);

    if (conn.connect(4, 5, 1) != 0) {
        std::fprintf(stderr, "connect(%s) failed\n", connect_string);
        return 1;
    }
    if (conn.wait_until_ready(30, 0) != 0) {
        std::fprintf(stderr, "cluster not ready\n");
        return 1;
    }
    Ndb ndb(&conn, database);
    if (ndb.init() != 0) {
        return report(ndb.getNdbError(), "Ndb::init");
    }
    const NdbDictionary::Table *tbl = ndb.getDictionary()->getTable(k_table);
    if (tbl == nullptr) {
        return report(ndb.getDictionary()->getNdbError(), "getTable");
    }
    return do_write ? write_row(&ndb, tbl, mds_id, new_ns)
                    : read_row(&ndb, tbl, mds_id);
}

} // namespace

int main(int argc, char **argv)
{
    uint64_t id64 = 0;
    uint64_t new_ns = 0;
    bool do_write = false;
    int rc;

    if (argc != 4 && argc != 5) {
        std::fprintf(stderr,
            "usage: lab_nr_hb_set CONNECT_STRING DATABASE MDS_ID [NEW_NS]\n");
        return 1;
    }
    if (!parse_u64(argv[3], &id64) || id64 == 0 || id64 > 128) {
        std::fprintf(stderr, "MDS_ID must be 1..128\n");
        return 1;
    }
    if (argc == 5) {
        if (!parse_u64(argv[4], &new_ns)) {
            std::fprintf(stderr, "NEW_NS must be an unsigned integer\n");
            return 1;
        }
        do_write = true;
    }

    if (ndb_init() != 0) {
        std::fprintf(stderr, "ndb_init failed\n");
        return 1;
    }
    rc = run(argv[1], argv[2], static_cast<uint32_t>(id64), do_write, new_ns);
    ndb_end(0);
    return rc;
}
