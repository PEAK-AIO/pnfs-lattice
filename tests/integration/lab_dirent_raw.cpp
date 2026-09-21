/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lab_dirent_raw.cpp -- Lab instrument: the raw NDB view of the
 * mds_dirents rows of one directory.
 *
 * A dirent that a scan returns but a primary-key read cannot find is
 * either a key-encoding mismatch (the stored entry_name bytes differ
 * from what readers encode for the same printable name) or a row the
 * data node holds in tuple storage without a hash-index element.  The
 * two are told apart by reading each scanned row back by primary key
 * with the EXACT bytes the scan returned: the first case then succeeds,
 * the second still answers 626.
 *
 * Not a ctest.  Usage:
 *   lab_dirent_raw CONNECT_STRING DATABASE PARENT_FILEID
 *
 * Prints, per row: stored entry_name size, its bytes in hex, the
 * decoded name, child_fileid, child_type, the row's GCI and the result
 * of the exact-bytes PK read.  Read-only.
 */

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <ndbapi/NdbApi.hpp>

namespace {

constexpr const char *k_table = "mds_dirents";
constexpr const char *k_col_parent = "parent_fileid";
constexpr const char *k_col_name = "entry_name";
constexpr const char *k_col_child = "child_fileid";
constexpr const char *k_col_type = "child_type";

struct raw_row {
    std::string key_bytes;   /* entry_name exactly as stored (prefix + data) */
    uint64_t child;
    uint32_t type;
    uint64_t gci;
};

int report(const NdbError &err, const char *what)
{
    std::fprintf(stderr, "%s: NDB error %d (%s)\n", what, err.code, err.message);
    return 1;
}

void print_hex(const std::string &s)
{
    for (unsigned char c : s) {
        std::printf("%02x", (unsigned)c);
    }
}

/* PK read with the stored bytes; returns 0 found, 1 not found, -1 error. */
int pk_read_exact(Ndb *ndb, const NdbDictionary::Table *tbl, uint64_t parent,
                  const std::string &key_bytes)
{
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) {
        return -1;
    }
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        ndb->closeTransaction(tx);
        return -1;
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(k_col_parent, static_cast<Uint64>(parent));
    op->equal(k_col_name, key_bytes.data(), (Uint32)key_bytes.size());
    NdbRecAttr *a_child = op->getValue(k_col_child, nullptr);
    if (a_child == nullptr) {
        ndb->closeTransaction(tx);
        return -1;
    }
    if (tx->execute(NdbTransaction::Commit) == -1) {
        NdbError err = tx->getNdbError();
        ndb->closeTransaction(tx);
        return (err.code == 626) ? 1 : -1;
    }
    int rc = (op->getNdbError().code == 626) ? 1 : 0;
    ndb->closeTransaction(tx);
    return rc;
}

int scan_parent(Ndb *ndb, const NdbDictionary::Table *tbl, uint64_t parent,
                std::vector<raw_row> *rows)
{
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) {
        return report(ndb->getNdbError(), "startTransaction");
    }
    NdbScanOperation *scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        NdbError err = tx->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "getNdbScanOperation");
    }
    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        NdbError err = scan->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "readTuples");
    }
    {
        NdbScanFilter filter(scan);
        filter.begin(NdbScanFilter::AND);
        filter.eq(tbl->getColumn(k_col_parent)->getColumnNo(),
                  static_cast<Uint64>(parent));
        filter.end();
    }
    NdbRecAttr *a_name = scan->getValue(k_col_name, nullptr);
    NdbRecAttr *a_child = scan->getValue(k_col_child, nullptr);
    NdbRecAttr *a_type = scan->getValue(k_col_type, nullptr);
    NdbRecAttr *a_gci = scan->getValue(NdbDictionary::Column::ROW_GCI, nullptr);
    if (a_name == nullptr || a_child == nullptr || a_type == nullptr ||
        a_gci == nullptr) {
        NdbError err = scan->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "getValue");
    }
    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        NdbError err = tx->getNdbError();
        ndb->closeTransaction(tx);
        return report(err, "scan execute");
    }
    int rc;
    while ((rc = scan->nextResult(true)) == 0) {
        raw_row r;
        const char *p = a_name->aRef();
        Uint32 sz = a_name->get_size_in_bytes();

        if (p != nullptr && sz > 0) {
            r.key_bytes.assign(p, sz);
        }
        r.child = static_cast<uint64_t>(a_child->u_64_value());
        r.type = a_type->u_8_value();
        r.gci = static_cast<uint64_t>(a_gci->u_64_value());
        rows->push_back(r);
    }
    scan->close();
    ndb->closeTransaction(tx);
    if (rc != 1) {
        std::fprintf(stderr, "scan ended with rc=%d\n", rc);
        return 1;
    }
    return 0;
}

int run(const char *connect_string, const char *database, uint64_t parent)
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

    std::vector<raw_row> rows;
    int rc = scan_parent(&ndb, tbl, parent, &rows);
    if (rc != 0) {
        return rc;
    }
    std::printf("parent %" PRIu64 ": %zu dirent row(s)\n", parent, rows.size());
    for (const raw_row &r : rows) {
        unsigned prefix = r.key_bytes.empty() ? 0U
                          : (unsigned)(unsigned char)r.key_bytes[0];
        std::string decoded = (r.key_bytes.size() > 1)
                              ? r.key_bytes.substr(1) : std::string();
        int pk = pk_read_exact(&ndb, tbl, parent, r.key_bytes);

        std::printf("  stored_size=%zu prefix_len=%u bytes=", r.key_bytes.size(),
                    prefix);
        print_hex(r.key_bytes);
        std::printf(" name=\"%s\" child_fileid=%" PRIu64 " type=%u gci=%" PRIu64
                    " pk_read_exact_bytes=%s\n",
                    decoded.c_str(), r.child, r.type, r.gci,
                    pk == 0 ? "FOUND" : (pk == 1 ? "NOT FOUND (626)" : "ERROR"));
    }
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

} // namespace

int main(int argc, char **argv)
{
    uint64_t parent = 0;
    int rc;

    if (argc != 4 || !parse_u64(argv[3], &parent)) {
        std::fprintf(stderr,
            "usage: lab_dirent_raw CONNECT_STRING DATABASE PARENT_FILEID\n");
        return 1;
    }
    if (ndb_init() != 0) {
        std::fprintf(stderr, "ndb_init failed\n");
        return 1;
    }
    rc = run(argv[1], argv[2], parent);
    ndb_end(0);
    return rc;
}
