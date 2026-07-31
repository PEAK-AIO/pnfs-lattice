/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rondb_schema.c -- Layer 1 RonDB schema + serialisation tests.
 *
 * Pure encoding round-trip tests.  No RonDB cluster needed.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include "pnfs_mds.h"
#include "rondb_schema.h"

static int tests_run;
static int tests_passed;
static int tests_failed;
static int current_test_failed;

#define ASSERT_EQ(a, b) do {						\
	if ((a) != (b)) {						\
		current_test_failed = 1;				\
		fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n",	\
			__FILE__, __LINE__, #a, (int)(a), #b, (int)(b));\
		return;							\
	}								\
} while (0)

#define ASSERT_EQ_U64(a, b) do {					\
	if ((a) != (b)) {						\
		current_test_failed = 1;				\
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		return;							\
	}								\
} while (0)

#define ASSERT_TRUE(x) do {						\
	if (!(x)) {							\
		current_test_failed = 1;				\
		fprintf(stderr, "  FAIL %s:%d: !(%s)\n",		\
			__FILE__, __LINE__, #x);			\
		return;							\
	}								\
} while (0)

#define RUN_TEST(fn) do {						\
	tests_run++;							\
	current_test_failed = 0;					\
	fprintf(stdout, "  %-48s", #fn);				\
	fflush(stdout);							\
	fn();								\
	if (current_test_failed) {					\
		tests_failed++;						\
		fprintf(stdout, "FAIL\n");				\
	} else {							\
		tests_passed++;						\
		fprintf(stdout, "PASS\n");				\
	}								\
} while (0)

/* -----------------------------------------------------------------------
 * 1. Inode serialize/deserialize round-trip
 * ----------------------------------------------------------------------- */

static void test_inode_round_trip(void)
{
	struct mds_inode orig, got;
	uint32_t shard_in = 42, shard_out = 0;
	uint8_t buf[RONDB_INODE_MAX_SIZE];
	int n;

	memset(&orig, 0, sizeof(orig));
	orig.fileid = 100;
	orig.type = MDS_FTYPE_REG;
	orig.mode = 0644;
	orig.nlink = 3;
	orig.uid = 1000;
	orig.gid = 1000;
	orig.size = 4096;
	orig.space_used = 4096;
	orig.atime.tv_sec = 1700000000;
	orig.atime.tv_nsec = 123456789;
	orig.mtime.tv_sec = 1700000001;
	orig.mtime.tv_nsec = 987654321;
	orig.ctime.tv_sec = 1700000002;
	orig.ctime.tv_nsec = 111222333;
	orig.change = 42;
	orig.generation = 7;
	orig.flags = MDS_IFLAG_DS_PENDING;
	orig.create_verf = 0xCAFE;
	orig.parent_fileid = 2;

	n = rondb_inode_serialize(&orig, shard_in, buf, sizeof(buf));
	ASSERT_EQ(n, RONDB_INODE_FIXED_SIZE);

	ASSERT_EQ(rondb_inode_deserialize(buf, (size_t)n, &got, &shard_out), 0);

	ASSERT_EQ_U64(got.fileid, orig.fileid);
	ASSERT_EQ((int)got.type, (int)orig.type);
	ASSERT_EQ(got.mode, orig.mode);
	ASSERT_EQ(got.nlink, orig.nlink);
	ASSERT_EQ_U64(got.uid, orig.uid);
	ASSERT_EQ_U64(got.gid, orig.gid);
	ASSERT_EQ_U64(got.size, orig.size);
	ASSERT_EQ_U64(got.space_used, orig.space_used);
	ASSERT_EQ_U64((uint64_t)got.atime.tv_sec, (uint64_t)orig.atime.tv_sec);
	ASSERT_EQ((int)got.atime.tv_nsec, (int)orig.atime.tv_nsec);
	ASSERT_EQ_U64((uint64_t)got.mtime.tv_sec, (uint64_t)orig.mtime.tv_sec);
	ASSERT_EQ_U64(got.change, orig.change);
	ASSERT_EQ_U64(got.generation, orig.generation);
	ASSERT_EQ(got.flags, orig.flags);
	ASSERT_EQ_U64(got.create_verf, orig.create_verf);
	ASSERT_EQ_U64(got.parent_fileid, orig.parent_fileid);
	ASSERT_EQ(shard_out, shard_in);
}

/* -----------------------------------------------------------------------
 * 2. Inode wire-image trailer boundaries
 *
 * rondb_inode_deserialize() selects each optional trailer purely on the
 * buffer LENGTH, so these constants decide which trailers are decoded at
 * all.  A trailer added to the layout without extending
 * RONDB_INODE_FIXED_SIZE would simply never be read back.
 *
 * Pin all three so a layout change that forgets the boundaries breaks
 * here first.
 * ----------------------------------------------------------------------- */

static void test_inode_size(void)
{
	ASSERT_EQ(RONDB_INODE_V1_SIZE,    137);
	ASSERT_EQ(RONDB_INODE_V8_SIZE,    145);  /* + synth_suid/sgid   */
	ASSERT_EQ(RONDB_INODE_FIXED_SIZE, 285);  /* + v9 inline stripe  */

	/* Boundaries must stay strictly ascending, otherwise the
	 * length gates in rondb_inode_deserialize() overlap. */
	ASSERT_TRUE(RONDB_INODE_V1_SIZE < RONDB_INODE_V8_SIZE);
	ASSERT_TRUE(RONDB_INODE_V8_SIZE < RONDB_INODE_FIXED_SIZE);
	ASSERT_TRUE(RONDB_INODE_FIXED_SIZE <= RONDB_INODE_MAX_SIZE);
}

/* -----------------------------------------------------------------------
 * 2b. An inode carrying the v9 inline single-stripe DS map must survive
 *     a full round-trip.
 *
 * For a single-stripe file this map is the ONLY record of which DS holds
 * the data -- such files have no mds_stripe_maps / mds_stripe_entries
 * rows by design -- so losing it in transit leaves the file's layout
 * unrecoverable.
 * ----------------------------------------------------------------------- */

static void test_inode_inline_stripe_round_trip(void)
{
	struct mds_inode orig, got;
	uint8_t buf[RONDB_INODE_MAX_SIZE];
	uint32_t shard_out = 0;
	int n;
	uint32_t i;

	memset(&orig, 0, sizeof(orig));
	orig.fileid = 216033;
	orig.type = MDS_FTYPE_REG;
	orig.mode = 0100644;
	orig.nlink = 1;
	orig.size = 7;
	orig.flags = MDS_IFLAG_INLINE_STRIPE;
	orig.synth_suid = 4000;
	orig.synth_sgid = 4001;
	orig.inline_ds_id = 7;
	orig.stripe_unit = 1048576;
	orig.inline_fh_len = 36;
	for (i = 0; i < orig.inline_fh_len; i++) {
		orig.inline_fh[i] = (uint8_t)(0x40 + i);
	}

	n = rondb_inode_serialize(&orig, 0, buf, sizeof(buf));
	ASSERT_EQ(n, RONDB_INODE_FIXED_SIZE);
	ASSERT_EQ(rondb_inode_deserialize(buf, (size_t)n, &got,
					  &shard_out), 0);

	ASSERT_EQ(got.flags, (uint32_t)MDS_IFLAG_INLINE_STRIPE);
	/* The v9 trailer: flag set MUST imply a usable DS binding. */
	ASSERT_EQ(got.inline_ds_id, (uint32_t)7);
	ASSERT_EQ(got.stripe_unit, (uint32_t)1048576);
	ASSERT_EQ(got.inline_fh_len, (uint32_t)36);
	ASSERT_EQ(memcmp(got.inline_fh, orig.inline_fh, 36), 0);
	/* v8 synth owner rides in the same image. */
	ASSERT_EQ(got.synth_suid, (uint32_t)4000);
	ASSERT_EQ(got.synth_sgid, (uint32_t)4001);
}

/* -----------------------------------------------------------------------
 * 2c. Trailers are gated on buffer length.
 *
 * This is the upgrade-compatibility contract for a PERSISTED image --
 * e.g. the rename journal's inode_snapshot column -- written by a binary
 * that predates a trailer: the older, shorter image still decodes, with
 * the newer fields defaulting to zero.
 *
 * Note the corollary: because the omitted region defaults to zero, a
 * short image and a full image with zeroed trailers deserialize to the
 * same struct.  Length therefore cannot signal "trailer not fetched";
 * only fetching the columns can.
 * ----------------------------------------------------------------------- */

static void test_inode_trailers_gated_by_length(void)
{
	struct mds_inode orig, got;
	uint8_t buf[RONDB_INODE_MAX_SIZE];

	memset(&orig, 0, sizeof(orig));
	orig.fileid = 4242;
	orig.type = MDS_FTYPE_REG;
	orig.flags = MDS_IFLAG_INLINE_STRIPE;
	orig.synth_suid = 4000;
	orig.synth_sgid = 4001;
	orig.inline_ds_id = 7;
	orig.stripe_unit = 65536;
	orig.inline_fh_len = 4;
	memcpy(orig.inline_fh, "\xDE\xAD\xBE\xEF", 4);

	ASSERT_EQ(rondb_inode_serialize(&orig, 0, buf, sizeof(buf)),
		  RONDB_INODE_FIXED_SIZE);

	/* v1 length: neither trailer decoded. */
	ASSERT_EQ(rondb_inode_deserialize(buf, RONDB_INODE_V1_SIZE,
					  &got, NULL), 0);
	ASSERT_EQ_U64(got.fileid, orig.fileid);
	ASSERT_EQ(got.synth_suid, (uint32_t)0);
	ASSERT_EQ(got.inline_fh_len, (uint32_t)0);

	/* v8 length: synth decoded, inline still absent. */
	ASSERT_EQ(rondb_inode_deserialize(buf, RONDB_INODE_V8_SIZE,
					  &got, NULL), 0);
	ASSERT_EQ(got.synth_suid, (uint32_t)4000);
	ASSERT_EQ(got.inline_fh_len, (uint32_t)0);

	/* Full length: everything decoded. */
	ASSERT_EQ(rondb_inode_deserialize(buf, RONDB_INODE_FIXED_SIZE,
					  &got, NULL), 0);
	ASSERT_EQ(got.synth_suid, (uint32_t)4000);
	ASSERT_EQ(got.inline_fh_len, (uint32_t)4);
}

/* -----------------------------------------------------------------------
 * 2d. Pin the region of the image the serializer actually writes.
 *
 * Serialising the same inode into two differently-poisoned buffers
 * makes every byte the serializer did NOT write show up as a mismatch.
 * The first mismatch therefore marks the end of the real payload, and
 * it must cover everything rondb_inode_deserialize() reads -- otherwise
 * a reader would consume caller stack.
 * ----------------------------------------------------------------------- */

static void test_inode_serializer_covers_all_decoded_bytes(void)
{
	/* base 125 + v8 synth 8 + v9 (ds_id 4 + unit 4 + fh_len 4
	 * + fh 128). */
	const int payload = 125 + 8 + (4 + 4 + 4 + MDS_NFS_FH_MAX);
	struct mds_inode orig;
	uint8_t a[RONDB_INODE_MAX_SIZE];
	uint8_t b[RONDB_INODE_MAX_SIZE];
	int first_diff = -1;
	int i;

	memset(&orig, 0, sizeof(orig));
	orig.fileid = 99;
	orig.type = MDS_FTYPE_REG;
	orig.flags = MDS_IFLAG_INLINE_STRIPE;
	orig.inline_ds_id = 3;
	orig.inline_fh_len = 8;
	memcpy(orig.inline_fh, "\x01\x02\x03\x04\x05\x06\x07\x08", 8);

	memset(a, 0xAA, sizeof(a));
	memset(b, 0x55, sizeof(b));
	ASSERT_EQ(rondb_inode_serialize(&orig, 1, a, sizeof(a)),
		  RONDB_INODE_FIXED_SIZE);
	ASSERT_EQ(rondb_inode_serialize(&orig, 1, b, sizeof(b)),
		  RONDB_INODE_FIXED_SIZE);

	for (i = 0; i < (int)sizeof(a); i++) {
		if (a[i] != b[i]) {
			first_diff = i;
			break;
		}
	}

	/* Everything the deserializer consumes must be genuinely written. */
	ASSERT_EQ(first_diff, payload);

	/* NOTE: payload (273) is 12 bytes short of
	 * RONDB_INODE_FIXED_SIZE (285) -- the three boundary constants
	 * each carry 12 bytes of slack over the fields they describe.
	 * Harmless today because every caller passes the declared size
	 * and the deserializer stops at `payload`, but it means "bytes
	 * written" and "length reported" are NOT the same number.  Do
	 * not narrow the constants without auditing every producer. */
	ASSERT_TRUE(payload < RONDB_INODE_FIXED_SIZE);
}

/* -----------------------------------------------------------------------
 * 3. Stripe entry serialize/deserialize round-trip
 * ----------------------------------------------------------------------- */

static void test_stripe_entry_round_trip(void)
{
	struct mds_ds_map_entry orig, got;
	uint8_t buf[RONDB_STRIPE_ENTRY_SIZE];

	memset(&orig, 0, sizeof(orig));
	orig.ds_id = 7;
	orig.nfs_fh_len = 4;
	memcpy(orig.nfs_fh, "\xDE\xAD\xBE\xEF", 4);

	ASSERT_EQ(rondb_stripe_entry_serialize(&orig, buf, sizeof(buf)),
		  RONDB_STRIPE_ENTRY_SIZE);

	ASSERT_EQ(rondb_stripe_entry_deserialize(buf, sizeof(buf), &got), 0);
	ASSERT_EQ(got.ds_id, (uint32_t)7);
	ASSERT_EQ(got.nfs_fh_len, (uint32_t)4);
	ASSERT_EQ(memcmp(got.nfs_fh, "\xDE\xAD\xBE\xEF", 4), 0);
}

/* -----------------------------------------------------------------------
 * 4. Rename journal entry round-trip
 * ----------------------------------------------------------------------- */

static void test_rj_round_trip(void)
{
	struct rondb_rename_journal_entry orig, got;
	uint8_t buf[RONDB_RJ_MAX_SIZE];
	int n;

	memset(&orig, 0, sizeof(orig));
	orig.txn_id = 12345;
	orig.state = RONDB_RJ_STATE_PREPARED;
	orig.role = RONDB_RJ_ROLE_COORDINATOR;
	orig.coordinator_mds_id = 1;
	orig.src_parent_fileid = 2;
	orig.dst_parent_fileid = 3;
	orig.src_child_fileid = 100;
	snprintf(orig.src_name, sizeof(orig.src_name), "oldfile.txt");
	snprintf(orig.dst_name, sizeof(orig.dst_name), "newfile.txt");
	orig.created_at_ns = 1700000000000000000ULL;

	n = rondb_rj_serialize(&orig, buf, sizeof(buf));
	ASSERT_TRUE(n > 0);

	ASSERT_EQ(rondb_rj_deserialize(buf, (size_t)n, &got), 0);

	ASSERT_EQ_U64(got.txn_id, orig.txn_id);
	ASSERT_EQ(got.state, orig.state);
	ASSERT_EQ(got.role, orig.role);
	ASSERT_EQ(got.coordinator_mds_id, orig.coordinator_mds_id);
	ASSERT_EQ_U64(got.src_parent_fileid, orig.src_parent_fileid);
	ASSERT_EQ_U64(got.dst_parent_fileid, orig.dst_parent_fileid);
	ASSERT_EQ_U64(got.src_child_fileid, orig.src_child_fileid);
	ASSERT_EQ(strcmp(got.src_name, "oldfile.txt"), 0);
	ASSERT_EQ(strcmp(got.dst_name, "newfile.txt"), 0);
	ASSERT_EQ_U64(got.created_at_ns, orig.created_at_ns);
}

/* -----------------------------------------------------------------------
 * 5. Lock resource key: Class 1 (parent/name)
 * ----------------------------------------------------------------------- */

static void test_lock_res_parent_name(void)
{
	uint8_t buf[RONDB_LOCK_KEY_MAX];
	uint64_t hint = 0;
	int len;

	len = rondb_lock_res_parent_name(42, "hello.txt",
					 buf, sizeof(buf), &hint);
	ASSERT_TRUE(len > 0);
	ASSERT_EQ(len, (int)(8 + 9)); /* fileid(8) + strlen("hello.txt") */
	ASSERT_EQ_U64(hint, (uint64_t)42);

	/* First 8 bytes = big-endian fileid 42. */
	ASSERT_EQ_U64(fdb_get_u64(buf), (uint64_t)42);

	/* Remaining bytes = name. */
	ASSERT_EQ(memcmp(buf + 8, "hello.txt", 9), 0);
}

/* -----------------------------------------------------------------------
 * 6. Lock resource key: Class 2 (directory mutation)
 * ----------------------------------------------------------------------- */

static void test_lock_res_dir(void)
{
	uint8_t buf[RONDB_LOCK_KEY_MAX];
	uint64_t hint = 0;
	int len;

	len = rondb_lock_res_dir(99, buf, sizeof(buf), &hint);
	ASSERT_EQ(len, 8);
	ASSERT_EQ_U64(hint, (uint64_t)99);
	ASSERT_EQ_U64(fdb_get_u64(buf), (uint64_t)99);
}

/* -----------------------------------------------------------------------
 * 7. Lock resource key: Class 3 (inode attribute)
 * ----------------------------------------------------------------------- */

static void test_lock_res_inode(void)
{
	uint8_t buf[RONDB_LOCK_KEY_MAX];
	uint64_t hint = 0;
	int len;

	len = rondb_lock_res_inode(500, buf, sizeof(buf), &hint);
	ASSERT_EQ(len, 8);
	ASSERT_EQ_U64(hint, (uint64_t)500);
	ASSERT_EQ_U64(fdb_get_u64(buf), (uint64_t)500);
}

/* -----------------------------------------------------------------------
 * 8. Lock resource key: Class 0 (topology)
 * ----------------------------------------------------------------------- */

static void test_lock_res_topology(void)
{
	uint8_t buf[RONDB_LOCK_KEY_MAX];
	uint64_t hint = 0;
	int len;

	len = rondb_lock_res_topology(buf, sizeof(buf), &hint);
	ASSERT_EQ(len, 8);
	ASSERT_EQ_U64(hint, RONDB_TOPOLOGY_SENTINEL);
	ASSERT_EQ_U64(fdb_get_u64(buf), RONDB_TOPOLOGY_SENTINEL);
}

/* -----------------------------------------------------------------------
 * 9. Table count matches
 * ----------------------------------------------------------------------- */

static void test_table_count(void)
{
	/* Verify RONDB_TABLE_COUNT matches the number of table name macros.
	 * Not automated -- just a sanity check that the constant is right. */
	ASSERT_EQ(RONDB_TABLE_COUNT, 35);
	/* Original 9 tables. */
	ASSERT_TRUE(RONDB_TBL_META[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_INODES[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DIRENTS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_STRIPE_MAPS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_STRIPE_ENTRIES[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_RENAME_JOURNAL[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_NS_LOCKS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_NS_LOCK_HOLDERS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_PARTITION_MAP[0] != '\0');
	/* Phase 1: 11 new tables (no inline_data -- pNFS routes data via DS). */
	ASSERT_TRUE(RONDB_TBL_XATTRS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DS_REGISTRY[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DS_PROVISION[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_QUOTA_RULES[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_QUOTA_USAGE[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_GC_QUEUE[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_LAYOUT_STATE[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_LAYOUT_BY_CLIENT[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_LAYOUT_BY_FILE[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DS_LAYOUT_IDX[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_CLIENT_RECOVERY[0] != '\0');
	/* Phase 9: 2 new tables. */
	ASSERT_TRUE(RONDB_TBL_NODE_REGISTRY[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DELTA_BROADCAST[0] != '\0');
	/* Shared protocol state: 12 new tables. */
	ASSERT_TRUE(RONDB_TBL_OPEN_STATE[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_OPEN_BY_FILE[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_OPEN_BY_CLIENT[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_BYTE_LOCKS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_LOCK_BY_OWNER[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DELEGATIONS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DELEG_BY_FILE[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DELEG_BY_CLIENT[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_SESSIONS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_SESSION_BY_CLIENT[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_CLIENTS[0] != '\0');
	ASSERT_TRUE(RONDB_TBL_DRC_SLOTS[0] != '\0');
}

/* -----------------------------------------------------------------------
 * 10. Lock class ordering: classes are numerically ordered 0 < 1 < 2 < 3
 * ----------------------------------------------------------------------- */

static void test_lock_class_ordering(void)
{
	ASSERT_TRUE(RONDB_LOCK_CLASS_TOPOLOGY < RONDB_LOCK_CLASS_PARENT_NAME);
	ASSERT_TRUE(RONDB_LOCK_CLASS_PARENT_NAME < RONDB_LOCK_CLASS_DIR_MUTATION);
	ASSERT_TRUE(RONDB_LOCK_CLASS_DIR_MUTATION < RONDB_LOCK_CLASS_INODE_ATTR);
}

/* -----------------------------------------------------------------------
 * 11. Lock ordering: canonical total-order for cross-dir rename
 *
 * Simulates the lock acquisition order for:
 *   RENAME(src_parent=5, "old", dst_parent=8, "new", src_child=42)
 * Expected order: (0,sentinel) < (1,5,"old") < (1,8,"new")
 *                 < (2,5) < (2,8) < (3,42)
 * ----------------------------------------------------------------------- */

static void test_lock_order_rename(void)
{
	struct rondb_lock_entry locks[6];
	int i;

	/* Class 0: topology SHARED. */
	locks[0].resource_class = RONDB_LOCK_CLASS_TOPOLOGY;
	locks[0].key_len = rondb_lock_res_topology(
		locks[0].key, sizeof(locks[0].key),
		&locks[0].partition_hint);
	ASSERT_TRUE(locks[0].key_len > 0);

	/* Class 1: (5, "old"). */
	locks[1].resource_class = RONDB_LOCK_CLASS_PARENT_NAME;
	locks[1].key_len = rondb_lock_res_parent_name(
		5, "old", locks[1].key, sizeof(locks[1].key),
		&locks[1].partition_hint);
	ASSERT_TRUE(locks[1].key_len > 0);

	/* Class 1: (8, "new"). */
	locks[2].resource_class = RONDB_LOCK_CLASS_PARENT_NAME;
	locks[2].key_len = rondb_lock_res_parent_name(
		8, "new", locks[2].key, sizeof(locks[2].key),
		&locks[2].partition_hint);
	ASSERT_TRUE(locks[2].key_len > 0);

	/* Class 2: dir(5). */
	locks[3].resource_class = RONDB_LOCK_CLASS_DIR_MUTATION;
	locks[3].key_len = rondb_lock_res_dir(
		5, locks[3].key, sizeof(locks[3].key),
		&locks[3].partition_hint);
	ASSERT_TRUE(locks[3].key_len > 0);

	/* Class 2: dir(8). */
	locks[4].resource_class = RONDB_LOCK_CLASS_DIR_MUTATION;
	locks[4].key_len = rondb_lock_res_dir(
		8, locks[4].key, sizeof(locks[4].key),
		&locks[4].partition_hint);
	ASSERT_TRUE(locks[4].key_len > 0);

	/* Class 3: inode(42). */
	locks[5].resource_class = RONDB_LOCK_CLASS_INODE_ATTR;
	locks[5].key_len = rondb_lock_res_inode(
		42, locks[5].key, sizeof(locks[5].key),
		&locks[5].partition_hint);
	ASSERT_TRUE(locks[5].key_len > 0);

	/* Verify strict ascending order. */
	for (i = 0; i < 5; i++) {
		ASSERT_TRUE(rondb_lock_order_cmp(&locks[i],
						 &locks[i + 1]) < 0);
	}

	/* Equal to self. */
	ASSERT_EQ(rondb_lock_order_cmp(&locks[0], &locks[0]), 0);
}

/* -----------------------------------------------------------------------
 * Main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "test_rondb_schema:\n");

	RUN_TEST(test_inode_round_trip);
	RUN_TEST(test_inode_size);
	RUN_TEST(test_inode_inline_stripe_round_trip);
	RUN_TEST(test_inode_trailers_gated_by_length);
	RUN_TEST(test_inode_serializer_covers_all_decoded_bytes);
	RUN_TEST(test_stripe_entry_round_trip);
	RUN_TEST(test_rj_round_trip);
	RUN_TEST(test_lock_res_parent_name);
	RUN_TEST(test_lock_res_dir);
	RUN_TEST(test_lock_res_inode);
	RUN_TEST(test_lock_res_topology);
	RUN_TEST(test_table_count);
	RUN_TEST(test_lock_class_ordering);
	RUN_TEST(test_lock_order_rename);

	fprintf(stdout, "\ntest_rondb_schema: %d/%d passed\n",
		tests_passed, tests_run);
	return (tests_failed == 0) ? 0 : 1;
}
