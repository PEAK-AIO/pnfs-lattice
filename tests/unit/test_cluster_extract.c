/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_cluster_extract.c -- Unit tests for the backend-neutral cluster
 * services extraction.
 *
 * A fabricated struct mds_catalogue carrying fake struct mds_cluster_ops
 * (and fake lifecycle slots) drives the daemon-side consumers exactly
 * as the dispatchers do, without any backend:
 *   - subtree_map_init_from_catalogue / refresh_from_catalogue /
 *     seed_shards over mds_cluster_partition_list / _put: a failed
 *     list is fatal after a bounded retry and never seeds root, the
 *     root claim is insert-only and EXISTS means another node owns
 *     root, shard seeding stays an upsert;
 *   - subtree_map_failover_take_over over mds_cluster_partition_cas:
 *     every partner-owned row is CAS'd (expected = partner) before its
 *     in-memory entry moves, a STALE row is skipped and stays the
 *     partner's, and a store without the slot keeps the memory-only
 *     behaviour;
 *   - cluster_membership_populate over mds_cluster_node_list, which
 *     merges only the registry's address fields into existing members
 *     (self keeps its configured standby role and partner);
 *   - failover_watchdog_start refusing with NOSUPPORT when the
 *     node_scan_stale slot is absent, and the heartbeat plausibility
 *     rule: a partner row below the realtime floor is indeterminate
 *     and never triggers a promotion attempt;
 *   - the mds_cluster_* / mds_catalogue_image_feed_* dispatchers'
 *     INVAL / NOSUPPORT / pass-through contract (C4);
 *   - the backend name table (pnfs_common: name <-> enum), the core's
 *     registration table (availability, open refusal) and the config
 *     parser's use of the former in both build flavours.
 */

#include <fcntl.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "catalogue_backend_names.h"
#include "mds_cluster.h"
#include "catalogue_internal.h"
#include "catalog_image.h"
#include "subtree_map.h"
#include "cluster_membership.h"
#include "failover.h"
#include "failover_watchdog.h"

/* -------------------------------------------------------------------
 * Test infrastructure
 * ------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;
static int test_failed;

#define ASSERT_EQ(a, b) do {                                    \
    if ((a) != (b)) {                                           \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, (long long)(a),         \
                #b, (long long)(b));                            \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define ASSERT_TRUE(cond) do {                                  \
    if (!(cond)) {                                              \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n",               \
                __FILE__, __LINE__, #cond);                     \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define ASSERT_STREQ(a, b) do {                                 \
    if (strcmp((a), (b)) != 0) {                                \
        fprintf(stderr, "  FAIL %s:%d: \"%s\" != \"%s\"\n",     \
                __FILE__, __LINE__, (a), (b));                  \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define RUN_TEST(fn) do {                                       \
    tests_run++;                                                \
    test_failed = 0;                                            \
    fprintf(stdout, "  %-52s", #fn);                            \
    fflush(stdout);                                             \
    fn();                                                       \
    if (!test_failed) {                                         \
        tests_passed++;                                         \
        fprintf(stdout, "PASS\n");                              \
    } else {                                                    \
        fprintf(stdout, "FAIL\n");                              \
    }                                                           \
} while (0)

/* -------------------------------------------------------------------
 * Fake cluster services
 *
 * Rows and recorded calls live in file-scope arrays with fixed
 * capacities; every test starts from fake_reset().
 * ------------------------------------------------------------------- */

#define FAKE_ROWS_MAX 8
#define FAKE_CALLS_MAX 16

struct fake_pm_row {
    uint32_t    partition_id;
    uint32_t    owner;
    uint8_t     state;
    const char *path;
};

struct fake_pm_put {
    uint32_t partition_id;
    uint32_t owner;
    uint8_t  state;
    bool     insert_only;
    char     path[64];
};

struct fake_node_row {
    uint32_t    mds_id;
    uint64_t    boot_epoch;
    const char *hostname;
    uint16_t    nfs_port;
    uint16_t    grpc_port;
    uint64_t    last_heartbeat_ns;
};

static struct fake_pm_row   fake_pm_rows[FAKE_ROWS_MAX];
static uint32_t             fake_pm_row_count;
static enum mds_status      fake_pm_list_status;
static uint32_t             fake_pm_list_calls;
/* When non-zero, list calls numbered below this deliver no rows: the
 * rows "appear" between two startup attempts (a peer's root claim, or
 * this node's own in-doubt insert landing). */
static uint32_t             fake_pm_reveal_at_call;
static struct fake_pm_put   fake_pm_puts[FAKE_CALLS_MAX];
static uint32_t             fake_pm_put_count;
static enum mds_status      fake_pm_put_status;

struct fake_pm_cas {
    uint32_t partition_id;
    uint32_t expected_owner;
    uint32_t new_owner;
    uint8_t  new_state;
};

static struct fake_pm_cas   fake_pm_cas_calls[FAKE_CALLS_MAX];
static uint32_t             fake_pm_cas_count;
/* Forced status for every CAS; MDS_OK means "apply the row rule":
 * NOTFOUND for an unknown partition_id, STALE when the row's owner is
 * not the expected one, otherwise the row is rewritten. */
static enum mds_status      fake_pm_cas_status;

static struct fake_node_row fake_node_rows[FAKE_ROWS_MAX];
static uint32_t             fake_node_row_count;
static enum mds_status      fake_node_list_status;

static struct {
    uint32_t mds_id;
    uint64_t boot_epoch;
    char     hostname[64];
    uint16_t nfs_port;
    uint16_t grpc_port;
    uint32_t calls;
}                           fake_register;
static struct {
    uint32_t mds_id;
    uint64_t boot_epoch;
    uint32_t calls;
}                           fake_heartbeat, fake_deregister;
static enum mds_status      fake_heartbeat_status;

static void fake_reset(void)
{
    memset(fake_pm_rows, 0, sizeof(fake_pm_rows));
    fake_pm_row_count = 0;
    fake_pm_list_status = MDS_OK;
    fake_pm_list_calls = 0;
    fake_pm_reveal_at_call = 0;
    memset(fake_pm_puts, 0, sizeof(fake_pm_puts));
    fake_pm_put_count = 0;
    fake_pm_put_status = MDS_OK;
    memset(fake_pm_cas_calls, 0, sizeof(fake_pm_cas_calls));
    fake_pm_cas_count = 0;
    fake_pm_cas_status = MDS_OK;
    memset(fake_node_rows, 0, sizeof(fake_node_rows));
    fake_node_row_count = 0;
    fake_node_list_status = MDS_OK;
    memset(&fake_register, 0, sizeof(fake_register));
    memset(&fake_heartbeat, 0, sizeof(fake_heartbeat));
    memset(&fake_deregister, 0, sizeof(fake_deregister));
    fake_heartbeat_status = MDS_OK;
}

static void fake_pm_add(uint32_t id, uint32_t owner, uint8_t state,
                        const char *path)
{
    if (fake_pm_row_count < FAKE_ROWS_MAX) {
        fake_pm_rows[fake_pm_row_count].partition_id = id;
        fake_pm_rows[fake_pm_row_count].owner = owner;
        fake_pm_rows[fake_pm_row_count].state = state;
        fake_pm_rows[fake_pm_row_count].path = path;
        fake_pm_row_count++;
    }
}

static void fake_node_add(uint32_t mds_id, const char *hostname,
                          uint16_t nfs_port, uint16_t grpc_port,
                          uint64_t last_heartbeat_ns)
{
    if (fake_node_row_count < FAKE_ROWS_MAX) {
        struct fake_node_row *r = &fake_node_rows[fake_node_row_count];

        r->mds_id = mds_id;
        r->boot_epoch = 100 + mds_id;
        r->hostname = hostname;
        r->nfs_port = nfs_port;
        r->grpc_port = grpc_port;
        r->last_heartbeat_ns = last_heartbeat_ns;
        fake_node_row_count++;
    }
}

static uint64_t realtime_now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void sleep_ms(unsigned ms)
{
    struct timespec ts;

    ts.tv_sec = (time_t)(ms / 1000U);
    ts.tv_nsec = (long)((ms % 1000U) * 1000000U);
    (void)nanosleep(&ts, NULL);
}

static enum mds_status fake_node_register(struct mds_catalogue *cat,
                                          uint32_t mds_id,
                                          uint64_t boot_epoch,
                                          const char *hostname,
                                          uint16_t nfs_port,
                                          uint16_t grpc_port)
{
    (void)cat;
    fake_register.mds_id = mds_id;
    fake_register.boot_epoch = boot_epoch;
    (void)snprintf(fake_register.hostname, sizeof(fake_register.hostname),
                   "%s", hostname);
    fake_register.nfs_port = nfs_port;
    fake_register.grpc_port = grpc_port;
    fake_register.calls++;
    return MDS_OK;
}

static enum mds_status fake_node_heartbeat(struct mds_catalogue *cat,
                                           uint32_t mds_id,
                                           uint64_t boot_epoch)
{
    (void)cat;
    fake_heartbeat.mds_id = mds_id;
    fake_heartbeat.boot_epoch = boot_epoch;
    fake_heartbeat.calls++;
    return fake_heartbeat_status;
}

static enum mds_status fake_node_deregister(struct mds_catalogue *cat,
                                            uint32_t mds_id,
                                            uint64_t boot_epoch)
{
    (void)cat;
    fake_deregister.mds_id = mds_id;
    fake_deregister.boot_epoch = boot_epoch;
    fake_deregister.calls++;
    return MDS_OK;
}

static enum mds_status fake_node_list(struct mds_catalogue *cat,
                                      mds_cluster_node_cb cb, void *ctx)
{
    uint32_t i;

    (void)cat;
    if (fake_node_list_status != MDS_OK) {
        return fake_node_list_status;
    }
    for (i = 0; i < fake_node_row_count; i++) {
        const struct fake_node_row *r = &fake_node_rows[i];

        if (cb(r->mds_id, r->boot_epoch, r->hostname, r->nfs_port,
               r->grpc_port, r->last_heartbeat_ns, ctx) != 0) {
            break;
        }
    }
    return MDS_OK;
}

static enum mds_status fake_node_scan_stale(struct mds_catalogue *cat,
                                            uint64_t threshold_ns,
                                            mds_cluster_stale_cb cb,
                                            void *ctx)
{
    uint32_t i;

    (void)cat;
    for (i = 0; i < fake_node_row_count; i++) {
        const struct fake_node_row *r = &fake_node_rows[i];

        if (r->last_heartbeat_ns < threshold_ns &&
            cb(r->mds_id, r->boot_epoch, r->last_heartbeat_ns, ctx) != 0) {
            break;
        }
    }
    return MDS_OK;
}

static enum mds_status fake_partition_list(struct mds_catalogue *cat,
                                           mds_cluster_partition_cb cb,
                                           void *ctx)
{
    uint32_t i;

    (void)cat;
    fake_pm_list_calls++;
    if (fake_pm_list_status != MDS_OK) {
        return fake_pm_list_status;
    }
    if (fake_pm_reveal_at_call != 0 &&
        fake_pm_list_calls < fake_pm_reveal_at_call) {
        return MDS_OK;
    }
    for (i = 0; i < fake_pm_row_count; i++) {
        const struct fake_pm_row *r = &fake_pm_rows[i];

        if (cb(r->partition_id, r->owner, r->state, r->path, ctx) != 0) {
            break;
        }
    }
    return MDS_OK;
}

static enum mds_status fake_partition_put(struct mds_catalogue *cat,
                                          uint32_t partition_id,
                                          uint32_t owner_mds_id,
                                          uint8_t state,
                                          const char *subtree_path,
                                          bool insert_only)
{
    (void)cat;
    if (fake_pm_put_count < FAKE_CALLS_MAX) {
        struct fake_pm_put *p = &fake_pm_puts[fake_pm_put_count];

        p->partition_id = partition_id;
        p->owner = owner_mds_id;
        p->state = state;
        p->insert_only = insert_only;
        (void)snprintf(p->path, sizeof(p->path), "%s", subtree_path);
    }
    fake_pm_put_count++;
    return fake_pm_put_status;
}

static enum mds_status fake_partition_cas(struct mds_catalogue *cat,
                                          uint32_t partition_id,
                                          uint32_t expected_owner,
                                          uint32_t new_owner,
                                          uint8_t new_state)
{
    uint32_t i;

    (void)cat;
    if (fake_pm_cas_count < FAKE_CALLS_MAX) {
        struct fake_pm_cas *c = &fake_pm_cas_calls[fake_pm_cas_count];

        c->partition_id = partition_id;
        c->expected_owner = expected_owner;
        c->new_owner = new_owner;
        c->new_state = new_state;
    }
    fake_pm_cas_count++;
    if (fake_pm_cas_status != MDS_OK) {
        return fake_pm_cas_status;
    }
    for (i = 0; i < fake_pm_row_count; i++) {
        struct fake_pm_row *r = &fake_pm_rows[i];

        if (r->partition_id != partition_id) {
            continue;
        }
        if (r->owner != expected_owner) {
            return MDS_ERR_STALE;
        }
        r->owner = new_owner;
        r->state = new_state;
        return MDS_OK;
    }
    return MDS_ERR_NOTFOUND;
}

static const struct mds_cluster_ops fake_ops_full = {
    .node_register   = fake_node_register,
    .node_heartbeat  = fake_node_heartbeat,
    .node_deregister = fake_node_deregister,
    .node_list       = fake_node_list,
    .node_scan_stale = fake_node_scan_stale,
    .partition_list  = fake_partition_list,
    .partition_put   = fake_partition_put,
    .partition_cas   = fake_partition_cas,
};

/* Everything but the stale scan: what the watchdog needs is missing. */
static const struct mds_cluster_ops fake_ops_no_stale = {
    .node_register   = fake_node_register,
    .node_heartbeat  = fake_node_heartbeat,
    .node_deregister = fake_node_deregister,
    .node_list       = fake_node_list,
    .partition_list  = fake_partition_list,
    .partition_put   = fake_partition_put,
    .partition_cas   = fake_partition_cas,
};

/* A partition map that can list and put but not CAS: the pre-slot
 * store, whose takeover stays memory-only. */
static const struct mds_cluster_ops fake_ops_no_cas = {
    .node_register   = fake_node_register,
    .node_heartbeat  = fake_node_heartbeat,
    .node_deregister = fake_node_deregister,
    .node_list       = fake_node_list,
    .node_scan_stale = fake_node_scan_stale,
    .partition_list  = fake_partition_list,
    .partition_put   = fake_partition_put,
};

/* A registry without a partition map. */
static const struct mds_cluster_ops fake_ops_registry_only = {
    .node_register   = fake_node_register,
    .node_heartbeat  = fake_node_heartbeat,
    .node_deregister = fake_node_deregister,
    .node_list       = fake_node_list,
    .node_scan_stale = fake_node_scan_stale,
};

/* -------------------------------------------------------------------
 * Fake lifecycle slots (image feed)
 * ------------------------------------------------------------------- */

static struct {
    struct mds_catalogue *cat;
    struct catalog_image *image;
    uint32_t              self_mds_id;
    uint32_t              poll_interval_ms;
    uint32_t              start_calls;
    uint32_t              stop_calls;
}                      fake_feed;
static enum mds_status fake_feed_start_status;

static enum mds_status fake_image_feed_start(struct mds_catalogue *cat,
                                             struct catalog_image *image,
                                             uint32_t self_mds_id,
                                             uint32_t poll_interval_ms)
{
    fake_feed.cat = cat;
    fake_feed.image = image;
    fake_feed.self_mds_id = self_mds_id;
    fake_feed.poll_interval_ms = poll_interval_ms;
    fake_feed.start_calls++;
    return fake_feed_start_status;
}

static void fake_image_feed_stop(struct mds_catalogue *cat)
{
    fake_feed.cat = cat;
    fake_feed.stop_calls++;
}

static const struct mds_catalogue_ops fake_lifecycle_no_feed = {
    .close = NULL,
    .probe = NULL,
};

static const struct mds_catalogue_ops fake_lifecycle_with_feed = {
    .image_feed_start = fake_image_feed_start,
    .image_feed_stop  = fake_image_feed_stop,
};

static const struct mds_catalogue_ops fake_lifecycle_start_only = {
    .image_feed_start = fake_image_feed_start,
};

/* -------------------------------------------------------------------
 * Fabricated catalogue handles
 *
 * The dispatchers touch only backend, caps, ops and cluster_ops.  The
 * handle is never passed to mds_catalogue_close(), so no backend
 * resources exist to release.
 * ------------------------------------------------------------------- */

static struct mds_catalogue fake_cat_storage;

static struct mds_catalogue *make_fake_cat(const struct mds_cluster_ops *cops,
                                           const struct mds_catalogue_ops *lops,
                                           uint32_t caps)
{
    memset(&fake_cat_storage, 0, sizeof(fake_cat_storage));
    fake_cat_storage.backend = MDS_BACKEND_MEMDB;
    fake_cat_storage.caps = caps;
    fake_cat_storage.ops = lops;
    fake_cat_storage.cluster_ops = cops;
    return &fake_cat_storage;
}

static void make_test_config(struct mds_config *cfg, uint32_t id,
                             const char *hostname)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->self.id = id;
    (void)snprintf(cfg->self.hostname, sizeof(cfg->self.hostname), "%s",
                   hostname);
    cfg->self.nfs_port = 2049;
    cfg->self.grpc_port = 50051;
    cfg->cluster_size = 1;
}

/* -------------------------------------------------------------------
 * Dispatcher contract: INVAL, NOSUPPORT, pass-through (C4)
 * ------------------------------------------------------------------- */

static void test_cluster_supported_predicates(void)
{
    struct mds_catalogue *cat;

    ASSERT_TRUE(!mds_cluster_supported(NULL));
    ASSERT_TRUE(!mds_cluster_stale_scan_supported(NULL));

    /* Every slot present AND the multi-process capability: supported. */
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_TRUE(mds_cluster_supported(cat));
    ASSERT_TRUE(mds_cluster_stale_scan_supported(cat));

    /* Slots present but an in-process store: never a cluster. */
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed, 0);
    ASSERT_TRUE(!mds_cluster_supported(cat));
    ASSERT_TRUE(mds_cluster_stale_scan_supported(cat));

    /* Missing partition map: not supported even with the capability;
     * the stale scan alone is still reported for what it is. */
    cat = make_fake_cat(&fake_ops_registry_only, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_TRUE(!mds_cluster_supported(cat));
    ASSERT_TRUE(mds_cluster_stale_scan_supported(cat));

    /* Missing stale scan only. */
    cat = make_fake_cat(&fake_ops_no_stale, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_TRUE(mds_cluster_supported(cat));
    ASSERT_TRUE(!mds_cluster_stale_scan_supported(cat));

    /* No cluster table at all. */
    cat = make_fake_cat(NULL, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_TRUE(!mds_cluster_supported(cat));
    ASSERT_TRUE(!mds_cluster_stale_scan_supported(cat));
}

static void test_cluster_dispatch_args_and_passthrough(void)
{
    struct mds_catalogue *cat;

    fake_reset();
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);

    /* Arguments are forwarded verbatim. */
    ASSERT_EQ(mds_cluster_node_register(cat, 7, 4242, "mds7.local",
                                        2049, 50057), MDS_OK);
    ASSERT_EQ(fake_register.calls, 1U);
    ASSERT_EQ(fake_register.mds_id, 7U);
    ASSERT_EQ(fake_register.boot_epoch, 4242U);
    ASSERT_STREQ(fake_register.hostname, "mds7.local");
    ASSERT_EQ(fake_register.nfs_port, 2049);
    ASSERT_EQ(fake_register.grpc_port, 50057);

    ASSERT_EQ(mds_cluster_node_deregister(cat, 7, 4242), MDS_OK);
    ASSERT_EQ(fake_deregister.calls, 1U);
    ASSERT_EQ(fake_deregister.mds_id, 7U);
    ASSERT_EQ(fake_deregister.boot_epoch, 4242U);

    /* C4: a slot's NOTFOUND reaches the caller unchanged. */
    fake_heartbeat_status = MDS_ERR_NOTFOUND;
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 7, 4242), MDS_ERR_NOTFOUND);
    ASSERT_EQ(fake_heartbeat.calls, 1U);
    ASSERT_EQ(fake_heartbeat.boot_epoch, 4242U);
    fake_heartbeat_status = MDS_ERR_STALE;
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 7, 4242), MDS_ERR_STALE);

    /* Argument validation precedes slot presence. */
    ASSERT_EQ(mds_cluster_node_register(NULL, 7, 1, "h", 1, 1),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_register(cat, 7, 1, NULL, 1, 1),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_heartbeat(NULL, 7, 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_deregister(NULL, 7, 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_list(cat, NULL, NULL), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_scan_stale(cat, 0, NULL, NULL),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_partition_list(cat, NULL, NULL), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_partition_put(cat, 0, 1, 0, NULL, false),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_partition_cas(NULL, 2, 2, 1,
                                        MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_INVAL);

    /* partition_cas forwards verbatim and its OK / NOTFOUND / STALE
     * reach the caller unchanged (C4). */
    fake_pm_add(2, 2, MDS_PARTITION_STATE_ACTIVE, "/shard2");
    ASSERT_EQ(mds_cluster_partition_cas(cat, 2, 2, 1,
                                        MDS_PARTITION_STATE_ACTIVE), MDS_OK);
    ASSERT_EQ(fake_pm_cas_count, 1U);
    ASSERT_EQ(fake_pm_cas_calls[0].partition_id, 2U);
    ASSERT_EQ(fake_pm_cas_calls[0].expected_owner, 2U);
    ASSERT_EQ(fake_pm_cas_calls[0].new_owner, 1U);
    ASSERT_EQ(fake_pm_cas_calls[0].new_state, MDS_PARTITION_STATE_ACTIVE);
    ASSERT_EQ(fake_pm_rows[0].owner, 1U);
    ASSERT_EQ(mds_cluster_partition_cas(cat, 2, 2, 1,
                                        MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_STALE);
    ASSERT_EQ(mds_cluster_partition_cas(cat, 5, 2, 1,
                                        MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_NOTFOUND);

    /* Absent slots -> NOSUPPORT. */
    cat = make_fake_cat(&fake_ops_registry_only, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(mds_cluster_partition_put(cat, 0, 1,
                                        MDS_PARTITION_STATE_ACTIVE, "/",
                                        false), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_partition_cas(cat, 2, 2, 1,
                                        MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_NOSUPPORT);
    cat = make_fake_cat(&fake_ops_no_stale, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(mds_cluster_node_scan_stale(cat, 1, NULL, NULL),
              MDS_ERR_INVAL);
    cat = make_fake_cat(NULL, &fake_lifecycle_no_feed, 0);
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 1, 1), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_node_deregister(cat, 1, 1), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_node_register(cat, 1, 1, "h", 1, 1),
              MDS_ERR_NOSUPPORT);
}

/* -------------------------------------------------------------------
 * subtree_map over the partition map
 * ------------------------------------------------------------------- */

static void test_subtree_init_loads_rows(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;
    struct subtree_entry e;

    fake_reset();
    fake_pm_add(0, 1, MDS_PARTITION_STATE_ACTIVE, "/");
    fake_pm_add(2, 2, MDS_PARTITION_STATE_ACTIVE, "/shard2");
    fake_pm_add(3, 3, MDS_PARTITION_STATE_FROZEN, "/shard3");
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);

    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 1, "mds1.local", &map),
              MDS_OK);
    ASSERT_TRUE(map != NULL);
    ASSERT_EQ(subtree_map_count(map), 3U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard2", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 2U);
    ASSERT_EQ((int)e.state, (int)SUBTREE_ACTIVE);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard3", &e), MDS_OK);
    ASSERT_EQ((int)e.state, (int)SUBTREE_FROZEN);
    ASSERT_TRUE(subtree_map_is_local(map, "/"));
    ASSERT_TRUE(!subtree_map_is_local(map, "/shard2/x"));
    /* Root was present: nothing was written back. */
    ASSERT_EQ(fake_pm_put_count, 0U);
    /* The hostname registered for referrals. */
    {
        char host[64];

        ASSERT_EQ(subtree_map_node_hostname(map, 1, host, sizeof(host)),
                  MDS_OK);
        ASSERT_STREQ(host, "mds1.local");
    }
    subtree_map_destroy(map);

    ASSERT_EQ(subtree_map_init_from_catalogue(NULL, 1, "h", &map),
              MDS_ERR_INVAL);
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 1, "h", NULL),
              MDS_ERR_INVAL);
}

/* Contract (mds_cluster.h): a failed partition_list at startup is
 * fatal after a bounded retry and never seeds root; the partition map
 * is the authority for root ownership and is never mutated on a read
 * failure.  The previous behaviour (empty map + root upsert) rewrote
 * the real root owner on a transient error. */
static void test_subtree_init_list_failure_is_fatal(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;

    fake_reset();
    fake_pm_add(2, 2, MDS_PARTITION_STATE_ACTIVE, "/shard2");
    fake_pm_list_status = MDS_ERR_IO;
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);

    /* Every attempt lists, none writes; the status passes through and
     * *out is untouched. */
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 7, "mds7.local", &map),
              MDS_ERR_IO);
    ASSERT_TRUE(map == NULL);
    ASSERT_EQ(fake_pm_list_calls, 3U);
    ASSERT_EQ(fake_pm_put_count, 0U);

    /* A store without a partition map is refused the same way. */
    fake_reset();
    fake_pm_list_status = MDS_ERR_NOSUPPORT;
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 7, NULL, &map),
              MDS_ERR_NOSUPPORT);
    ASSERT_TRUE(map == NULL);
    ASSERT_EQ(fake_pm_put_count, 0U);
}

/* Root claim: insert-only, exactly one node wins; EXISTS means another
 * node owns root and the map is reloaded to learn the owner; a
 * transient put failure never leaves this node owning root locally. */
static void test_subtree_init_root_claim_insert_only(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;
    struct subtree_entry e;

    /* Empty map: root is claimed with ONE insert-only put for self and
     * added locally only after the store accepted it. */
    fake_reset();
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 7, "mds7.local", &map),
              MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 1U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 7U);
    ASSERT_EQ((int)e.state, (int)SUBTREE_ACTIVE);
    ASSERT_EQ(fake_pm_list_calls, 1U);
    ASSERT_EQ(fake_pm_put_count, 1U);
    ASSERT_EQ(fake_pm_puts[0].partition_id, 0U);
    ASSERT_EQ(fake_pm_puts[0].owner, 7U);
    ASSERT_EQ(fake_pm_puts[0].state, MDS_PARTITION_STATE_ACTIVE);
    ASSERT_STREQ(fake_pm_puts[0].path, "/");
    ASSERT_TRUE(fake_pm_puts[0].insert_only);
    subtree_map_destroy(map);
    map = NULL;

    /* Lost the claim: node 9 inserted root between our list and our
     * put.  EXISTS is not an error -- the map is re-listed and root
     * comes back owned by 9, never by self. */
    fake_reset();
    fake_pm_add(0, 9, MDS_PARTITION_STATE_ACTIVE, "/");
    fake_pm_reveal_at_call = 2;
    fake_pm_put_status = MDS_ERR_EXISTS;
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 7, NULL, &map), MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 1U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 9U);
    ASSERT_TRUE(!subtree_map_is_local(map, "/"));
    ASSERT_EQ(fake_pm_list_calls, 2U);
    ASSERT_EQ(fake_pm_put_count, 1U);
    ASSERT_TRUE(fake_pm_puts[0].insert_only);
    subtree_map_destroy(map);
    map = NULL;

    /* In-doubt claim: the put reports a failure but the insert landed
     * (root owned by self appears on the re-list).  The retry converges
     * on the store's view; the claim is not repeated blindly. */
    fake_reset();
    fake_pm_add(0, 7, MDS_PARTITION_STATE_ACTIVE, "/");
    fake_pm_reveal_at_call = 2;
    fake_pm_put_status = MDS_ERR_IO;
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 7, NULL, &map), MDS_OK);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 7U);
    ASSERT_EQ(fake_pm_list_calls, 2U);
    ASSERT_EQ(fake_pm_put_count, 1U);
    subtree_map_destroy(map);
    map = NULL;

    /* A root claim that keeps failing is fatal: one insert-only put
     * per attempt, no local root, status passed through. */
    fake_reset();
    fake_pm_put_status = MDS_ERR_IO;
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 7, NULL, &map),
              MDS_ERR_IO);
    ASSERT_TRUE(map == NULL);
    ASSERT_EQ(fake_pm_list_calls, 3U);
    ASSERT_EQ(fake_pm_put_count, 3U);
    ASSERT_TRUE(fake_pm_puts[0].insert_only);
    ASSERT_TRUE(fake_pm_puts[2].insert_only);
}

static void test_subtree_refresh_from_catalogue(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;
    struct subtree_entry e;

    fake_reset();
    fake_pm_add(0, 1, MDS_PARTITION_STATE_ACTIVE, "/");
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 1, "mds1", &map), MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 1U);

    /* Another node changed root's owner and added a shard. */
    fake_pm_rows[0].owner = 3;
    fake_pm_add(2, 2, MDS_PARTITION_STATE_MIGRATING, "/shard2");
    ASSERT_EQ(subtree_map_refresh_from_catalogue(map, cat), MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 2U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 3U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard2", &e), MDS_OK);
    ASSERT_EQ((int)e.state, (int)SUBTREE_MIGRATING);

    /* A refresh failure changes nothing and passes the status through. */
    fake_pm_list_status = MDS_ERR_DELAY;
    ASSERT_EQ(subtree_map_refresh_from_catalogue(map, cat), MDS_ERR_DELAY);
    ASSERT_EQ(subtree_map_count(map), 2U);

    /* Refresh over a backend without a partition map: NOSUPPORT. */
    cat = make_fake_cat(&fake_ops_registry_only, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(subtree_map_refresh_from_catalogue(map, cat),
              MDS_ERR_NOSUPPORT);

    ASSERT_EQ(subtree_map_refresh_from_catalogue(NULL, cat), MDS_ERR_INVAL);
    ASSERT_EQ(subtree_map_refresh_from_catalogue(map, NULL), MDS_ERR_INVAL);
    subtree_map_destroy(map);
}

static void test_subtree_seed_shards(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;
    struct subtree_entry e;
    const char *peers[2] = { "mds1.local", "mds2.local" };
    char host[64];
    uint32_t i;

    fake_reset();
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &map),
              MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 1U);

    /* Single-node cluster: nothing to seed, nothing written. */
    ASSERT_EQ(subtree_map_seed_shards(map, cat, 1, peers, 2), MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 1U);
    ASSERT_EQ(fake_pm_put_count, 0U);

    /* Three nodes: /shard1../shard3 owned by 1..3, each persisted with
     * partition_id == mds_id as an upsert (insert_only == false). */
    ASSERT_EQ(subtree_map_seed_shards(map, cat, 3, peers, 2), MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 4U);
    ASSERT_EQ(fake_pm_put_count, 3U);
    for (i = 0; i < 3; i++) {
        char path[16];

        (void)snprintf(path, sizeof(path), "/shard%u", (unsigned)(i + 1));
        ASSERT_EQ(fake_pm_puts[i].partition_id, i + 1);
        ASSERT_EQ(fake_pm_puts[i].owner, i + 1);
        ASSERT_EQ(fake_pm_puts[i].state, MDS_PARTITION_STATE_ACTIVE);
        ASSERT_STREQ(fake_pm_puts[i].path, path);
        ASSERT_TRUE(!fake_pm_puts[i].insert_only);
        ASSERT_EQ(subtree_map_lookup_exact(map, path, &e), MDS_OK);
        ASSERT_EQ(e.owner_mds_id, i + 1);
    }
    /* Peer hostnames register referral nodes; the third has none. */
    ASSERT_EQ(subtree_map_node_hostname(map, 2, host, sizeof(host)), MDS_OK);
    ASSERT_STREQ(host, "mds2.local");
    ASSERT_EQ(subtree_map_node_hostname(map, 3, host, sizeof(host)),
              MDS_ERR_NOTFOUND);

    /* A map that already has shards is left alone (idempotent). */
    ASSERT_EQ(subtree_map_seed_shards(map, cat, 3, peers, 2), MDS_OK);
    ASSERT_EQ(fake_pm_put_count, 3U);

    ASSERT_EQ(subtree_map_seed_shards(NULL, cat, 3, NULL, 0), MDS_ERR_INVAL);
    ASSERT_EQ(subtree_map_seed_shards(map, NULL, 3, NULL, 0), MDS_ERR_INVAL);
    subtree_map_destroy(map);

    /* Persist failures keep the in-memory seed for this boot. */
    fake_reset();
    fake_pm_put_status = MDS_ERR_IO;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &map),
              MDS_OK);
    ASSERT_EQ(subtree_map_seed_shards(map, cat, 2, NULL, 0), MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 3U);
    ASSERT_EQ(fake_pm_put_count, 2U);
    subtree_map_destroy(map);
}

/* -------------------------------------------------------------------
 * cluster_membership over the node registry
 * ------------------------------------------------------------------- */

static void test_membership_populate(void)
{
    struct mds_catalogue *cat;
    struct mds_config cfg;
    struct subtree_map *map = NULL;
    struct cluster_membership *cm = NULL;
    struct cluster_member m;
    struct cluster_member joiner;
    char host[64];

    /* Self is a configured standby paired with node 2; the registry
     * (which carries addresses only) lists self and two peers. */
    fake_reset();
    fake_node_add(1, "mds1.local", 2049, 50051, 1000);
    fake_node_add(2, "mds2.local", 2050, 50052, 2000);
    fake_node_add(3, "mds3.local", 2051, 50053, 3000);
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    make_test_config(&cfg, 1, "mds1.local");
    cfg.self_role = (int)NODE_STANDBY;
    cfg.self_failover_partner_id = 2;
    (void)snprintf(cfg.cluster_bind_addr, sizeof(cfg.cluster_bind_addr),
                   "10.0.0.1");
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &map),
              MDS_OK);
    ASSERT_EQ(cluster_membership_init(&cfg, map, NULL, &cm), MDS_OK);
    ASSERT_EQ(cluster_membership_count(cm), 1U);
    ASSERT_EQ(cluster_membership_get(cm, 1, &m), MDS_OK);
    ASSERT_EQ((int)m.role, (int)NODE_STANDBY);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_IDLE);

    /* Node 3 joined through the transport as a standby of 1 before the
     * registry scan; its topology is local state too. */
    memset(&joiner, 0, sizeof(joiner));
    joiner.mds_id = 3;
    (void)snprintf(joiner.hostname, sizeof(joiner.hostname), "old3.local");
    joiner.nfs_port = 1;
    joiner.grpc_port = 1;
    joiner.role = NODE_STANDBY;
    joiner.lifecycle = NODE_IDLE;
    joiner.failover_partner_id = 1;
    joiner.wire_compat_version = 7;
    (void)snprintf(joiner.cluster_addr, sizeof(joiner.cluster_addr),
                   "10.0.0.3");
    ASSERT_EQ(cluster_node_join(cm, &joiner), MDS_OK);
    ASSERT_EQ(cluster_membership_count(cm), 2U);

    ASSERT_EQ(cluster_membership_populate(cm, cat), MDS_OK);
    ASSERT_EQ(cluster_membership_count(cm), 3U);

    /* Self: the registry row refreshes nothing it did not already
     * know; the configured role, lifecycle, partner, cluster address
     * and wire-compat version survive (main.c arms failover only when
     * self is still NODE_STANDBY here). */
    ASSERT_EQ(cluster_membership_get(cm, 1, &m), MDS_OK);
    ASSERT_EQ((int)m.role, (int)NODE_STANDBY);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_IDLE);
    ASSERT_EQ(m.failover_partner_id, 2U);
    ASSERT_EQ(m.wire_compat_version, (uint32_t)PNFS_MDS_WIRE_COMPAT_VERSION);
    ASSERT_STREQ(m.cluster_addr, "10.0.0.1");
    ASSERT_STREQ(m.hostname, "mds1.local");
    ASSERT_EQ(m.nfs_port, 2049);

    /* Transport-joined peer: address fields follow the registry, the
     * JOIN topology is kept. */
    ASSERT_EQ(cluster_membership_get(cm, 3, &m), MDS_OK);
    ASSERT_STREQ(m.hostname, "mds3.local");
    ASSERT_EQ(m.nfs_port, 2051);
    ASSERT_EQ(m.grpc_port, 50053);
    ASSERT_EQ((int)m.role, (int)NODE_STANDBY);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_IDLE);
    ASSERT_EQ(m.failover_partner_id, 1U);
    ASSERT_EQ(m.wire_compat_version, 7U);
    ASSERT_STREQ(m.cluster_addr, "10.0.0.3");

    /* Registry-only peer: inserted with the registry defaults and the
     * legacy wire-compat version 1 (never 0, which would fail the
     * promotion compat gate against self's real version). */
    ASSERT_EQ(cluster_membership_get(cm, 2, &m), MDS_OK);
    ASSERT_STREQ(m.hostname, "mds2.local");
    ASSERT_EQ(m.nfs_port, 2050);
    ASSERT_EQ(m.grpc_port, 50052);
    ASSERT_EQ((int)m.role, (int)NODE_ACTIVE);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_ACTIVE_SERVING);
    ASSERT_EQ(m.failover_partner_id, 0U);
    ASSERT_EQ(m.wire_compat_version, 1U);
    ASSERT_STREQ(m.cluster_addr, "");
    ASSERT_TRUE(m.join_time_sec != 0);
    /* Peer hostnames flow into the subtree map for referrals. */
    ASSERT_EQ(subtree_map_node_hostname(map, 3, host, sizeof(host)), MDS_OK);
    ASSERT_STREQ(host, "mds3.local");

    /* Re-populating is a merge, not a duplicate insert, and still does
     * not touch local state. */
    ASSERT_EQ(cluster_membership_populate(cm, cat), MDS_OK);
    ASSERT_EQ(cluster_membership_count(cm), 3U);
    ASSERT_EQ(cluster_membership_get(cm, 1, &m), MDS_OK);
    ASSERT_EQ((int)m.role, (int)NODE_STANDBY);
    ASSERT_EQ(m.failover_partner_id, 2U);

    /* Scan failure passes through unchanged (C4). */
    fake_node_list_status = MDS_ERR_IO;
    ASSERT_EQ(cluster_membership_populate(cm, cat), MDS_ERR_IO);
    fake_node_list_status = MDS_OK;

    /* No registry slot -> NOSUPPORT; NULL arguments -> INVAL. */
    cat = make_fake_cat(NULL, &fake_lifecycle_no_feed, 0);
    ASSERT_EQ(cluster_membership_populate(cm, cat), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(cluster_membership_populate(NULL, cat), MDS_ERR_INVAL);
    ASSERT_EQ(cluster_membership_populate(cm, NULL), MDS_ERR_INVAL);

    cluster_membership_destroy(cm);
    subtree_map_destroy(map);
}

/* -------------------------------------------------------------------
 * failover watchdog: refuses without the stale scan, runs with it
 * ------------------------------------------------------------------- */

static void test_watchdog_start_contract(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;
    struct failover_ctx *fo = NULL;
    struct failover_watchdog *wd = (struct failover_watchdog *)&fake_feed;
    struct failover_cfg fo_cfg;
    struct failover_watchdog_cfg wd_cfg;

    fake_reset();
    cat = make_fake_cat(&fake_ops_no_stale, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(subtree_map_init(NULL, NULL, 2, "standby", NULL, &map), MDS_OK);
    memset(&fo_cfg, 0, sizeof(fo_cfg));
    fo_cfg.self_id = 2;
    fo_cfg.partner_id = 1;
    fo_cfg.map = map;
    fo_cfg.cat = cat;
    ASSERT_EQ(failover_init(&fo_cfg, &fo), MDS_OK);

    memset(&wd_cfg, 0, sizeof(wd_cfg));
    wd_cfg.fo = fo;
    wd_cfg.cat = cat;
    wd_cfg.partner_id = 1;

    /* No node_scan_stale slot: nothing starts, *out is cleared. */
    ASSERT_EQ(failover_watchdog_start(&wd_cfg, &wd), MDS_ERR_NOSUPPORT);
    ASSERT_TRUE(wd == NULL);

    /* Argument validation comes first and is independent of slots. */
    ASSERT_EQ(failover_watchdog_start(NULL, &wd), MDS_ERR_INVAL);
    ASSERT_EQ(failover_watchdog_start(&wd_cfg, NULL), MDS_ERR_INVAL);
    wd_cfg.partner_id = 0;
    ASSERT_EQ(failover_watchdog_start(&wd_cfg, &wd), MDS_ERR_INVAL);
    wd_cfg.partner_id = 1;
    wd_cfg.cat = NULL;
    ASSERT_EQ(failover_watchdog_start(&wd_cfg, &wd), MDS_ERR_INVAL);
    wd_cfg.cat = cat;
    wd_cfg.fo = NULL;
    ASSERT_EQ(failover_watchdog_start(&wd_cfg, &wd), MDS_ERR_INVAL);
    wd_cfg.fo = fo;

    /* With the slot the watchdog starts; a short poll interval and a
     * long observation grace keep the thread from promoting anything
     * before it is stopped and joined. */
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    wd_cfg.cat = cat;
    wd_cfg.poll_interval_ms = 10;
    wd_cfg.min_observe_ms = 60000;
    ASSERT_EQ(failover_watchdog_start(&wd_cfg, &wd), MDS_OK);
    ASSERT_TRUE(wd != NULL);
    failover_watchdog_stop(wd);
    failover_watchdog_stop(NULL);
    ASSERT_EQ((int)failover_get_role(fo), (int)FAILOVER_STANDBY);

    failover_destroy(fo);
    subtree_map_destroy(map);
}

/* -------------------------------------------------------------------
 * Heartbeat clock domain: the plausibility rule
 * ------------------------------------------------------------------- */

static void test_heartbeat_plausibility_predicate(void)
{
    /* The floor is 2020-01-01T00:00:00Z in nanoseconds. */
    ASSERT_EQ(FAILOVER_HB_REALTIME_FLOOR_NS,
              1577836800ULL * 1000000000ULL);

    /* Anything a CLOCK_MONOTONIC writer can produce is implausible:
     * 0, one second, 30 days and 10 years of uptime. */
    ASSERT_TRUE(!failover_heartbeat_plausible(0));
    ASSERT_TRUE(!failover_heartbeat_plausible(1000000000ULL));
    ASSERT_TRUE(!failover_heartbeat_plausible(30ULL * 86400ULL *
                                              1000000000ULL));
    ASSERT_TRUE(!failover_heartbeat_plausible(10ULL * 366ULL * 86400ULL *
                                              1000000000ULL));
    ASSERT_TRUE(!failover_heartbeat_plausible(
                    FAILOVER_HB_REALTIME_FLOOR_NS - 1));

    /* The floor itself and every later realtime stamp are plausible. */
    ASSERT_TRUE(failover_heartbeat_plausible(FAILOVER_HB_REALTIME_FLOOR_NS));
    ASSERT_TRUE(failover_heartbeat_plausible(
                    FAILOVER_HB_REALTIME_FLOOR_NS + 1));
    ASSERT_TRUE(failover_heartbeat_plausible(realtime_now_ns()));
    ASSERT_TRUE(failover_heartbeat_plausible(UINT64_MAX));

    /* The startup budget sits below the default stale threshold. */
    ASSERT_TRUE(CLUSTER_STARTUP_DEADLINE_MS <
                FAILOVER_WATCHDOG_STALE_TIMEOUT_MS_DEFAULT);
}

/* detect_cb hook: failover_promote consults it before any state
 * change, so counting its calls observes "promotion attempted" without
 * letting a promotion happen (0 = partner alive -> MDS_ERR_PERM). */
static _Atomic int detect_calls;

static int fake_detect_partner_alive(uint32_t partner_id, void *arg)
{
    (void)partner_id;
    (void)arg;
    atomic_fetch_add(&detect_calls, 1);
    return 0;
}

/* Run the watchdog against the current fake registry for @run_ms with
 * a 10 ms poll and no boot-up grace; returns the promotion attempts. */
static int watchdog_attempts_over(struct failover_ctx *fo,
                                  struct mds_catalogue *cat,
                                  unsigned run_ms)
{
    struct failover_watchdog *wd = NULL;
    struct failover_watchdog_cfg wd_cfg;

    memset(&wd_cfg, 0, sizeof(wd_cfg));
    wd_cfg.fo = fo;
    wd_cfg.cat = cat;
    wd_cfg.partner_id = 1;
    wd_cfg.poll_interval_ms = 10;
    wd_cfg.min_observe_ms = 1;
    atomic_store(&detect_calls, 0);
    if (failover_watchdog_start(&wd_cfg, &wd) != MDS_OK) {
        return -1;
    }
    sleep_ms(run_ms);
    failover_watchdog_stop(wd);
    return atomic_load(&detect_calls);
}

/* A partner row below the realtime floor is indeterminate: the store
 * reports it as stale (it is below any threshold) but the watchdog
 * must skip the tick and never attempt a promotion.  A plausible stale
 * row does trigger the attempt; a fresh one does not. */
static void test_watchdog_indeterminate_partner_skips_tick(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;
    struct failover_ctx *fo = NULL;
    struct failover_cfg fo_cfg;
    int attempts;

    fake_reset();
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(subtree_map_init(NULL, NULL, 2, "standby", NULL, &map), MDS_OK);
    memset(&fo_cfg, 0, sizeof(fo_cfg));
    fo_cfg.self_id = 2;
    fo_cfg.partner_id = 1;
    fo_cfg.map = map;
    fo_cfg.cat = cat;
    fo_cfg.detect_cb = fake_detect_partner_alive;
    ASSERT_EQ(failover_init(&fo_cfg, &fo), MDS_OK);

    /* Partner stamped 5 s of host uptime (pre-upgrade writer): many
     * ticks, zero attempts. */
    fake_node_add(1, "primary", 2049, 50051, 5ULL * 1000000000ULL);
    attempts = watchdog_attempts_over(fo, cat, 150);
    ASSERT_EQ(attempts, 0);
    ASSERT_EQ((int)failover_get_role(fo), (int)FAILOVER_STANDBY);

    /* The same partner with a realtime stamp a minute old IS stale:
     * promotion is attempted (and refused by detect_cb). */
    fake_node_rows[0].last_heartbeat_ns =
        realtime_now_ns() - 60ULL * 1000000000ULL;
    attempts = watchdog_attempts_over(fo, cat, 150);
    ASSERT_TRUE(attempts >= 1);
    ASSERT_EQ((int)failover_get_role(fo), (int)FAILOVER_STANDBY);

    /* A fresh realtime stamp is below no threshold: no attempt. */
    fake_node_rows[0].last_heartbeat_ns = realtime_now_ns();
    attempts = watchdog_attempts_over(fo, cat, 150);
    ASSERT_EQ(attempts, 0);
    ASSERT_EQ((int)failover_get_role(fo), (int)FAILOVER_STANDBY);

    /* An implausible row of ANOTHER node never affects the partner. */
    fake_node_add(3, "other", 2049, 50053, 1);
    attempts = watchdog_attempts_over(fo, cat, 150);
    ASSERT_EQ(attempts, 0);

    failover_destroy(fo);
    subtree_map_destroy(map);
}

/* -------------------------------------------------------------------
 * Image feed lifecycle dispatchers
 * ------------------------------------------------------------------- */

static void test_image_feed_dispatch(void)
{
    struct mds_catalogue *cat;
    struct catalog_image *img = NULL;

    ASSERT_EQ(catalog_image_create(&img), 0);
    memset(&fake_feed, 0, sizeof(fake_feed));
    fake_feed_start_status = MDS_OK;

    /* NULL slots -> NOSUPPORT, not supported. */
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed, 0);
    ASSERT_TRUE(!mds_catalogue_image_feed_supported(cat));
    ASSERT_EQ(mds_catalogue_image_feed_start(cat, img, 1, 50),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_catalogue_image_feed_stop(cat), MDS_ERR_NOSUPPORT);
    ASSERT_TRUE(!mds_catalogue_image_feed_supported(NULL));
    ASSERT_EQ(mds_catalogue_image_feed_start(NULL, img, 1, 50),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_catalogue_image_feed_stop(NULL), MDS_ERR_INVAL);

    /* Half a pair is not "supported" (both slots or neither). */
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_start_only, 0);
    ASSERT_TRUE(!mds_catalogue_image_feed_supported(cat));
    ASSERT_EQ(mds_catalogue_image_feed_stop(cat), MDS_ERR_NOSUPPORT);

    /* Both slots: arguments forwarded, status passed through. */
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_with_feed, 0);
    ASSERT_TRUE(mds_catalogue_image_feed_supported(cat));
    ASSERT_EQ(mds_catalogue_image_feed_start(cat, NULL, 1, 50),
              MDS_ERR_INVAL);
    ASSERT_EQ(fake_feed.start_calls, 0U);
    ASSERT_EQ(mds_catalogue_image_feed_start(cat, img, 9, 75), MDS_OK);
    ASSERT_EQ(fake_feed.start_calls, 1U);
    ASSERT_TRUE(fake_feed.cat == cat);
    ASSERT_TRUE(fake_feed.image == img);
    ASSERT_EQ(fake_feed.self_mds_id, 9U);
    ASSERT_EQ(fake_feed.poll_interval_ms, 75U);
    fake_feed_start_status = MDS_ERR_IO;
    ASSERT_EQ(mds_catalogue_image_feed_start(cat, img, 9, 75), MDS_ERR_IO);
    ASSERT_EQ(mds_catalogue_image_feed_stop(cat), MDS_OK);
    ASSERT_EQ(fake_feed.stop_calls, 1U);

    catalog_image_destroy(img);
}

/* -------------------------------------------------------------------
 * stderr capture (bounded) for message assertions
 * ------------------------------------------------------------------- */

struct stderr_capture {
    int  saved_fd;
    int  fd;
    char path[128];
};

/* Redirect fd 2 into a temp file; returns 0 on success. */
static int stderr_capture_begin(struct stderr_capture *c)
{
    (void)snprintf(c->path, sizeof(c->path),
                   "/tmp/pnfs-cluster-extract-%d.err", (int)getpid());
    (void)fflush(stderr);
    c->saved_fd = dup(STDERR_FILENO);
    c->fd = open(c->path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (c->saved_fd < 0 || c->fd < 0) {
        if (c->saved_fd >= 0) {
            (void)close(c->saved_fd);
        }
        if (c->fd >= 0) {
            (void)close(c->fd);
        }
        return -1;
    }
    (void)dup2(c->fd, STDERR_FILENO);
    return 0;
}

/* Restore fd 2 and copy what was captured (NUL-terminated) into out. */
static void stderr_capture_end(struct stderr_capture *c,
                               char *out, size_t cap)
{
    FILE *ef;
    size_t nread;

    out[0] = '\0';
    (void)fflush(stderr);
    (void)dup2(c->saved_fd, STDERR_FILENO);
    (void)close(c->saved_fd);
    (void)close(c->fd);
    ef = fopen(c->path, "r");
    if (ef != NULL) {
        nread = fread(out, 1, cap - 1, ef);
        out[nread] = '\0';
        (void)fclose(ef);
    }
    (void)unlink(c->path);
}

/* -------------------------------------------------------------------
 * Backend name table (pnfs_common) and registry (core)
 * ------------------------------------------------------------------- */

static bool rondb_compiled_in(void)
{
#ifdef HAVE_RONDB
    return true;
#else
    return false;
#endif
}

static bool fdb_compiled_in(void)
{
#ifdef HAVE_FDB
    return true;
#else
    return false;
#endif
}

static void test_backend_names(void)
{
    enum mds_catalogue_backend be = MDS_BACKEND_NONE;
    enum mds_catalogue_backend two[2] = { MDS_BACKEND_FDB, MDS_BACKEND_RONDB };
    char names[128];

    ASSERT_EQ(mds_catalogue_backend_from_name("rondb", &be), MDS_OK);
    ASSERT_EQ((int)be, (int)MDS_BACKEND_RONDB);
    ASSERT_EQ(mds_catalogue_backend_from_name("memdb", &be), MDS_OK);
    ASSERT_EQ((int)be, (int)MDS_BACKEND_MEMDB);
    ASSERT_EQ(mds_catalogue_backend_from_name("fdb", &be), MDS_OK);
    ASSERT_EQ((int)be, (int)MDS_BACKEND_FDB);

    /* Unknown names leave *out untouched; the lookup is exact. */
    be = MDS_BACKEND_NONE;
    ASSERT_EQ(mds_catalogue_backend_from_name("bogus", &be), MDS_ERR_INVAL);
    ASSERT_EQ(mds_catalogue_backend_from_name("RonDB", &be), MDS_ERR_INVAL);
    ASSERT_EQ(mds_catalogue_backend_from_name("", &be), MDS_ERR_INVAL);
    ASSERT_EQ((int)be, (int)MDS_BACKEND_NONE);
    ASSERT_EQ(mds_catalogue_backend_from_name(NULL, &be), MDS_ERR_INVAL);
    ASSERT_EQ(mds_catalogue_backend_from_name("rondb", NULL), MDS_ERR_INVAL);

    /* enum -> name round trip; the sentinel and garbage have none. */
    ASSERT_STREQ(mds_catalogue_backend_name(MDS_BACKEND_RONDB), "rondb");
    ASSERT_STREQ(mds_catalogue_backend_name(MDS_BACKEND_MEMDB), "memdb");
    ASSERT_STREQ(mds_catalogue_backend_name(MDS_BACKEND_FDB), "fdb");
    ASSERT_TRUE(mds_catalogue_backend_name(MDS_BACKEND_NONE) == NULL);
    ASSERT_TRUE(mds_catalogue_backend_name((enum mds_catalogue_backend)77)
                == NULL);

    /* Every known name, in table order, independent of the build. */
    ASSERT_EQ(mds_catalogue_backend_known_names(names, sizeof(names)), 3U);
    ASSERT_STREQ(names, "rondb, memdb, fdb");

    /* join: order preserved, empty set spelled out, bad ids marked. */
    ASSERT_EQ(mds_catalogue_backend_join_names(two, 2, names, sizeof(names)),
              2U);
    ASSERT_STREQ(names, "fdb, rondb");
    ASSERT_EQ(mds_catalogue_backend_join_names(two, 0, names, sizeof(names)),
              0U);
    ASSERT_STREQ(names, "(none)");
    two[0] = (enum mds_catalogue_backend)77;
    ASSERT_EQ(mds_catalogue_backend_join_names(two, 1, names, sizeof(names)),
              1U);
    ASSERT_STREQ(names, "?");
    /* Truncation stays inside the buffer and keeps the count. */
    memset(names, 'X', sizeof(names));
    ASSERT_EQ(mds_catalogue_backend_known_names(names, 4), 3U);
    ASSERT_TRUE(strlen(names) < 4);
    ASSERT_EQ(mds_catalogue_backend_known_names(NULL, 8), 0U);
    ASSERT_EQ(mds_catalogue_backend_known_names(names, 0), 0U);
}

static void test_backend_registry(void)
{
    char names[128];
    size_t n;
    size_t expect = 0;

    /* NONE and garbage are never available. */
    ASSERT_TRUE(!mds_catalogue_backend_available(MDS_BACKEND_NONE));
    ASSERT_TRUE(!mds_catalogue_backend_available(
                    (enum mds_catalogue_backend)77));
    /* rondb / fdb availability follows the build flavour exactly. */
    ASSERT_EQ((int)mds_catalogue_backend_available(MDS_BACKEND_RONDB),
              (int)rondb_compiled_in());
    ASSERT_EQ((int)mds_catalogue_backend_available(MDS_BACKEND_FDB),
              (int)fdb_compiled_in());

    /* The name list agrees with the availability predicate. */
    if (mds_catalogue_backend_available(MDS_BACKEND_RONDB)) {
        expect++;
    }
    if (mds_catalogue_backend_available(MDS_BACKEND_MEMDB)) {
        expect++;
    }
    if (mds_catalogue_backend_available(MDS_BACKEND_FDB)) {
        expect++;
    }
    n = mds_catalogue_backend_available_names(names, sizeof(names));
    ASSERT_EQ(n, expect);
    if (n == 0) {
        ASSERT_STREQ(names, "(none)");
    } else {
        ASSERT_TRUE(names[0] != '\0');
        ASSERT_EQ((int)(strstr(names, "rondb") != NULL),
                  (int)mds_catalogue_backend_available(MDS_BACKEND_RONDB));
        ASSERT_EQ((int)(strstr(names, "memdb") != NULL),
                  (int)mds_catalogue_backend_available(MDS_BACKEND_MEMDB));
        ASSERT_EQ((int)(strstr(names, "fdb") != NULL),
                  (int)mds_catalogue_backend_available(MDS_BACKEND_FDB));
    }
    ASSERT_EQ(mds_catalogue_backend_available_names(NULL, 8), 0U);
    ASSERT_EQ(mds_catalogue_backend_available_names(names, 0), 0U);
}

/* Opening a backend the binary cannot construct fails with the same
 * status the old hard-coded factory used (MDS_ERR_INVAL), never
 * touches *out, and logs the backend name plus the available list. */
static void test_open_unavailable_backend(void)
{
    struct mds_config cfg;
    struct mds_catalogue *cat = (struct mds_catalogue *)&fake_feed;
    struct stderr_capture cap;
    char err[512];

    memset(&cfg, 0, sizeof(cfg));

    /* Known; refused by name when this build lacks it. */
    if (!fdb_compiled_in()) {
        cfg.catalogue_backend = MDS_BACKEND_FDB;
        ASSERT_EQ(stderr_capture_begin(&cap), 0);
        ASSERT_EQ(mds_catalogue_open(&cfg, &cat), MDS_ERR_INVAL);
        stderr_capture_end(&cap, err, sizeof(err));
        ASSERT_TRUE(cat == (struct mds_catalogue *)&fake_feed);
        ASSERT_TRUE(strstr(err, "catalogue_backend fdb not compiled in")
                    != NULL);
        ASSERT_TRUE(strstr(err, "available:") != NULL);
    }

    /* No backend configured (the no-RonDB default). */
    cfg.catalogue_backend = MDS_BACKEND_NONE;
    ASSERT_EQ(stderr_capture_begin(&cap), 0);
    ASSERT_EQ(mds_catalogue_open(&cfg, &cat), MDS_ERR_INVAL);
    stderr_capture_end(&cap, err, sizeof(err));
    ASSERT_TRUE(strstr(err, "catalogue_backend not set") != NULL);
    ASSERT_TRUE(strstr(err, "available:") != NULL);

    /* Garbage id and NULL arguments. */
    cfg.catalogue_backend = (enum mds_catalogue_backend)77;
    ASSERT_EQ(stderr_capture_begin(&cap), 0);
    ASSERT_EQ(mds_catalogue_open(&cfg, &cat), MDS_ERR_INVAL);
    stderr_capture_end(&cap, err, sizeof(err));
    ASSERT_TRUE(strstr(err, "unknown catalogue_backend 77") != NULL);
    ASSERT_EQ(mds_catalogue_open(NULL, &cat), MDS_ERR_INVAL);
    ASSERT_EQ(mds_catalogue_open(&cfg, NULL), MDS_ERR_INVAL);

    /* memdb / rondb: refused by name when this build lacks them. */
    if (!mds_catalogue_backend_available(MDS_BACKEND_MEMDB)) {
        cfg.catalogue_backend = MDS_BACKEND_MEMDB;
        ASSERT_EQ(stderr_capture_begin(&cap), 0);
        ASSERT_EQ(mds_catalogue_open(&cfg, &cat), MDS_ERR_INVAL);
        stderr_capture_end(&cap, err, sizeof(err));
        ASSERT_TRUE(strstr(err, "catalogue_backend memdb not compiled in")
                    != NULL);
    }
    if (!mds_catalogue_backend_available(MDS_BACKEND_RONDB)) {
        cfg.catalogue_backend = MDS_BACKEND_RONDB;
        ASSERT_EQ(stderr_capture_begin(&cap), 0);
        ASSERT_EQ(mds_catalogue_open(&cfg, &cat), MDS_ERR_INVAL);
        stderr_capture_end(&cap, err, sizeof(err));
        ASSERT_TRUE(strstr(err, "catalogue_backend rondb not compiled in")
                    != NULL);
    }
    ASSERT_TRUE(cat == (struct mds_catalogue *)&fake_feed);
}

/* -------------------------------------------------------------------
 * Failover takeover over the partition map (mds_cluster_partition_cas)
 * ------------------------------------------------------------------- */

/* Every partner-owned row is CAS'd with expected = partner before its
 * in-memory entry moves; a row the store refuses (another node took
 * it) is skipped and stays the partner's in memory; a refresh then
 * agrees with the store; a replay finds nothing to take. */
static void test_failover_take_over_persists_with_cas(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;
    struct subtree_entry e;
    uint32_t taken = 99;

    fake_reset();
    fake_pm_add(0, 0, MDS_PARTITION_STATE_ACTIVE, "/");   /* seeded root */
    fake_pm_add(1, 1, MDS_PARTITION_STATE_ACTIVE, "/shard1");
    fake_pm_add(2, 2, MDS_PARTITION_STATE_ACTIVE, "/shard2");
    fake_pm_add(3, 2, MDS_PARTITION_STATE_ACTIVE, "/shard3");
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 1, "mds1.local", &map),
              MDS_OK);
    ASSERT_EQ(subtree_map_count(map), 4U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard3", &e), MDS_OK);
    ASSERT_TRUE(e.pm_backed);
    ASSERT_EQ(e.partition_id, 3U);

    /* Node 9 took /shard3 behind our back: our CAS on it is STALE. */
    fake_pm_rows[3].owner = 9;

    ASSERT_EQ(subtree_map_failover_take_over(map, cat, 2, 1, &taken), MDS_OK);
    ASSERT_EQ(taken, 1U);
    ASSERT_EQ(fake_pm_cas_count, 2U);
    ASSERT_EQ(fake_pm_cas_calls[0].partition_id, 2U);
    ASSERT_EQ(fake_pm_cas_calls[0].expected_owner, 2U);
    ASSERT_EQ(fake_pm_cas_calls[0].new_owner, 1U);
    ASSERT_EQ(fake_pm_cas_calls[0].new_state, MDS_PARTITION_STATE_ACTIVE);
    ASSERT_EQ(fake_pm_cas_calls[1].partition_id, 3U);
    ASSERT_EQ(fake_pm_cas_calls[1].expected_owner, 2U);
    /* Store: /shard2 rewritten, /shard3 untouched. */
    ASSERT_EQ(fake_pm_rows[2].owner, 1U);
    ASSERT_EQ(fake_pm_rows[3].owner, 9U);
    /* Memory: /shard2 moved; /shard3 was NOT flipped -- still what the
     * last load said (the partner), never a claim the store refused. */
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard2", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 1U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard3", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 2U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard1", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 1U);

    /* The refresh agrees with the store on both. */
    ASSERT_EQ(subtree_map_refresh_from_catalogue(map, cat), MDS_OK);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard2", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 1U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard3", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 9U);

    /* Replay: nothing of the partner's is left; no CAS is issued. */
    fake_pm_cas_count = 0;
    taken = 99;
    ASSERT_EQ(subtree_map_failover_take_over(map, cat, 2, 1, &taken), MDS_OK);
    ASSERT_EQ(taken, 0U);
    ASSERT_EQ(fake_pm_cas_count, 0U);

    /* Rollback direction (failover_promote after a later phase fails):
     * the same transfer with expected = self hands the row back, and
     * a second attempt is STALE because self no longer owns it. */
    ASSERT_EQ(subtree_map_failover_transfer(map, cat, "/shard2", 1, 2),
              MDS_OK);
    ASSERT_EQ(fake_pm_rows[2].owner, 2U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard2", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 2U);
    ASSERT_EQ(subtree_map_failover_transfer(map, cat, "/shard2", 1, 2),
              MDS_ERR_STALE);
    ASSERT_EQ(subtree_map_failover_transfer(map, cat, "/nowhere", 2, 1),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(subtree_map_failover_transfer(NULL, cat, "/shard2", 2, 1),
              MDS_ERR_INVAL);
    subtree_map_destroy(map);
    map = NULL;

    /* Store unreachable: nothing moves in memory and IO is reported,
     * so the promotion aborts instead of serving from a map the store
     * never accepted. */
    fake_reset();
    fake_pm_add(0, 0, MDS_PARTITION_STATE_ACTIVE, "/");
    fake_pm_add(2, 2, MDS_PARTITION_STATE_ACTIVE, "/shard2");
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 1, NULL, &map), MDS_OK);
    fake_pm_cas_status = MDS_ERR_IO;
    taken = 99;
    ASSERT_EQ(subtree_map_failover_take_over(map, cat, 2, 1, &taken),
              MDS_ERR_IO);
    ASSERT_EQ(taken, 0U);
    ASSERT_EQ(fake_pm_cas_count, 1U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard2", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 2U);
    ASSERT_EQ(fake_pm_rows[1].owner, 2U);
    subtree_map_destroy(map);
    map = NULL;

    /* Memory-only entries (subtree_map_add: local mode, splits) and a
     * NULL catalogue never reach the store. */
    fake_reset();
    fake_pm_add(0, 0, MDS_PARTITION_STATE_ACTIVE, "/");
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 1, NULL, &map), MDS_OK);
    ASSERT_EQ(subtree_map_add(map, "/local", 2, NULL, SUBTREE_ACTIVE, 1),
              MDS_OK);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/local", &e), MDS_OK);
    ASSERT_TRUE(!e.pm_backed);
    taken = 99;
    ASSERT_EQ(subtree_map_failover_take_over(map, cat, 2, 1, &taken), MDS_OK);
    ASSERT_EQ(taken, 1U);
    ASSERT_EQ(fake_pm_cas_count, 0U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/local", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 1U);
    ASSERT_EQ(subtree_map_failover_transfer(map, NULL, "/local", 1, 2),
              MDS_OK);
    ASSERT_EQ(fake_pm_cas_count, 0U);
    subtree_map_destroy(map);
}

/* A store that lists and puts but has no CAS keeps the pre-slot
 * behaviour: the takeover moves the entries in memory only (and the
 * store keeps the partner as owner, which is exactly the D3 exposure
 * the slot closes). */
static void test_failover_take_over_without_cas_slot(void)
{
    struct mds_catalogue *cat;
    struct subtree_map *map = NULL;
    struct subtree_entry e;
    uint32_t taken = 99;

    fake_reset();
    fake_pm_add(0, 0, MDS_PARTITION_STATE_ACTIVE, "/");
    fake_pm_add(2, 2, MDS_PARTITION_STATE_ACTIVE, "/shard2");
    cat = make_fake_cat(&fake_ops_no_cas, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_TRUE(mds_cluster_supported(cat));
    ASSERT_EQ(subtree_map_init_from_catalogue(cat, 1, NULL, &map), MDS_OK);

    ASSERT_EQ(subtree_map_failover_take_over(map, cat, 2, 1, &taken), MDS_OK);
    ASSERT_EQ(taken, 1U);
    ASSERT_EQ(fake_pm_cas_count, 0U);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/shard2", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, 1U);
    ASSERT_EQ(fake_pm_rows[1].owner, 2U);
    subtree_map_destroy(map);
}

/* -------------------------------------------------------------------
 * Writer-side heartbeat tick: the self-fencing decision main.c acts on
 * ------------------------------------------------------------------- */

static void test_heartbeat_tick_supersession(void)
{
    struct mds_catalogue *cat;
    uint64_t superseder = 77;

    fake_reset();
    cat = make_fake_cat(&fake_ops_full, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);

    /* OK / NOTFOUND / NOSUPPORT pass through; no read-back, epoch 0. */
    fake_heartbeat_status = MDS_OK;
    ASSERT_EQ(cluster_heartbeat_tick(cat, 3, 4242, &superseder), MDS_OK);
    ASSERT_EQ(fake_heartbeat.calls, 1U);
    ASSERT_EQ(fake_heartbeat.mds_id, 3U);
    ASSERT_EQ(fake_heartbeat.boot_epoch, 4242U);
    ASSERT_EQ(superseder, 0U);
    fake_heartbeat_status = MDS_ERR_NOTFOUND;
    superseder = 77;
    ASSERT_EQ(cluster_heartbeat_tick(cat, 3, 4242, &superseder),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(superseder, 0U);

    /* STALE: the row now carries the newer incarnation's epoch
     * (fake_node_add stamps 100 + mds_id), which is named. */
    fake_heartbeat_status = MDS_ERR_STALE;
    fake_node_add(3, "mds3", 2049, 50051, 1);
    ASSERT_EQ(cluster_heartbeat_tick(cat, 3, 4242, &superseder),
              MDS_ERR_STALE);
    ASSERT_EQ(superseder, 103U);

    /* STALE with an unreadable registry: still STALE, epoch 0. */
    fake_node_list_status = MDS_ERR_IO;
    superseder = 77;
    ASSERT_EQ(cluster_heartbeat_tick(cat, 3, 4242, &superseder),
              MDS_ERR_STALE);
    ASSERT_EQ(superseder, 0U);
    fake_node_list_status = MDS_OK;

    /* A NULL epoch out-pointer is allowed; NULL handle is INVAL. */
    ASSERT_EQ(cluster_heartbeat_tick(cat, 3, 4242, NULL), MDS_ERR_STALE);
    ASSERT_EQ(cluster_heartbeat_tick(NULL, 3, 4242, &superseder),
              MDS_ERR_INVAL);

    /* No registry at all: NOSUPPORT passes through. */
    cat = make_fake_cat(NULL, &fake_lifecycle_no_feed,
                        MDS_CAT_CAP_MULTI_PROCESS);
    ASSERT_EQ(cluster_heartbeat_tick(cat, 3, 4242, &superseder),
              MDS_ERR_NOSUPPORT);
}

/* -------------------------------------------------------------------
 * Config parsing of catalogue_backend in both build flavours
 * ------------------------------------------------------------------- */

static int write_tmp_ini(const char *content, char path_out[128])
{
    FILE *f;

    (void)snprintf(path_out, 128, "/tmp/pnfs-cluster-extract-%d.conf",
                   (int)getpid());
    f = fopen(path_out, "w");
    if (f == NULL) {
        return -1;
    }
    (void)fputs(content, f);
    (void)fclose(f);
    return 0;
}

/* Parse @ini with stderr captured into @err_out (bounded). */
static enum mds_status load_capturing_stderr(const char *ini,
                                             struct mds_config *cfg,
                                             char *err_out, size_t err_cap)
{
    char cfg_path[128];
    struct stderr_capture cap;
    enum mds_status st;

    err_out[0] = '\0';
    if (write_tmp_ini(ini, cfg_path) != 0) {
        return MDS_ERR_IO;
    }
    if (stderr_capture_begin(&cap) != 0) {
        (void)unlink(cfg_path);
        return MDS_ERR_IO;
    }
    st = mds_config_load(cfg_path, cfg);
    stderr_capture_end(&cap, err_out, err_cap);
    (void)unlink(cfg_path);
    return st;
}

/* The parser knows names, not builds: every KNOWN name parses in every
 * build flavour (mds_catalogue_open refuses the unavailable ones, see
 * test_open_unavailable_backend); only an unknown name is a parse
 * error, and it lists the known names. */
static void test_config_backend_parsing(void)
{
    struct mds_config cfg;
    char err[512];
    enum mds_status st;

    /* Missing key: rondb when compiled in, otherwise no backend at all
     * (never a silent fallback to whatever else is built). */
    st = load_capturing_stderr("mds_id = 3\n", &cfg, err, sizeof(err));
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)cfg.catalogue_backend,
              rondb_compiled_in() ? (int)MDS_BACKEND_RONDB
                                  : (int)MDS_BACKEND_NONE);
    ASSERT_EQ(cfg.self.id, 3U);

    /* Known names parse regardless of availability. */
    st = load_capturing_stderr("catalogue_backend = fdb\n", &cfg,
                               err, sizeof(err));
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)cfg.catalogue_backend, (int)MDS_BACKEND_FDB);
    st = load_capturing_stderr("catalogue_backend = memdb\n", &cfg,
                               err, sizeof(err));
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)cfg.catalogue_backend, (int)MDS_BACKEND_MEMDB);
    st = load_capturing_stderr("catalogue_backend = rondb\n", &cfg,
                               err, sizeof(err));
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)cfg.catalogue_backend, (int)MDS_BACKEND_RONDB);
    /* Last key wins, like every other key. */
    st = load_capturing_stderr("catalogue_backend = fdb\n"
                               "catalogue_backend = memdb\n", &cfg,
                               err, sizeof(err));
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)cfg.catalogue_backend, (int)MDS_BACKEND_MEMDB);

    /* Unknown name: parse error naming it and listing the known names
     * (not the available ones -- the parser does not know the build). */
    st = load_capturing_stderr("catalogue_backend = bogus\n", &cfg,
                               err, sizeof(err));
    ASSERT_EQ(st, MDS_ERR_INVAL);
    ASSERT_TRUE(strstr(err, "unknown catalogue_backend 'bogus'") != NULL);
    ASSERT_TRUE(strstr(err, "known: rondb, memdb, fdb") != NULL);
    st = load_capturing_stderr("catalogue_backend = RonDB\n", &cfg,
                               err, sizeof(err));
    ASSERT_EQ(st, MDS_ERR_INVAL);
    ASSERT_TRUE(strstr(err, "unknown catalogue_backend 'RonDB'") != NULL);
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_cluster_extract\n");

    /* The factory reports refusals through MDS_LOG_ERROR, which drops
     * every record until the logger is initialised; route it to stderr
     * so the message assertions below can capture it. */
    mds_log_init(NULL);

    RUN_TEST(test_cluster_supported_predicates);
    RUN_TEST(test_cluster_dispatch_args_and_passthrough);
    RUN_TEST(test_subtree_init_loads_rows);
    RUN_TEST(test_subtree_init_list_failure_is_fatal);
    RUN_TEST(test_subtree_init_root_claim_insert_only);
    RUN_TEST(test_subtree_refresh_from_catalogue);
    RUN_TEST(test_subtree_seed_shards);
    RUN_TEST(test_failover_take_over_persists_with_cas);
    RUN_TEST(test_failover_take_over_without_cas_slot);
    RUN_TEST(test_membership_populate);
    RUN_TEST(test_watchdog_start_contract);
    RUN_TEST(test_heartbeat_plausibility_predicate);
    RUN_TEST(test_watchdog_indeterminate_partner_skips_tick);
    RUN_TEST(test_heartbeat_tick_supersession);
    RUN_TEST(test_image_feed_dispatch);
    RUN_TEST(test_backend_names);
    RUN_TEST(test_backend_registry);
    RUN_TEST(test_open_unavailable_backend);
    RUN_TEST(test_config_backend_parsing);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
