/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * cluster_drain.c — Donor-local drain orchestrator (Seq 8).
 */

#include "cluster_drain.h"

#include <stdlib.h>
#include <string.h>

#include "cluster_membership.h"
#include "cluster_transport.h"
#include "mds_catalogue.h"
#include "migration.h"
#include "subtree_map.h"

/* -----------------------------------------------------------------------
 * cluster_drain_self
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status cluster_drain_self(
    struct cluster_membership *ctx,
    struct subtree_map *map,
    struct mds_catalogue *cat,
    uint32_t dest_mds_id)
{

    if (ctx == NULL || map == NULL || cat == NULL) {
        return MDS_ERR_INVAL;
    }
    if (dest_mds_id == 0) {
        return MDS_ERR_INVAL;
    }

    uint32_t self_id = subtree_map_self_id(map);

    /* ---- Pre-flight: validate destination eligibility ---- */
    if (dest_mds_id == self_id) {
        return MDS_ERR_INVAL;
    }
    if (!cluster_membership_can_own_subtrees(ctx, dest_mds_id)) {
        return MDS_ERR_PERM;
    }

    /* ---- Pre-flight: validate self is ACTIVE + ACTIVE_SERVING ---- */
    struct cluster_member self_m;
    enum mds_status st = cluster_membership_get(ctx, self_id, &self_m);
    if (st != MDS_OK) {
        return st;
    }
    if (self_m.role != NODE_ACTIVE) {
        return MDS_ERR_PERM;
    }
    if (self_m.lifecycle != NODE_ACTIVE_SERVING) {
        return MDS_ERR_PERM;
    }

    /* ---- Transition to DRAINING ---- */
    st = cluster_membership_set_lifecycle(ctx, self_id, NODE_DRAINING);
    if (st != MDS_OK) {
        return st;
    }

    /* ---- Enumerate owned subtrees ---- */
    struct subtree_entry *subs = NULL;
    uint32_t nsubs = 0;
    st = subtree_map_get_node_subtrees(map, self_id, &subs, &nsubs);
    if (st != MDS_OK) {
        goto undrain;
    }

    /* Reject if any owned subtree is root "/". */
    for (uint32_t i = 0; i < nsubs; i++) {
        if (subs[i].path[0] == '/' && subs[i].path[1] == '\0') {
            free(subs);
            st = MDS_ERR_PERM;
            goto undrain;
        }
    }

    /* ---- Migrate each subtree ---- */
    for (uint32_t i = 0; i < nsubs; i++) {
        /* Resolve subtree root fileid via the catalogue API. */
        uint64_t root_fid = 0;
        st = mds_cat_resolve_path(cat, subs[i].path, &root_fid);
        if (st != MDS_OK) {
            free(subs);
            goto undrain;
        }

        /* Connect migration transport to destination. */
        struct migration_transport *transport = NULL;
        st = cluster_transport_connect_migration_by_id(
            ctx, dest_mds_id, &transport);
        if (st != MDS_OK) {
            free(subs);
            goto undrain;
        }

        /* Run the 4-phase migration. */
        st = migration_initiate(cat, map, transport,
                                subs[i].path, root_fid, dest_mds_id,
                                       NULL);

        cluster_transport_disconnect_migration(transport);

        if (st != MDS_OK) {
            free(subs);
            goto undrain;
        }
    }

    free(subs);

    /* ---- Verify zero remaining ---- */
    if (subtree_map_node_owns_subtrees(map, self_id)) {
        st = MDS_ERR_IO;
        goto undrain;
    }

    /* ---- Transition to DRAINED ---- */
    st = cluster_membership_set_lifecycle(ctx, self_id, NODE_DRAINED);
    if (st != MDS_OK) {
        goto undrain;
    }

    return MDS_OK;

undrain:
    /* Best-effort: restore to ACTIVE_SERVING. */
    (void)cluster_membership_set_lifecycle(ctx, self_id,
                                           NODE_ACTIVE_SERVING);
    return st;
}
