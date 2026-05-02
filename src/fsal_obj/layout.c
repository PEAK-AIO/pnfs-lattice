/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout.c — LAYOUTGET / LAYOUTRETURN / LAYOUTCOMMIT handlers.
 *
 * Returns flex file layouts (RFC 8435) to pNFS clients.
 * See docs/architecture.md §8 for layout encoding details.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "pnfs_mds.h"
#include "layout_types.h"
#include "layout_recall.h"

enum mds_status layout_get(uint64_t fileid,
                           enum layout_iomode iomode,
                           uint64_t offset,
                           uint64_t length,
                           uint64_t clientid,
                           void **buf,
                           /* NOLINTNEXTLINE(readability-non-const-parameter) */
                           size_t *buf_len,
                           struct mds_stateid *stateid)
{
    /* TODO: implement
     * 1. Lookup stripe map for fileid from catalogue
     * 2. Build ff_layout4 body:
     *    a. For each stripe: DS address (multipath_list),
     *       DS file handle, stateid, synthetic uid/gid
     *    b. stripe_unit from stripe_map
     * 3. Create layout_state record in catalogue
     *    (layout_state key: [fileid][clientid][stateid])
     * 4. Populate output struct with XDR-encoded layout body
     * 5. Replicated commit
     */
    (void)fileid;
    (void)iomode;
    (void)offset;
    (void)length;
    (void)clientid;
    (void)buf;
    (void)buf_len;
    (void)stateid;
    return MDS_ERR_LAYOUTUNAVAIL;
}

enum mds_status layout_return(uint64_t fileid,
                              const struct mds_stateid *stateid,
                              enum layout_iomode iomode,
                              uint64_t offset,
                              uint64_t length)
{
    /* TODO: implement
     * 1. Lookup layout_state record
     * 2. Process layoutreturn body:
     *    - If ff_ioerr4 present → handle DS errors
     *    - If ff_iostats4 present → update stats
     * 3. Remove layout_state record from catalogue
     * 4. Replicated commit
     */
    (void)fileid;
    (void)stateid;
    (void)iomode;
    (void)offset;
    (void)length;
    return MDS_ERR_INVAL;
}

enum mds_status layout_commit(uint64_t fileid,
                              const struct mds_stateid *stateid,
                              uint64_t new_offset,
                              uint64_t new_length)
{
    /* TODO: implement
     * 1. Update file size if new_offset + new_length > current size
     * 2. Update mtime
     * 3. Replicated commit
     */
    (void)fileid;
    (void)stateid;
    (void)new_offset;
    (void)new_length;
    return MDS_ERR_INVAL;
}

enum mds_status device_info_get(const struct mds_deviceid *device,
                                void **buf,
                                /* NOLINTNEXTLINE(readability-non-const-parameter) */
                                size_t *buf_len)
{
    /* TODO: implement
     * Encode DS addresses for GETDEVICEINFO.
     */
    (void)device;
    (void)buf;
    (void)buf_len;
    return MDS_ERR_INVAL;
}

__attribute__((unused))
static int layout_recall(uint64_t fileid, uint64_t offset, uint64_t length)
{
    /* TODO: implement
     * Send CB_LAYOUTRECALL to all clients holding layouts for
     * this fileid range. Used before migration or DS failure.
     */
    (void)fileid;
    (void)offset;
    (void)length;
    return -1;
}
