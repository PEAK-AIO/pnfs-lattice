/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_types.h -- Flex file layout structures (RFC 8435).
 */

#ifndef LAYOUT_TYPES_H
#define LAYOUT_TYPES_H

#include <stdint.h>
#include "pnfs_mds.h"

/* NFSv4.1 layout type constants */
#define LAYOUT4_FLEX_FILES  4

/* NFSv4.1 layout iomode */
enum layout_iomode {
    LAYOUTIOMODE4_READ = 1,
    LAYOUTIOMODE4_RW   = 2,
    LAYOUTIOMODE4_ANY  = 3,
};

/* Flex file flags (RFC 8435 S5.1) */
#define FF_FLAGS_NO_LAYOUTCOMMIT   0x00000001
#define FF_FLAGS_NO_IO_THRU_MDS    0x00000002
#define FF_FLAGS_NO_READ_IO        0x00000004
#define FF_FLAGS_WRITE_ONE_MIRROR  0x00000008

/* -----------------------------------------------------------------------
 * Device ID -- identifies a set of DS addresses
 * ----------------------------------------------------------------------- */

#define DEVICEID4_SIZE 16

struct mds_deviceid {
    uint8_t id[DEVICEID4_SIZE];
};

/* -----------------------------------------------------------------------
 * DS address info (for GETDEVICEINFO)
 * ----------------------------------------------------------------------- */

struct mds_ds_addr {
    uint32_t ds_id;
    char     host[256];
    uint16_t port;
    bool     rdma;          /* true -> netid "rdma", false -> "tcp" */
};

/* -----------------------------------------------------------------------
 * Layout stateid (simplified NFSv4 stateid)
 * ----------------------------------------------------------------------- */

struct mds_stateid {
    uint32_t seqid;
    uint8_t  other[12];
};

/* -----------------------------------------------------------------------
 * Layout record -- persisted in catalogue for crash recovery
 * ----------------------------------------------------------------------- */

struct mds_layout_record {
    uint64_t             fileid;
    uint64_t             clientid;
    uint32_t             layout_type;     /* LAYOUT4_FLEX_FILES */
    enum layout_iomode   iomode;
    uint64_t             offset;
    uint64_t             length;
    struct mds_stateid   stateid;
    uint32_t             ds_count;
    struct mds_ds_map_entry ds_map[];     /* flexible array */
};

/* -----------------------------------------------------------------------
 * Layout operations API
 * ----------------------------------------------------------------------- */

struct mds_layout_ctx;

/**
 * @brief Generate a flex file layout for LAYOUTGET.
 * @param fileid   File to generate layout for.
 * @param iomode   READ or RW.
 * @param offset   Start of requested range.
 * @param length   Length of requested range (0 = EOF).
 * @param clientid Requesting client.
 * @param[out] buf     Receives XDR-encoded layout body.
 * @param[out] buf_len Receives layout body length.
 * @param[out] stateid Receives the layout stateid.
 * @return MDS_OK on success.
 */
enum mds_status layout_get(uint64_t fileid,
                           enum layout_iomode iomode,
                           uint64_t offset,
                           uint64_t length,
                           uint64_t clientid,
                           void **buf,
                           size_t *buf_len,
                           struct mds_stateid *stateid);

/**
 * @brief Process LAYOUTRETURN.
 * @param fileid   File.
 * @param stateid  Layout stateid being returned.
 * @param iomode   Mode of the returned segment.
 * @param offset   Start of returned range.
 * @param length   Length of returned range.
 * @return MDS_OK on success.
 */
enum mds_status layout_return(uint64_t fileid,
                              const struct mds_stateid *stateid,
                              enum layout_iomode iomode,
                              uint64_t offset,
                              uint64_t length);

/**
 * @brief Process LAYOUTCOMMIT -- update file size if needed.
 * @param fileid     File.
 * @param stateid    Layout stateid.
 * @param new_offset New last-write offset reported by client.
 * @param new_length New last-write length.
 * @return MDS_OK on success.
 */
enum mds_status layout_commit(uint64_t fileid,
                              const struct mds_stateid *stateid,
                              uint64_t new_offset,
                              uint64_t new_length);

/**
 * @brief Encode DS addresses for GETDEVICEINFO.
 * @param device    Device ID to look up.
 * @param[out] buf      Receives XDR-encoded device address.
 * @param[out] buf_len  Receives buffer length.
 * @return MDS_OK on success.
 */
enum mds_status device_info_get(const struct mds_deviceid *device,
                                void **buf,
                                size_t *buf_len);

#endif /* LAYOUT_TYPES_H */
