/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_io_limits.h -- per-DS I/O limits (Wave 5 T5.1).
 *
 * The MDS must not advertise a read/write size its data servers
 * reject: in flex-files pNFS the client writes DIRECTLY to the DS at
 * the size the MDS put in GETDEVICEINFO (ffdv_rsize/ffdv_wsize), so
 * an oversized advertisement against a stock 512 KiB knfsd wedges
 * hard-mounted clients in an unkillable retry loop.
 *
 * This module probes each ONLINE generic DS with NFSv3 FSINFO,
 * applies the safety policy below, and serves the effective per-DS
 * values to the GETDEVICEINFO/LAYOUTGET encoders plus a min-across-
 * DSes defence-in-depth value for the MDS's own MAXREAD/MAXWRITE.
 *
 * Policy:
 *   - probed rtmax/wtmax are capped at 1 MiB (the Linux client's own
 *     NFS_MAX_FILE_IO_SIZE ceiling) and rounded down to a 4 KiB
 *     multiple;
 *   - a DS that has never answered a probe advertises a safe 64 KiB
 *     fallback;
 *   - a failed probe keeps the last-known-good values (never revert
 *     a working DS to fallback because of one lost probe);
 *   - a decoded limit below 4 KiB marks the DS ineligible for new
 *     layout placement;
 *   - any change to the effective values bumps the DS's device-ID
 *     generation (so new layouts carry a new device ID and clients
 *     re-fetch device info); a DECREASE or an eligibility loss
 *     additionally recalls the DS's outstanding layouts -- after the
 *     safe values are published, never before.
 *
 * Values are process-local by design: every MDS independently
 * validates the DS instance it serves; nothing is persisted.
 *
 * When the module is disabled (ds_iolimit_probe_ms = 0) every getter
 * reports the legacy constant 1 MiB, reproducing the pre-Wave-5 wire
 * output bit-for-bit.
 */

#ifndef DS_IO_LIMITS_H
#define DS_IO_LIMITS_H

#include <stdint.h>
#include <stdbool.h>

/** Hard ceiling: the Linux client caps per-DS I/O at 1 MiB anyway. */
#define DS_IOLIMIT_CAP_BYTES      1048576U
/** Effective values are rounded down to a multiple of this. */
#define DS_IOLIMIT_ROUND_BYTES    4096U
/** Advertised for a DS that has never answered a probe. */
#define DS_IOLIMIT_FALLBACK_BYTES 65536U
/** Advertised for every DS when the module is disabled (legacy). */
#define DS_IOLIMIT_LEGACY_BYTES   1048576U

struct mds_catalogue;
struct layout_recall;

/**
 * Probe function signature -- ds_nfs3_fsinfo() shape.  Injectable so
 * unit tests can drive the policy without a live DS.
 */
typedef int (*ds_iolimit_probe_fn)(const char *host, uint16_t port,
                                   const char *export_path,
                                   uint32_t *rtmax_out,
                                   uint32_t *wtmax_out,
                                   uint32_t timeout_ms);

/**
 * Enable the module (idempotent).  Until this is called every getter
 * returns the legacy constants.  @return 0 on success.
 */
int ds_io_limits_enable(void);

/** Tear down (stops the prober if running).  NULL-safe/idempotent. */
void ds_io_limits_shutdown(void);

/** True once ds_io_limits_enable() has run (and no shutdown since). */
bool ds_io_limits_enabled(void);

/** Inject a probe function (tests).  NULL restores ds_nfs3_fsinfo. */
void ds_io_limits_set_probe_fn(ds_iolimit_probe_fn fn);

/** Borrow the recall coordinator used on capability decreases. */
void ds_io_limits_set_recall(struct layout_recall *lr);

/**
 * Effective advertised limits for one DS.
 *
 * Disabled module: legacy 1 MiB, generation 0.  Enabled but never
 * probed: 64 KiB fallback, generation 0.  Otherwise the policy-
 * processed values and the current device-ID generation.
 */
void ds_io_limits_get(uint32_t ds_id, uint32_t *rsize, uint32_t *wsize,
                      uint32_t *generation);

/**
 * Minimum effective read/write across eligible probed DSes --
 * defence in depth for the MDS's own MAXREAD/MAXWRITE.  Legacy 1 MiB
 * when disabled or when nothing has been probed yet.
 */
void ds_io_limits_min(uint32_t *maxread, uint32_t *maxwrite);

/** False only when a probe decoded limits below 4 KiB (broken DS). */
bool ds_io_limits_eligible(uint32_t ds_id);

/**
 * One probe pass over all ONLINE generic DSes in the catalogue.
 * Publishes changed values (generation bump) before invoking the
 * recall hook for decreases.  Called by the prober thread and by
 * tests.
 */
void ds_io_limits_sweep(struct mds_catalogue *cat);

/**
 * Start the periodic prober thread (runs one sweep immediately, then
 * every @interval_ms).  Requires ds_io_limits_enable() first.
 * @return 0 on success, -1 on error.
 */
int ds_io_limits_start(struct mds_catalogue *cat, uint32_t interval_ms);

/** Stop and join the prober thread.  NULL-safe/idempotent. */
void ds_io_limits_stop(void);

#endif /* DS_IO_LIMITS_H */
