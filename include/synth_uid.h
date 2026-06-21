/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * synth_uid.h -- RFC 8435 §2.2.1 synthetic UID derivation.
 *
 * Derives a deterministic per-(fileid, stripe, mirror) uid from
 * an MDS-side secret using HMAC-SHA256.  The result is clamped to
 * [0x40000000, 0x7FFFFFFF] so it cannot collide with real local
 * users on the DS host.
 *
 * Used by:
 *   - compound_layout.c::op_layoutget  (ffl_user on the wire)
 *   - proxy_io.c::mds_proxy_set_ds_owner  (chown on DS backing file)
 *
 * Both call sites MUST use the same function so the advertised
 * ffl_user and the DS file owner are always in agreement.
 */

#ifndef SYNTH_UID_H
#define SYNTH_UID_H

#include <stdint.h>
#include <string.h>
#include <openssl/hmac.h>

/* Synthetic UID range: 0x40000000 .. 0x7FFFFFFF (1 Gi namespace). */
#define SYNTH_UID_BASE  0x40000000U
#define SYNTH_UID_MASK  0x3FFFFFFFU

/**
 * Derive a synthetic uid from (secret, fileid, stripe, mirror).
 *
 * @param secret      MDS-side 32-byte key.
 * @param secret_len  Length of secret (must be 32).
 * @param fileid      MDS file ID.
 * @param stripe      Stripe index.
 * @param mirror      Mirror index.
 * @return uid in [SYNTH_UID_BASE, SYNTH_UID_BASE | SYNTH_UID_MASK].
 */
static inline uint32_t synth_uid_from_secret(
    const uint8_t *secret, uint32_t secret_len,
    uint64_t fileid, uint32_t stripe, uint32_t mirror)
{
    /*
     * Input: fileid (8 bytes LE) || stripe (4 bytes LE) || mirror (4 bytes LE).
     * Total: 16 bytes, fixed size, no ambiguity.
     */
    uint8_t input[16];
    uint8_t digest[32];
    unsigned int dlen = 0;
    uint32_t raw;

    /* Encode little-endian. */
    memcpy(input + 0, &fileid, 8);
    memcpy(input + 8, &stripe, 4);
    memcpy(input + 12, &mirror, 4);

    (void)HMAC(EVP_sha256(), secret, (int)secret_len,
               input, sizeof(input), digest, &dlen);

    /* First 4 bytes of digest → uid, clamped to synthetic range. */
    memcpy(&raw, digest, 4);
    return (raw & SYNTH_UID_MASK) | SYNTH_UID_BASE;
}

#endif /* SYNTH_UID_H */
