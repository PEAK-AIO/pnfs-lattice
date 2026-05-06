/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * hpc_hint.h -- shared in-memory representation for the HPC striping hint.
 */

#ifndef HPC_HINT_H
#define HPC_HINT_H

#include <stdint.h>

#define HPC_HINT_BODY_SIZE   16U  /* uint64 + uint32 + uint32 */

struct pnfs_hpc_hint {
    uint64_t expected_file_size;
    uint32_t expected_client_count;
    uint32_t flags;
};

#endif /* HPC_HINT_H */
