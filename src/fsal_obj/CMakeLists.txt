# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

add_library(pnfs_fsal_obj STATIC
    fsal_obj_main.c
    layout.c
    ds_file.c
    placement.c
    referral.c
    lease_table.c
)

target_include_directories(pnfs_fsal_obj PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(pnfs_fsal_obj
    PRIVATE
        pnfs_common
        
)
