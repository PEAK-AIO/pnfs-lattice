# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT

# eBPF programs require a separate build step (clang -target bpf).
# The userspace loader is built as a normal C library.

if(LIBBPF_FOUND)
    add_library(pnfs_bpf STATIC
        repl_monitor.c
    )

    target_include_directories(pnfs_bpf PUBLIC
        ${PROJECT_SOURCE_DIR}/include
    )

    target_link_libraries(pnfs_bpf
        PRIVATE
            pnfs_common
            ${LIBBPF_LIBRARIES}
    )

    # Custom command to build BPF object from .bpf.c
    # Requires: clang, bpftool (for skeleton generation)
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/repl_monitor.bpf.o
        COMMAND clang -O2 -g -target bpf
                -I${PROJECT_SOURCE_DIR}/include
                -c ${CMAKE_CURRENT_SOURCE_DIR}/repl_monitor.bpf.c
                -o ${CMAKE_CURRENT_BINARY_DIR}/repl_monitor.bpf.o
        DEPENDS repl_monitor.bpf.c
        COMMENT "Building eBPF program repl_monitor.bpf.o"
    )

    add_custom_target(bpf_programs ALL
        DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/repl_monitor.bpf.o
    )
else()
    message(STATUS "libbpf not found — eBPF monitoring disabled")
endif()
