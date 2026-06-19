# Contributing to Lattice

**Copyright (c) 2026 PeakAIO. Licensed under MIT — by submitting a contribution you agree that your contribution will be licensed under the same terms (see `LICENSE-MIT`).**

This project targets kernel-grade code quality. Every line of code must
be written as if it will be reviewed by Linus Torvalds, because it will be.

---

## 1. Code Style

We follow the Linux kernel coding style with the following specifics:

- **K&R bracing**: opening brace on the same line as the statement.
  Exception: function definitions open on the next line (kernel style).
- **Hard tabs for indentation**: 8-character tab width, matching kernel
  convention. This is non-negotiable. If your editor converts tabs to
  spaces, fix your editor.
- **Line length**: 100 characters maximum. No exceptions.
- **Single-letter variables** are fine in small scopes (loop indices,
  pointer iterators). Use descriptive names in larger scopes.
- **`lowercase_with_underscores`** for variables and functions.
  **`UPPERCASE`** for macros and constants only.
- **No typedefs for structs**. Use `struct foo` everywhere. Typedefs
  hide what things are. The only acceptable typedefs are for function
  pointers and genuinely opaque types.
- **Declare variables at the top of the block** (C99 declarations in
  the middle of code are acceptable only when the variable's scope is
  genuinely limited and the initialisation depends on preceding logic).

### What will get your patch rejected

- Tabs/spaces mixing.
- Braces around single-statement bodies (unless one arm of if/else is
  multi-line, then both get braces — kernel rule).
- Trailing whitespace.
- Gratuitous blank lines.
- "Yoda conditions" (`if (0 == x)`).
- C++ style comments (`//`) in .c files. Use `/* */`.

---

## 2. Compiler Discipline

All code **must** compile cleanly with GCC 12.1 or later:

```
-std=c11 -Wall -Wextra -Werror -Wshadow -Wstrict-prototypes
-Wmissing-prototypes -Wold-style-definition -Wformat=2
-Wno-unused-parameter -Wdeclaration-after-statement
```

Additionally, code must pass:

- **`sparse`** (`make C=1`): catches address space mismatches, incorrect
  `__user`/`__kernel` annotations (relevant for the kernel patch), and
  type confusion.
- **`cppcheck --enable=all`**: no warnings on any committed code.
- **`clang-tidy`** with `cert-*`, `bugprone-*`, `readability-*` checks.

Suppressing warnings with pragmas or casts is not acceptable unless
accompanied by a comment explaining why the warning is a false positive.

---

## 3. No Undefined Behaviour

Zero tolerance. If you cannot prove a memory access is safe, the code
does not ship.

Specific rules:

- **Every `malloc`/`calloc`/`realloc` return is checked.** No exceptions.
- **Every pointer is NULL-checked before dereference** at API boundaries.
  Internal functions may skip this only if the caller provably guarantees
  non-NULL.
- **No signed integer overflow.** Use unsigned types for sizes, counts,
  and indices. If you must do signed arithmetic, prove the range or use
  `__builtin_add_overflow()`.
- **No type-punning through pointer casts** that violates strict aliasing.
  Use `memcpy()` for type conversion. The compiler will optimise it.
- **No `void *` arithmetic.** Always cast to `char *` or `uint8_t *` first.
- **No variable-length arrays (VLAs).** They are a stack overflow waiting
  to happen. Use `malloc` or a fixed-size buffer with bounds checking.
- **No `alloca()`.** Same reason.
- **Shift amounts must be less than the type width.** Check them.

---

## 4. Error Handling

Every system call, library call, and internal function that can fail
**must** have its return value checked and handled.

- **Propagate errors, don't swallow them.** If `fopen()` fails, return
  an error code to the caller. Do not `fprintf` and continue.
- **Use `goto cleanup`** for multi-resource functions. This is the
  established kernel pattern for a reason: it keeps the happy path
  linear and ensures all resources are released on every error path.
- **No silent failures.** If something fails, log it (component + level)
  and return a meaningful error code.
- **Check `errno` immediately** after the failing call. Don't call other
  functions between the failure and the `errno` read.

### `goto` rules

- Only jump forward (to a cleanup label at the bottom of the function).
- Labels are `out_free_foo:` style (describe what they clean up).
- Never `goto` across variable initialisations.

---

## 5. Memory Safety

- **Ownership must be documented.** Every allocated buffer must have a
  clear owner. If ownership transfers (e.g., `repl_send_async` takes
  ownership of the delta buffer), document it in a comment at the call
  site and the function declaration.
- **Free exactly once.** After `free(ptr)`, set `ptr = NULL`. This is
  cheap insurance against double-free.
- **No dangling pointers.** If a function frees something that other code
  might reference, invalidate those references first.
- **Use `sizeof(*ptr)`** for allocation, not `sizeof(struct foo)`. This
  is immune to type changes:
  ```c
  struct foo *p = calloc(1, sizeof(*p));
  ```
- **`valgrind --leak-check=full` must report zero errors** on all test
  binaries. This is a CI gate.

---

## 6. Function Design

- **Functions must be short.** Target < 50 lines. Absolute maximum 100
  lines. If a function exceeds this, it is doing too much — refactor.
- **Single responsibility.** One function does one thing.
- **Obvious control flow.** A reader should understand what the function
  does by reading it top to bottom. No clever tricks, no hidden gotos,
  no deeply nested conditionals (max 3 levels of nesting).
- **`static` by default.** Every function is `static` (internal linkage)
  unless it needs to be called from another translation unit.
- **`const` correctness.** If a parameter is not modified, it is `const`.
  If a return value should not be modified, it is `const`. No exceptions.
- **`restrict`** on pointer parameters where aliasing is not expected.
  This documents intent and enables compiler optimisation.

---

## 7. Comments

- **Comments explain WHY, not WHAT.** `/* increment counter */` on `i++`
  is worse than no comment. `/* Retry because DS may be transiently
  unavailable during migration */` is useful.
- **No commented-out code in committed files.** If code is not needed,
  delete it. That is what version control is for.
- **Doxygen for public API:** every function in a header must have:
  ```c
  /**
   * brief_description
   *
   * @param name  What it is and valid range.
   * @return      What the return value means.
   *
   * Ownership: caller retains / transfers / borrows.
   * Thread safety: safe / requires lock X.
   */
  ```
- **File-level comments** at the top of every .c file: what it does,
  which architecture section it implements.

---

## 8. Commit Discipline

- **One logical change per commit.** Do not mix refactoring with
  bug fixes with new features.
- **Bisectable history.** Every commit must compile and pass all
  existing tests. No "fix build in next commit" patterns.
- **Commit message format:**
  ```
  subsystem: short imperative summary (< 72 chars)

  Explain WHAT changed and WHY. Not HOW — the diff shows how.
  Reference architecture.md section numbers when relevant.

  Update docs/architecture.md §23 status for affected items.
  ```
  Commits are authored as `Eyal Lemberger <eyal.lemberger@peakaio.com>`.
  Do not add `Co-Authored-By`, `Authored-By`, or any other attribution
  trailer.
- **No merge commits on the main branch.** Rebase only.
- **Branch flow for `peak-hpc`:**
  - `main` is the compatibility/source branch.
  - Shared work should land in `main` first and then flow from `main` to
    `peak-hpc`.
  - Do not open PRs from `peak-hpc` into `main`.
  - Keep the GitHub Actions check `Branch Policy / Reject peak-hpc ancestry into main`
    required on `main` so obvious reverse merges are blocked.
- **Tag phase completions:** `v0.1-phase1`, `v0.2-phase2`, etc.

---

## 9. Testing Requirements

No code ships without tests.

- **Unit tests** for every public function. Cover:
  - Happy path (nominal input)
  - Error paths (NULL, zero-length, max values, OOM simulation)
  - Edge cases (boundary conditions, empty directories, single-entry)
- **Integration tests** for every phase milestone.
- **Test names describe the scenario:** `test_rename_cross_dir_same_mds`,
  not `test_rename_3`.
- **Tests must be deterministic.** No timing dependencies, no random
  data without fixed seeds, no reliance on external services (mock them).
- **valgrind on all test binaries** in CI.
- **Fuzz testing** for any code that parses external input (XDR decode,
  config parser, protobuf).

---

## 10. Static Analysis Gate

Before any code is committed, it must pass:

1. `gcc` and `clang` with all warnings enabled (see §2).
2. `sparse` for type checking.
3. `cppcheck --enable=all --error-exitcode=1`.
4. `clang-tidy` with CERT and bugprone checks.
5. `valgrind --leak-check=full --error-exitcode=1` on test binaries.

These will be CI-enforced. No overrides without sign-off from the
project lead.

---

## 11. Performance Rules

- **Measure before optimising.** No speculative optimisation. Profile
  with `perf` or `flamegraph` and prove the bottleneck exists before
  changing code for performance.
- **O(n) where possible.** Document algorithmic complexity in comments
  for any function that is not obviously O(1) or O(n).
- **No malloc in the hot path** after initialisation. Pre-allocate
  buffers, use pools, or use stack allocation (with bounded size).
- **Cache-friendly data layout.** Structs that are traversed together
  are stored together. Use `_Static_assert` to verify struct sizes
  and alignment when it matters.

---

## 12. Security

- **Sanitise all external input.** NFS client data is untrusted. Config
  files are semi-trusted. Nothing from the network is trusted.
- **No format string vulnerabilities.** Never pass user-controlled data
  as a format string. Use `"%s"` explicitly.
- **No hard-coded secrets.** All credentials, tokens, and keys come from
  config or environment.
- **Principle of least privilege.** The daemon drops root after binding
  privileged ports.
- **No TOCTOU.** If you check a condition and then act on it, the
  condition must still hold when you act. Use atomic operations or
  hold locks across check-and-act sequences.

---

## 13. Review Checklist

Before submitting code for review, verify:

- [ ] Compiles with `-Wall -Wextra -Werror` on both gcc and clang
- [ ] No `cppcheck` or `clang-tidy` warnings
- [ ] `valgrind` clean on all affected tests
- [ ] All new functions have Doxygen comments
- [ ] All error paths return meaningful error codes
- [ ] All allocations are checked and freed
- [ ] No UB: no signed overflow, no NULL deref, no OOB access
- [ ] Tests cover happy path, error paths, and edge cases
- [ ] Commit is bisectable (compiles + tests pass in isolation)
- [ ] architecture.md §23 updated with status change

---

*When in doubt, ask: "Would this survive Linus's review?" If no,
rewrite it until the answer is yes.*
