# 0002: Parallel masking in ordered segments

- **Status:** accepted
- **Date:** November 2026
- **Scope:** `--threads` (`src/engine/sink.rs`, `src/engine/mod.rs`) and the allocator of the binary (`src/main.rs`, `.cargo/config.toml`)

## Context

With rules, masking dominates the cost of a run: on COPY, HMAC on four keys takes throughput from 1.7 to 0.6 million rows per second, and the full rule set to 0.42 million ([benchmarks](../benchmarks.md)). Issue #10 asks for parallel masking. It has to keep the properties the rest of the tool relies on:

* a single pass over the input;
* memory bounded independently of the size of the dump;
* the output byte for byte in input order, with every unmasked byte copied;
* fail closed: a block that cannot be masked stops the run;
* reproducible runs with `--seed`.

## Decision

1. **One reading thread, N masking threads.** The calling thread reads, parses and writes; `--threads N` adds N threads that only mask rows. Parsing stays on one thread because the parser's state at a byte depends on every earlier byte.
2. **Segments.** The reading thread groups its output into segments: the bytes to copy, plus the rows to mask with their place among those bytes. A segment closes after 1,024 masked rows or 256 KiB of input. Rows are copied into the segment's own buffer, and segments and output buffers are reused, so no thread allocates per row or frees what another thread allocated.
3. **Bounded and in order.** At most 2N segments are out at a time (queued, being masked, or done but waiting for an earlier one). The reading thread writes finished segments in sequence and holds early ones until their turn. Memory is proportional to N times the segment size, not to the dump.
4. **One generator per segment.** The rows of a segment draw from a `StdRng` seeded with 256 bits from the run's generator, in segment order. Where segments end depends only on the input, so a seeded run writes the same output for any N. With N = 1 the rows are masked in place, without threads, but with the same per-segment generators. This changed the seeded golden output once.
5. **Errors.** A fail-closed error still writes everything before it and returns the same error as with one thread. A panic in a masking thread becomes an error for its segment, so the reading thread never waits for a segment that will not come.
6. **mimalloc as the global allocator of the binary**, built to commit memory on demand (see below).

The pool uses `std` only: `std::thread::scope`, which lets the masking threads borrow the compiled rules, and `std::sync::mpsc` channels.

## Alternatives

* **A separate writer thread.** It would require the output to be `Send`. Writing a segment is cheap compared with parsing it, so the reading thread writes.
* **Rayon or crossbeam.** New dependencies for what scoped threads and two channels already do, and Rayon's parallel iterators work on a collection that is known in advance, not on a stream with bounded memory.
* **One generator shared by the threads.** A lock taken for every value, and an output that depends on scheduling.
* **One generator per row.** Seeding ChaCha12 for every row costs more than many maskers, and the output would be the same as with segments.
* **Parsing on several threads.** A chunk boundary can fall inside a string literal or a dollar-quoted body, so the chunks cannot be parsed independently without a first pass to find safe cut points. This remains future work.

## Allocator

With glibc's allocator the threads gave no speedup: the full rule set on COPY took 7.2 s with one thread and 5.7–7.6 s with two to four. glibc's per-thread cache (tcache) keeps freed chunks from any arena. A chunk allocated by the reading thread and freed by a masking thread (thread start-up does this) is handed out again by the masking thread. A `realloc` of it then locks the reading thread's arena and allocates the replacement there, so these chunks keep circulating and every masking thread waits on that lock. The maskers call `realloc` often: the `fake` crate builds names and e-mails with `format!` and `replace`. Disabling the cache (`GLIBC_TUNABLES=glibc.malloc.tcache_count=0`) brought 4 threads from 6.3 s to 2.5 s. That confirmed the cause, but an environment variable cannot be the fix.

mimalloc's heaps are per thread, and with it the same run takes 1.8 s with three threads. Its default on Linux commits whole arenas, which raised the peak memory from 6 to 15.5 MiB with one thread and to up to 66 MiB with threads. Built with `MI_DEFAULT_ARENA_EAGER_COMMIT=0`, set in `.cargo/config.toml`, it peaks at 6.6 MiB with one thread and at most 13.6 MiB with four, at the same speed. The option cannot be set from `main`, because mimalloc reserves its first arena before `main` runs.

What it costs: a C dependency built by `cc`, 157 KiB of binary, and a build setting that a `CFLAGS` variable in the environment replaces. If that happens, the binary is still correct, but its peak memory is higher. The library and the Criterion benchmarks keep the system allocator.

## Consequences

* With three threads every rule set runs at 93–104% of the speed of a copy without rules ([benchmarks](../benchmarks.md#parallel-masking)). The parser on the reading thread is now the bottleneck, which reopens the decision on the chunked parser.
* Peak memory grows with N (segments in flight and one heap per thread) but not with the dump.
* The default stays at one thread, which runs as before.
