# io_uring buffer ring allocators: adaptive vs recycling

Compares Netty's `IoUringAdaptiveBufferRingAllocator` with `IoUringRecyclingBufferRingAllocator` from [netty/netty#17635](https://github.com/netty/netty/pull/17635) (the local copy matches the PR). An open-loop ramp finds the maximum QPS of each, the worker CPU is compared at equal load, and async-profiler compares heap and direct memory allocations.

## Summary

- **Same maximum QPS.** For all three blocking times, both allocators pass 180k QPS and saturate at 200k QPS. At saturation the two worker threads together use about 190%–199%, i.e. both workers are fully busy and the server is the bottleneck.
- **No measurable difference in worker CPU at equal load.** Differences stay within ±4% and go both ways, which is within the noise of a single run.
- **Same latency.** Below saturation the P99 of both is essentially the same, about 0.2–3.7 ms above the blocking time.
- **Recycling never fell back.** `fallbackAllocations` and `foreignThreadAllocations` are 0 in every run: the regions were never used up and nothing was allocated off the event loop.
- **Recycling does no direct memory malloc in steady state.** At 100k QPS the adaptive allocator's worker threads malloc about 2108 bytes per request (about 210 MB/s of direct memory allocated and freed again); recycling mallocs nothing. Heap allocation is only about 3% lower, within sampling noise. See [Allocations](#allocations-async-profiler).

In this HTTP scenario the allocator is not the bottleneck. The worker event loops are bound by HTTP/H2 codec work and io_uring send/receive, and the allocator difference is too small to move the maximum QPS.

## Setup

- **Hardware and software:** Intel Core i5-13600KF (6 P cores / 12 threads plus 8 E cores), Linux 7.3.0-6-generic, Oracle GraalVM 25.0.2, Netty 4.2.18.Final.
- **JVM:** both server and client use a fixed 512 MiB heap with G1, Netty leak detection disabled and Unsafe enabled.
- **CPU isolation:** processes only; no thread is pinned. The server runs on the P cores (CPUs 0–11) and the client on the E cores (CPUs 12–19).
- **Date:** 2026-10-06.

### Server

- **Threads:** 1 acceptor and 2 worker event loops (`-Dperf.workers=2`). Port 4399 accepts cleartext HTTP/1.1 and HTTP/2 prior knowledge.
- **io_uring buffer ring:** 4096 entries, refilled in batches of 2048, `batchAllocation=true`, multishot recv.
- **adaptive:** minimum and initial buffer size 1024 bytes, maximum 4096 bytes, `largeAllocation=true`.
- **recycling:** fixed 4096-byte slots, matching adaptive's maximum so both read with the same granularity. Default in-flight headroom (a quarter of the ring size); each worker thread has its own region.
- **Business logic:** a complete request is retained and handed to a virtual thread that waits 5, 10 or 20 ms in `LockSupport.parkNanos` (`-Dperf.blocking.ms`), standing in for Redis or another blocking call. The worker thread then answers with HTTP 200 and a fixed 2048-byte JSON body.
- **Cross-thread release:** each request is released on the worker thread or on the acceptor thread with equal probability, exercising recycling's cross-thread return path.
- **Buffers held while blocking:** the H1 aggregated content references buffers derived from the ring; for H2 it is the aggregated content the adapter copied DATA frames into.

### Client

- **Load threads:** 5 independent NIO event loops. Each paces its share of the schedule on its own loop, with no cross-thread hand-off. Request i of the global schedule is sent by loop `i % 5`.
- **Connections:** 8192 H1 connections in total, one request per connection at a time; 128 H2 connections, at most 64 concurrent streams each.
- **Requests:** POST with 2048-byte request and response bodies. H1 and H2 alternate by global sequence number, so QPS is split exactly 50/50.

## Method: open-loop ramp

- **Open loop:** each step sends requests strictly on schedule at the target QPS, without waiting for responses. The schedule has 1 ms granularity: every millisecond, all requests that are due are sent.
- **Ramp:** start at 20k QPS and add 20k per step. Before the first step there is a 15 s unmeasured warm-up. Each step sends unmeasured for 5 s, then measured for 15 s, then stops sending and waits for all of its requests before the next step.
- **Saturation:** the ramp stops at the first step where either
  - the actual QPS is below 95% of the target, or
  - any request failed, was rejected because the connection pool was busy, or did not complete.
- **Maximum QPS:** the target of the last step with actual QPS ≥ 95% of the target and no errors.
- **Latency:** P99 is measured from the actual send; `arrival_p99` is measured from the scheduled send time and therefore also includes client-side scheduling and queueing.
- **CPU:** utime + stime from `/proc` before and after the measurement window. "Worker" is the sum over all `perf-worker-*` threads, "client loop" the sum over all `perf-client-*` threads; 100% is one core.
- **Isolation between runs:** every combination of allocator and blocking time uses fresh server and client processes.

## Results

### Maximum QPS

| Blocking | adaptive | recycling | Worker CPU at the saturating step (200k), adaptive / recycling |
|---:|---:|---:|---:|
| 5 ms | 180,000 | 180,000 ¹ | 198.9% / 193.6% |
| 10 ms | 180,000 | 180,000 | 198.8% / 198.8% |
| 20 ms | 180,000 | 180,000 | 198.6% / 186.5% |

¹ In the main run, recycling with 5 ms blocking was flagged as saturated at 160k QPS: 0.7% of the requests were rejected (11,088 pool-busy) and P99 was 117.5 ms. But the workers were only at 156.9% combined, nowhere near saturation, which points to a transient stall. A rerun of this combination matched all the others (180k passes, 200k saturates). The table uses the rerun, and so do the recycling 5 ms rows below.

With a 20k step, the true limit lies between 180k and 200k; this test cannot resolve differences within that range.

### Worker CPU at equal load (both workers combined)

| Blocking | Load | adaptive | recycling | Difference |
|---:|---:|---:|---:|---:|
| 5 ms | 100k | 94.3% | 94.6% | +0.3% |
| 5 ms | 140k | 129.8% | 127.9% | −1.5% |
| 5 ms | 180k | 165.5% | 166.3% | +0.5% |
| 10 ms | 100k | 104.9% | 98.4% | −6.2% |
| 10 ms | 140k | 138.4% | 134.6% | −2.7% |
| 10 ms | 180k | 168.5% | 165.9% | −1.5% |
| 20 ms | 100k | 98.4% | 98.5% | +0.1% |
| 20 ms | 140k | 138.3% | 139.2% | +0.7% |
| 20 ms | 180k | 169.6% | 165.8% | −2.2% |

Difference is recycling relative to adaptive. Worker CPU grows roughly linearly with QPS, about 9%–10% of a core per 10k QPS. The −6.2% at 10 ms / 100k is the only point outside ±3%; every other load in that group stays within ±3%, so it is treated as single-run noise.

### Latency and process CPU at 180k QPS (the last passing step)

| Blocking | P99 adaptive / recycling | Server process CPU adaptive / recycling | Client loop CPU adaptive / recycling |
|---:|---:|---:|---:|
| 5 ms | 7.370 / 7.169 ms | 359.0% / 352.1% | 383.3% / 379.7% |
| 10 ms | 12.594 / 12.199 ms | 350.4% / 346.4% | 408.6% / 410.0% |
| 20 ms | 23.739 / 23.194 ms | 359.0% / 338.6% | 405.9% / 386.8% |

Server process CPU includes the workers, the acceptor, virtual thread carriers, GC and JIT. The 5 client loops use about 380%–410% combined, roughly 80% each, so they still have headroom and it is the server that saturates.

### Recycling counters

| Blocking | fallbackAllocations | foreignThreadAllocations |
|---:|---:|---:|
| 5 ms | 0 | 0 |
| 10 ms | 0 | 0 |
| 20 ms | 0 | 0 |

## Allocations (async-profiler)

### Method

- **Load:** fixed 100k QPS with 5 ms blocking, otherwise the same setup as above. One run per allocator; both held 100k QPS without errors.
- **Collection:** during the measured phase, async-profiler 4.5 is attached to the server process and records 15 s of heap allocations (`-e alloc --total -t`) followed by 15 s of native allocations (`-e nativemem --nofree --total -t`). Netty's direct memory ends up in malloc via `Unsafe.allocateMemory`, so `nativemem` covers it. `--nofree` counts mallocs only, not frees.
- **Normalization:** 15 s × 100k QPS = 1.5 million requests; total bytes are divided by the number of requests.

### Results (per request)

| | adaptive | recycling |
|---|---:|---:|
| Native (malloc), worker threads | **2108.5 B** | **0 B** |
| Native (malloc), whole process | 2116.3 B | 1.8 B |
| Heap, worker threads | 3790.6 B | 3670.0 B |
| Heap, virtual thread carriers | 151.3 B | 151.7 B |
| Heap, whole process | 3941.9 B | 3821.7 B |

### Direct memory

- All of adaptive's 2108 B per request come from one call path: `IoUringBufferRing.fill` (batch refill of the ring) → `AbstractIoUringBufferRingAllocator.allocateBatch` → `AdaptivePoolingAllocator.allocate` → **`allocateFallback`** → `AdaptiveByteBufAllocator$DirectChunkAllocator.allocate` → `Unsafe.allocateMemory`.
- In other words, when the ring is refilled in a batch, `AdaptivePoolingAllocator`'s magazines fail to serve the request and it falls back to mallocing a one-off chunk per buffer, which is freed again when the buffer is released. At 100k QPS that is roughly 210 MB/s malloc'd and freed.
- Recycling does no malloc at all in steady state: buffers are carved out of the pre-allocated regions and reused once returned.
- From the source, `allocateFallback` is only reached when the magazine returns null. Why the magazines return null during these batch refills (2048 buffers at a time) has not been investigated yet.

### Heap

- Worker threads allocate about 120 B less per request, roughly 3%. Most of the difference is in `UnpooledSlicedByteBuf` (266.0 vs 191.9 B) and `CompositeByteBuf$Component` (44.7 vs 26.2 B). A plausible cause: adaptive's buffer size floats between 1 and 4 KB, and with smaller buffers a request takes several recvs, producing extra slices and composite components.
- async-profiler's heap allocation profile is sampled, and some lambda classes have different names in the two runs, so the classes cannot be matched one by one. The 3% should be read as "slightly less" only. This is also what one would expect: recycling mainly saves the direct memory behind the buffers, while adaptive's buffer objects are pooled and reused as well.

### Relation to the QPS results

The ~210 MB/s of malloc/free does not lower adaptive's maximum QPS; the system absorbs that cost. It does put constant pressure on malloc and risks fragmentation, and recycling removes it entirely.

## Caveats

- **One measured run per combination** (plus one rerun for recycling at 5 ms). Differences within ±4% should not be read as one allocator being better.
- **The saturation criterion is strict:** a single pool-busy rejection ends the ramp, so a transient stall near the limit can stop it one step early, as happened in the first recycling 5 ms run. Real saturation is easy to tell apart: P50 jumps from about 5 ms to more than 45 ms.
- **H2 collapses first at saturation:** H1 mostly keeps up with its target while H2 gets many rejections, because H2's in-flight requests share 128 connections and have less pool headroom. It does not mean H2 is processed more slowly.
- **Why threads are no longer pinned:** an earlier setup pinned a single worker thread to one core. Virtual thread carriers created on demand by the worker inherited its CPU affinity and landed on the same core, and loopback softirq processing also ran there. Neither shows up in the worker thread's CPU time, so the worker looked 70% busy while the core was actually saturated. The setup therefore moved to 2 workers with only process-level isolation between server and client.

## Raw data

- Main run: `target/perf/20261006-173507/` (`results.csv`, `max-throughput.csv`, `recycling-stats.csv`, and server and client logs for every combination).
- Rerun of recycling at 5 ms: `target/perf/20261006-175716/`.
- Allocation comparison: `target/perf/20261006-alloc/` (async-profiler collapsed output and the logs of both runs).
- Reproduce with `./run-perf.sh`. Tunable through `WORKERS`, `CLIENT_LOOPS`, `SERVER_CPUS`, `CLIENT_CPUS`, `BLOCKING_MS`, `ALLOCATORS`, `RAMP_START`, `RAMP_STEP` and `RAMP_MAX`.
