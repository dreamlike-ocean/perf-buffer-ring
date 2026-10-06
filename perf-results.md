# io_uring buffer ring allocator 对比：adaptive vs recycling

对比 Netty 自带的 `IoUringAdaptiveBufferRingAllocator` 和 PR [netty/netty#17635](https://github.com/netty/netty/pull/17635) 中的 `IoUringRecyclingBufferRingAllocator`（本地代码与 PR 版本一致）。使用开环爬坡找出各自的最大 QPS，比较相同负载下 worker 线程的 CPU 开销，并用 async-profiler 对比堆和 direct memory 的分配量。

## 结论

- **最大 QPS 相同**：3 种阻塞时间下，两种 allocator 都在 18 万 QPS 通过、20 万 QPS 到顶。到顶时两个 worker 线程合计约 190%–199%，也就是两个 worker 都已跑满，瓶颈在 server。
- **相同负载下的 worker CPU 没有可测出的差异**：差别在 ±4% 以内，正负方向不固定，属于单次测量的噪声范围。
- **延迟相同**：未到顶时，两者的 P99 基本相同，只比业务阻塞时间多出约 0.2–3.7 ms。
- **recycling 没有退化到 fallback**：所有组的 `fallbackAllocations` 和 `foreignThreadAllocations` 都是 0。region 没有被用尽，也没有在非 event loop 线程上分配。
- **recycling 稳定运行时完全不 malloc direct memory**：10 万 QPS 下，adaptive 的 worker 线程平均每个请求 malloc 约 2108 字节（约 210 MB/s 的 direct memory 反复申请和释放），recycling 是 0。堆分配只少了约 3%，在采样误差范围内。详见[分配对比](#分配对比async-profiler)。

在这个 HTTP 场景里，allocator 不是瓶颈：瓶颈在 worker event loop 上 HTTP/H2 的编解码和 io_uring 的收发，allocator 本身的差异不足以影响最大 QPS。

## 测试环境

- **机器和软件**：Intel Core i5-13600KF（6 个 P 核共 12 个超线程，加 8 个 E 核），Linux 7.3.0-6-generic，Oracle GraalVM 25.0.2，Netty 4.2.18.Final。
- **JVM**：server 和 client 都使用 512 MiB 固定堆和 G1，关闭 Netty 泄漏检测，开启 Unsafe。
- **CPU 隔离**：只按进程隔离，进程内的线程不单独绑核。server 运行在 P 核（CPU 0–11）上，client 运行在 E 核（CPU 12–19）上。
- **测试日期**：2026-10-06。

### Server

- **线程**：1 个 acceptor、2 个 worker event loop（`-Dperf.workers=2`），端口 4399 同时接收明文 HTTP/1.1 和 HTTP/2 prior knowledge。
- **io_uring buffer ring**：ring 大小 4096，批量补充 2048 个 buffer，`batchAllocation=true`，开启 multishot recv。
- **adaptive**：buffer 最小和初始 1024 字节，最大 4096 字节，`largeAllocation=true`。
- **recycling**：固定 4096 字节一个 slot，与 adaptive 的最大 buffer 一致，避免两边单次 recv 的粒度不同。in-flight 余量取默认值（ring 大小的 1/4），每个 worker 线程各有一块 region。
- **业务模拟**：收到完整请求后保留引用，交给虚拟线程，用 `LockSupport.parkNanos` 等待 5、10 或 20 ms（`-Dperf.blocking.ms`），模拟 Redis 等阻塞调用。完成后回到 worker 线程，返回 HTTP 200 和固定的 2048 字节 JSON。
- **跨线程释放**：请求在 worker 线程和 acceptor 线程上释放各占约一半，覆盖 recycling 的跨线程归还路径。
- **业务期间持有的 buffer**：H1 的聚合 content 引用 ring 派生出来的 buffer；H2 持有的是 adapter 复制 DATA 帧之后得到的聚合 content。

### Client

- **发压线程**：5 个独立的 NIO event loop，发送时间表在每个 loop 内部按 tick 执行，没有跨线程投递。全局时间表上的第 i 个请求由第 `i % 5` 个 loop 发送。
- **连接**：H1 共 8192 条连接，每条同时只有一个请求；H2 共 128 条连接，每条最多 64 个并发 stream。
- **请求**：POST，请求体和响应体都是 2048 字节。H1 和 H2 按全局序号的奇偶交替，QPS 严格五五开。

## 测试方法：开环爬坡

- **开环**：每档按目标 QPS 严格按时间表发请求，不等上一个请求的响应。时间表的最小粒度是 1 ms，每 1 ms 发出一批到期的请求。
- **爬坡**：从 2 万 QPS 开始，每档增加 2 万。全程第一档之前先不统计地预热 15 秒；每一档先不统计地发 5 秒，再统计 15 秒，然后停止发送，等这一档的请求全部完成，再进入下一档。
- **到顶判定**：满足任意一条就停止加压。
  - 实际 QPS 低于目标的 95%。
  - 出现失败（failed）、连接池满（pool_busy）或未完成请求（unfinished）。
- **最大 QPS**：最后一档满足"实际 QPS ≥ 目标的 95% 且没有任何错误"的目标 QPS。
- **延迟**：P99 从实际发送开始计时；`arrival_p99` 从计划发送时间开始计时，额外包含 client 侧的调度和排队。
- **CPU**：统计窗口前后读取 `/proc` 下的 utime + stime。worker 是所有 `perf-worker-*` 线程合计，client loop 是所有 `perf-client-*` 线程合计，100% 等于一个核。
- **隔离**：每一组（allocator × 阻塞时间）都启动全新的 server 和 client 进程。

## 结果

### 最大 QPS

| 阻塞时间 | adaptive | recycling | 到顶档（20 万）worker CPU（adaptive / recycling） |
|---:|---:|---:|---:|
| 5 ms | 180,000 | 180,000 ¹ | 198.9% / 193.6% |
| 10 ms | 180,000 | 180,000 | 198.8% / 198.8% |
| 20 ms | 180,000 | 180,000 | 198.6% / 186.5% |

¹ recycling + 5 ms 在正式那轮中，于 16 万 QPS 被判定为到顶：0.7% 的请求被拒（pool_busy 11088 次），P99 为 117.5 ms。但这一档 worker 合计只有 156.9%，并没有跑满，看起来像是一次偶发抖动。单独重跑这一组之后，结果与其他组一致：18 万通过，20 万到顶。表中采用重跑的结果，下面的表格中 recycling + 5 ms 也都使用重跑数据。

步长是 2 万，所以真实上限落在 18 万到 20 万之间，这次测试无法区分这个区间内的差异。

### 相同负载下的 worker CPU（两个 worker 合计）

| 阻塞时间 | 负载 | adaptive | recycling | 差异 |
|---:|---:|---:|---:|---:|
| 5 ms | 10 万 | 94.3% | 94.6% | +0.3% |
| 5 ms | 14 万 | 129.8% | 127.9% | −1.5% |
| 5 ms | 18 万 | 165.5% | 166.3% | +0.5% |
| 10 ms | 10 万 | 104.9% | 98.4% | −6.2% |
| 10 ms | 14 万 | 138.4% | 134.6% | −2.7% |
| 10 ms | 18 万 | 168.5% | 165.9% | −1.5% |
| 20 ms | 10 万 | 98.4% | 98.5% | +0.1% |
| 20 ms | 14 万 | 138.3% | 139.2% | +0.7% |
| 20 ms | 18 万 | 169.6% | 165.8% | −2.2% |

差异 = recycling 相对 adaptive 的变化。worker CPU 大致随 QPS 线性增长，每 1 万 QPS 约占一个核的 9%–10%。10 ms 那组 10 万档 −6.2% 是唯一超过 ±3% 的点，同一组其他负载下都在 ±3% 以内，因此判断为单次测量的噪声。

### 18 万 QPS（最后一档通过）的延迟和进程 CPU

| 阻塞时间 | P99 adaptive / recycling | server 进程 CPU adaptive / recycling | client loop CPU adaptive / recycling |
|---:|---:|---:|---:|
| 5 ms | 7.370 / 7.169 ms | 359.0% / 352.1% | 383.3% / 379.7% |
| 10 ms | 12.594 / 12.199 ms | 350.4% / 346.4% | 408.6% / 410.0% |
| 20 ms | 23.739 / 23.194 ms | 359.0% / 338.6% | 405.9% / 386.8% |

server 进程 CPU 包括 worker、acceptor、虚拟线程 carrier、GC 和 JIT。client 的 5 个 loop 合计约 380%–410%，平均每个约 80%，仍有余量，所以到顶的是 server。

### recycling 的计数

| 阻塞时间 | fallbackAllocations | foreignThreadAllocations |
|---:|---:|---:|
| 5 ms | 0 | 0 |
| 10 ms | 0 | 0 |
| 20 ms | 0 | 0 |

## 分配对比（async-profiler）

### 方法

- **负载**：固定 10 万 QPS、阻塞 5 ms，其他配置与上文相同，两种 allocator 各跑一次，都稳定在 10 万 QPS，没有错误。
- **采集**：进入统计阶段后，用 async-profiler 4.5 attach 到 server 进程，先采 15 秒堆分配（`-e alloc --total -t`），再采 15 秒 native 内存（`-e nativemem --nofree --total -t`）。netty 的 direct memory 最终通过 `Unsafe.allocateMemory` 调用 malloc 分配，所以能被 `nativemem` 统计到。`--nofree` 表示只统计 malloc，不统计 free。
- **换算**：15 秒 × 10 万 QPS = 150 万个请求，用总字节数除以请求数，得到每个请求的分配量。

### 结果（平均每个请求）

| | adaptive | recycling |
|---|---:|---:|
| native（malloc），worker 线程 | **2108.5 B** | **0 B** |
| native（malloc），整个进程 | 2116.3 B | 1.8 B |
| 堆，worker 线程 | 3790.6 B | 3670.0 B |
| 堆，虚拟线程 carrier | 151.3 B | 151.7 B |
| 堆，整个进程 | 3941.9 B | 3821.7 B |

### direct memory

- adaptive 的 2108 B/请求全部来自同一条调用路径：`IoUringBufferRing.fill`（批量补充 ring）→ `AbstractIoUringBufferRingAllocator.allocateBatch` → `AdaptivePoolingAllocator.allocate` → **`allocateFallback`** → `AdaptiveByteBufAllocator$DirectChunkAllocator.allocate` → `Unsafe.allocateMemory`。
- 也就是说，ring 批量补充 buffer 时，`AdaptivePoolingAllocator` 的 magazine 没能分配成功，退回到 fallback：每个 buffer 单独 malloc 一块一次性的 chunk，buffer 释放时再 free。按 10 万 QPS 换算，大约每秒 malloc 和 free 各 210 MB。
- recycling 在稳定运行时没有任何 malloc：buffer 都从预先申请好的 region 里切出，归还后直接复用。
- 从源码看，`allocateFallback` 只在 magazine 返回 null 时才会被调用。这次批量补充（一次 2048 个 buffer）时 magazine 为什么会返回 null，还没有查清楚。

### 堆

- worker 线程每个请求少分配约 120 B，大约 3%。差异主要在 `UnpooledSlicedByteBuf`（266.0 对 191.9 B）和 `CompositeByteBuf$Component`（44.7 对 26.2 B）。一个可能的原因是：adaptive 的 buffer 大小在 1–4 KB 之间浮动，buffer 小的时候一个请求需要多次 recv，多出了 slice 和 composite 组件。
- async-profiler 的堆分配是采样统计，部分 lambda 类在两次运行中的类名也不同，无法逐项对应。所以这 3% 只能视为"略少"。这也符合预期：recycling 节省的主要是 buffer 背后的 direct memory，而 adaptive 的 buffer 对象本身也是池化复用的。

### 与 QPS 结果的关系

每秒约 210 MB 的 malloc/free 没有拉低 adaptive 的最大 QPS，这部分开销被系统吸收了。但它会给 malloc 带来持续压力，也有内存碎片的风险。recycling 把这部分开销完全消除了。

## 注意事项

- **每组只有一次正式测量**（recycling + 5 ms 额外重跑了一次）。表中 ±4% 以内的差异不应解读为两者有优劣之分。
- **到顶判定偏严格**：只要有一个请求因为连接池满被拒，这一档就会被判定为到顶。边界附近可能因为一次偶发抖动提前一档停下，recycling + 5 ms 第一次运行就是这种情况。真正到顶时，P50 会从约 5 ms 跳到 45 ms 以上，两者很容易区分。
- **到顶时 H2 先崩**：到顶时 H1 基本还能维持目标 QPS，H2 则被大量拒绝。这是因为 H2 的在途请求集中在 128 条连接上，连接池余量更小，并不说明 H2 的处理更慢。
- **为什么不再绑核**：之前的测试只把一个 worker 线程绑到单个核上，有两个问题。一是由 worker 按需创建的虚拟线程 carrier 会继承它的 CPU 亲和性，挤到同一个核上；二是 loopback 的软中断也在这个核上执行。这两部分都不计入 worker 线程的 CPU 时间，导致 worker 显示只有 70%、实际上整个核已经跑满。所以改成 2 个 worker，并只按进程隔离 server 和 client。

## 原始数据

- 正式测量：`target/perf/20261006-173507/`（`results.csv`、`max-throughput.csv`、`recycling-stats.csv`，以及各组的 server 和 client 日志）。
- recycling + 5 ms 重跑：`target/perf/20261006-175716/`。
- 分配对比：`target/perf/20261006-alloc/`（async-profiler 的 collapsed 输出，以及两次压测的日志）。
- 复现：`./run-perf.sh`。可用环境变量 `WORKERS`、`CLIENT_LOOPS`、`SERVER_CPUS`、`CLIENT_CPUS`、`BLOCKING_MS`、`ALLOCATORS`、`RAMP_START`、`RAMP_STEP`、`RAMP_MAX` 调整。
