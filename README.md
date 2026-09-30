# async-io-scheduler-java

一个本地异步 IO 调度与缓冲层。向多个并发提交方提供统一的块读写入口，把请求
提交与磁盘执行解耦；后台单线程调度器负责优先级/公平选择、配额限流、顺序预取、
相邻小写入合并与 WAL 两阶段落盘。

## 快速开始

```java
SchedulerConfig config = new SchedulerConfig();
try (AsyncIoScheduler scheduler =
             new DefaultAsyncIoScheduler(Path.of("./iodata"), config)) {
    Ticket<Void> write = scheduler.submitWrite(
            "order-service", Priority.HIGH, 0, ByteBuffer.wrap(payload));
    write.get();                       // 返回后写入已持久化，崩溃不丢

    Ticket<Integer> read = scheduler.submitRead(
            "order-service", Priority.NORMAL, 0, ByteBuffer.allocate(4096));
    int bytes = read.get();
}
```

提交方法绝不执行磁盘 IO，立即返回 `Ticket`（受理凭证）。后台线程异步执行；
可通过 `Ticket.get` 阻塞、`Ticket.asFuture()` 组合或轮询 `isDone()`。

## 架构

- `api.AsyncIoScheduler`：统一入口；`DefaultAsyncIoScheduler` 为唯一实现。
- `queue.PendingRequestQueue`：全局 + 每提交方双上限的有界多优先级待处理队列。
- `scheduling`：`PrioritySelector` 策略，含严格优先级与老化优先级两种实现。
- `quota.QuotaManager`：按提交方的固定窗口带宽配额。
- `prefetch`：`SequentialDetector` 顺序/随机识别，`PrefetchManager` 预算与度量。
- `buffer`：`BlockCache`（LRU + 预取标记）与 `WriteCoalescer`（合并/批量）。
- `storage`：`BlockStorage` 块存储、`WriteAheadLog` 两阶段日志、`FileDurableChannel`。

## 一致性与持久性语义

采用 **write-ahead log 两阶段提交**，每个写入批次：

1. 将批次内全部块以一个或多个 `DATA` 记录追加到 WAL 并 `force`（含元数据）；
2. 追加 `COMMIT` 记录并 `force`；
3. 把批次写入数据文件并 `force(true)`；
4. 至此才完成提交方的写入 `Ticket`；
5. 截断已应用的 WAL 前缀。

**确认点即持久性边界**：`submitWrite` 的 `Ticket` 完成只发生在第 3 步
`force` 成功之后。因此即使进程在确认返回后立即崩溃/掉电（文件系统遵守
`fsync` 语义的前提下），写入也完整可读。

崩溃恢复规则：

- 有 `COMMIT` 的批次：在重启时按批次号顺序重放到数据文件（重放幂等），随后截断 WAL；
- 只写了 `DATA` 没有 `COMMIT` 的未完成批次：整体识别为“未确认”，其帧被截断丢弃，
  对读完全不可见，绝不出现部分块；
- WAL 尾部 CRC 校验失败、magic 不符或帧被截断：一律视为撕裂尾部并丢弃；
- 已确认数据（含此前的批次）不受任何未完成/损坏尾部影响。

单批次内部的多个块是原子的：要么整批可见，要么整批不可见。

## 调度、优先级与公平性

- `fairScheduling=true`（默认）：使用 `AgingPrioritySelector`。请求等待时间每超过
  `starvationAgeThreshold` 就提升一个有效优先级，因此持续的高优先级流量无法长期
  饿死低优先级请求（老化到与高优先级同级后按请求 ID 先到先得）。
- `fairScheduling=false`：使用 `StrictPrioritySelector`，严格高优先级优先、同级 FIFO；
  此模式下低优先级可能被长期饿死，仅建议在受控场景使用。
- 读写突发不互相抢占：预取仅在“没有任何待处理用户读、没有待处理写、没有配额延迟
  请求、且写合并缓冲区为空”时才会发起，因此预取永远排在写请求与低优先级用户读之后。

## 写合并与批量落盘

- 同一批次中对同一块的重复写就地覆盖（只保留最后内容），合并次数计入
  `mergedWrites` 指标；
- 相邻/任意脏块按块号升序组成一批，满足以下任一条件即落盘：
  - 脏块数达到 `maxWriteBatchBlocks`；
  - 最老脏块等待时间达到 `maxBatchDelay`。
- 部分块写走读-改-写（RMW），与块内已有未覆盖字节合并后落盘。

## 预取

- 每个“提交方 + 地址区域”维护一个 `SequentialDetector`：连续前向跨步视为顺序，
  窗口按 `start → 2× → … → max` 增长；非前向跳跃累计达到 `prefetchRandomThreshold`
  即进入随机模式，窗口退避为 0；超过 `prefetchIdleReset` 无访问则复位。
- 全局预取驻留总量受 `prefetchTotalBudgetBlocks` 约束。
- 可度量：`prefetchIssuedBlocks`（发起）、`prefetchHitBlocks`（被用户读命中）、
  `prefetchWastedBlocks`（淘汰前从未被消费）。

## 队列背压与隔离

- 全局上限 `maxPendingRequests`，每提交方上限 `maxPendingPerSubmitter`，单个慢/故障
  提交方无法占满队列拖垮整体。
- `overflowPolicy=BLOCK`（默认）：提交线程在有界条件上等待，仍立即受理由调度完成，
  其他提交方继续提交不受影响；
- `overflowPolicy=REJECT`：队列满时快速失败（`QueueRejectedException`）并计入
  `rejectedRequests`。

## 配额限流

- `setQuota(submitter, readBytesPerSec, writeBytesPerSec)` 按提交方设置读写带宽；
  `0` 表示不限。也可用全局默认 `readQuotaBytesPerSec/writeQuotaBytesPerSec`。
- `quotaAction=DELAY`（默认）：超配额请求保留在调度侧、窗口刷新后自动继续，不占用
  提交方的队列名额，且不阻塞其他提交方；
- `quotaAction=REJECT`：超配额立即以 `QuotaExceededException` 失败该凭证。
- 用量通过 `quotaSnapshot(submitter)` 查询（限额、窗口内已用量、被延迟次数）。

## 可观测指标（metricsSnapshot）

| 指标 | 含义 |
| --- | --- |
| `pendingReads` / `pendingWrites` | 待处理读/写请求数 |
| `completedReads` / `completedWrites` | 已完成请求数 |
| `rejectedRequests` | 队列满被拒绝次数 |
| `mergedWrites` | 被合并/就地覆盖的写入次数 |
| `flushedBatches` | 已落盘批次数 |
| `prefetchIssuedBlocks` / `HitBlocks` / `WastedBlocks` | 预取发起/命中/浪费块数 |
| `quotaDelayedRequests` | 因配额被延迟或拒绝的次数 |
| `recoveredBatches` / `discardedBatches` | 启动时恢复/丢弃的批次数 |

## 配置项（SchedulerConfig）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `blockSize` | `4096` | 块大小（字节），WAL/缓存/合并的基本单位 |
| `maxPendingRequests` | `1024` | 全局待处理请求上限 |
| `maxPendingPerSubmitter` | `256` | 单提交方待处理上限 |
| `overflowPolicy` | `BLOCK` | 队列满策略：`BLOCK` / `REJECT` |
| `maxWriteBatchBlocks` | `32` | 单批次最大脏块数，达到即落盘 |
| `maxBatchDelay` | `20ms` | 最老脏块等待上限，达到即落盘 |
| `cacheBlocks` | `4096` | 块缓存容量（块数，LRU） |
| `prefetchStartBlocks` | `2` | 顺序预取初始窗口 |
| `prefetchMaxWindowBlocks` | `16` | 预取窗口上限 |
| `prefetchTotalBudgetBlocks` | `256` | 全局预取驻留总量预算 |
| `prefetchRandomThreshold` | `3` | 连续随机访问多少次后退避为 0 |
| `prefetchIdleReset` | `100ms` | 流空闲多久后复位检测状态 |
| `fairScheduling` | `true` | 是否启用老化公平调度 |
| `starvationAgeThreshold` | `50ms` | 每等待多久提升一个有效优先级 |
| `readQuotaBytesPerSec` | `0` | 默认读带宽配额（0 不限） |
| `writeQuotaBytesPerSec` | `0` | 默认写带宽配额（0 不限） |
| `quotaAction` | `DELAY` | 超配额策略：`DELAY` / `REJECT` |
| `quotaWindow` | `1s` | 配额固定窗口长度（限额按此窗口折算） |

## 测试

`mvn test` 覆盖：多提交方并发竞争、防优先级饿死、顺序/随机预取退避与命中浪费、
写合并边界、有界队列背压、配额延迟/拒绝/隔离、WAL 未完成批次丢弃与端到端崩溃恢复。

## 边界与假设

- 持久性依赖底层文件系统正确实现 `force(true)`（等价 `fsync` 含元数据）。
- 单数据文件 + 单 WAL，单机本地存储；未实现跨目录分片或多盘并行。
- 调度为单后台线程，目标是可预测的顺序化与合并，而非多盘并行吞吐。
