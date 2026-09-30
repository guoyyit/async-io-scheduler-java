# 本地异步 IO 调度与缓冲层

`AsyncIOScheduler` 为多个并发组件提供统一的块读写入口，把"请求提交"与"磁盘执行"解耦，
并集成带优先级与防饿死的调度器、自适应预取、相邻小写入合并批量落盘、按提交方/方向的
带宽配额，以及基于追加日志（journal）的崩溃恢复。

## 快速上手

```java
SchedulerConfig config = new SchedulerConfig();
try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
        config, Path.of("data.bin"), Path.of("data.journal"))) {
    scheduler.start();

    IoTicket ticket = scheduler.submitWrite("order-service", 4096, payload);
    IoCompletion done = ticket.completion().get();   // 确认返回时已 crash-safe

    IoTicket read = scheduler.submitRead("order-service", 4096, payload.length);
    byte[] bytes = read.completion().get().data();
}
```

提交方法立即返回 `IoTicket`（受理凭证），其中的
`CompletableFuture<IoCompletion>` 在后台 worker 执行完成后完成；提交线程本身不做磁盘 IO，
只做有界准入检查，因此提交方之间互不阻塞。

## 一致性与崩溃恢复语义

- 数据文件 + 追加日志（`storage/DurableBlockStore`、`journal/Journal`）。
- 每个写批次的落盘顺序固定为：
  1. 构造批次记录（magic、序号、各段 offset/长度/数据、CRC32C），追加写入 journal 并
     `FileDescriptor.sync()` 强制到稳定存储；
  2. 把批次各段写入数据文件并再次 `sync()`；
  3. 成功后清空 journal。
- **写入确认时机**：只有在第 2 步完成后才完成提交方的 future。因此一旦写入被确认，
  即使进程立刻崩溃，重启后数据也一定完整可读。
- **未确认写入**：只可能处于"日志已 fsync、数据文件尚未更新"的窗口。恢复时整批重放，
  满足"要么完整生效、要么完全不可见"，不会出现部分写入。
- **损坏识别**：journal 中每个批次都带 CRC32C 与尾部提交标记。恢复顺序扫描：
  - CRC 不匹配 / 缺少提交标记 / 长度非法 → 视为撕裂或垃圾尾部，从该位置起全部丢弃并截断；
  - 此前已确认的数据（在数据文件中或位于更早的完整批次中）不受影响。
- 批次内多个 segment 按 offset 排序后依次应用；重叠段以后写覆盖先写（last-write-wins）。

journal 单批次记录布局（小端序）：

```
int  magic = 0x4A425431
long sequence
int  segmentCount
重复 segmentCount 次: long offset, int length, byte[length]
int  crc32c        // 覆盖 magic 到各段数据的全部字节
int  commitMarker  // 0x434F4D49
```

## 调度、优先级与公平性

- 每个优先级（`HIGH/NORMAL/LOW`）一个 FIFO 子队列（`scheduling/PriorityRequestQueue`）。
- `FairnessPolicy` 三种可配置策略：
  - `STRICT_PRIORITY`：永远先服务高优先级；低优先级可能长期饿死（仅建议压测使用）。
  - `AGING_STRICT`（默认）：严格优先级 + 老化。请求等待超过
    `scheduler.aging-threshold` 后被临时提升到最高优先级参与选择，保证高优先级不长期
    饿死低优先级，同时不破坏短时的优先级响应。
  - `WEIGHTED_FAIR`：按 `high-weight/normal-weight/low-weight` 做加权轮转，
    每个优先级都能稳定获得服务份额。
- worker 数量由 `scheduler.io-workers` 配置；读路径与写路径在同一调度器下并存，
  且预取只在"当前没有待处理写、也没有排队请求"时才由 worker 顺带发起，
  因此预取不会抢占写请求与低优先级读。
- 读请求若与仍在写缓冲中的区间重叠，会先强制 flush 对应批次，保证 read-your-writes。

## 写合并与批量落盘

- `scheduling/WriteCoalescer` 维护按 offset 排序的写缓冲，专用落盘线程在以下任一条件
  满足时 flush：
  - 缓冲字节数达到 `scheduler.max-batch-bytes`；
  - 最老写入的等待时间达到 `scheduler.batch-linger-ms`（等待延迟上限）；
  - 重叠读或关闭时显式 `flush()`。
- flush 时把相邻或重叠的 segment 合并成尽量少的连续区间，超过批量上限则拆成多批。
- 合并效果可通过指标度量：`writeBatches`（批次数）、`writeBatchBytes`（journal 字节）、
  `mergedWrites`（被合并掉的写入条数）。

## 顺序读检测与自适应预取

- 预取状态按 `streamId` 独立维护（`prefetch/PrefetchManager`）。
- 连续顺序读达到 `scheduler.sequential-hit-threshold` 次后开始预读，窗口从
  `scheduler.prefetch-min-bytes` 起步，每命中一次按 `scheduler.prefetch-growth-factor`
  增长，上限为 `scheduler.prefetch-max-bytes`。
- 出现非顺序的随机跳转时立即停止本轮预取，并把窗口乘以
  `scheduler.prefetch-backoff-factor` 退避（不低于最小窗口），退避次数计入
  `prefetchBackoffs`。
- 全部预取缓存总字节数受 `scheduler.prefetch-total-budget-bytes` 约束，按 LRU 淘汰；
  被淘汰而从未被用户读取的字节计入 `prefetchWastedBytes`，命中计入
  `prefetchHits/prefetchHitBytes`，发起量计入 `prefetchesIssued/prefetchBytesIssued`。

## 并发准入与背压

- 全局待处理上限 `scheduler.max-pending-requests`，单提交方上限
  `scheduler.max-pending-per-submitter`。两者任一达到上限，新的提交立即抛
  `QueueRejectedException`（有界排队 / 拒绝策略），不入队、不分配 future。
- 单个提交方的突发或慢速故障最多占用自己的配额，不会拖垮全局吞吐。
- `pendingCount()` / `pendingCount(submitter)` 可查询当前积压。

## 带宽配额

- `quota/QuotaManager` 对每个提交方的读、写各维护一个令牌桶（桶容量约为 1 秒配额，
  限制突发）。
- 通过 `scheduler.setQuota(submitter, IoType, bytesPerSec)` 动态设置；
  默认使用 `scheduler.default-read-quota-bytes-per-sec` 与
  `scheduler.default-write-quota-bytes-per-sec`（默认不限速）。
- 超配额后的动作由 `OverQuotaAction` 决定，可通过
  `scheduler.setOverQuotaAction(...)` 动态调整：
  - `DELAY`（默认）：阻塞 worker 直到攒够令牌（计入 `quotaDelayedRequests`）；
  - `DEGRADE`：立即服务但把该请求降级为 `LOW` 优先级（计入 `degradedRequests`）；
  - `REJECT`：请求以 `QuotaExceededException` 异常完成，不占用磁盘带宽。
- `quotas().snapshots()` / `snapshot(submitter, type)` 返回限速值、估算消耗速率、
  可用令牌与累计延迟次数。

## 全部配置项（`config/SchedulerConfig`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `blockSize` | 4096 | 逻辑块大小（字节） |
| `maxPendingRequests` | 1024 | 全局待处理请求上限 |
| `maxPendingPerSubmitter` | 256 | 单提交方待处理上限 |
| `ioWorkers` | 2 | 后台 IO worker 线程数 |
| `shutdownTimeout` | 10s | 关闭时等待 worker 的最长时间 |
| `fairnessPolicy` | AGING_STRICT | STRICT_PRIORITY / AGING_STRICT / WEIGHTED_FAIR |
| `agingThreshold` | 500ms | AGING_STRICT 下等待多久后提升优先级 |
| `highWeight` | 4.0 | WEIGHTED_FAIR 高优先级权重 |
| `normalWeight` | 2.0 | WEIGHTED_FAIR 普通优先级权重 |
| `lowWeight` | 1.0 | WEIGHTED_FAIR 低优先级权重 |
| `maxBatchBytes` | 65536 | 单落盘批次的最大字节数 |
| `batchLinger` | 5ms | 写批次等待延迟上限 |
| `prefetchEnabled` | true | 是否启用预取 |
| `prefetchMinBytes` | 4096 | 预取窗口下限 |
| `prefetchMaxBytes` | 65536 | 预取窗口上限 |
| `prefetchTotalBudgetBytes` | 1048576 | 预取缓存总字节预算 |
| `prefetchGrowthFactor` | 2.0 | 顺序命中时窗口增长倍数 |
| `prefetchBackoffFactor` | 0.5 | 随机访问时窗口退避倍数 |
| `sequentialHitThreshold` | 2 | 连续顺序读多少次后开始预取 |
| `overQuotaAction` | DELAY | 超配额动作：DELAY / DEGRADE / REJECT |
| `defaultReadQuotaBytesPerSec` | 无限 | 提交方读配额默认值（字节/秒） |
| `defaultWriteQuotaBytesPerSec` | 无限 | 提交方写配额默认值（字节/秒） |

指标通过 `scheduler.metrics().snapshot()` 获取，涵盖提交/完成/失败/拒绝/降级/延迟计数、
写批次与合并计数、预取发起/命中/浪费/退避计数。

## 测试覆盖

- `DurableBlockStoreTest`：正常提交、批内重叠覆盖、崩溃窗口后重放、撕裂尾部丢弃、
  CRC 损坏批次识别与截断。
- `AsyncIOSchedulerConcurrencyTest`：重启后已确认写入可读、16 个并发提交方互不阻塞、
  读己之写、全局/单提交方有界队列拒绝、老化防低优先级饿死。
- `PrefetchManagerTest`：窗口增长、随机访问退避、命中服务、总量预算有界。
- `WriteCoalescerTest`：相邻合并、重叠后写覆盖、批量上限拆批、linger 上限、
  缓冲区间可见性。
- `QuotaManagerTest` / `SchedulerQuotaIntegrationTest`：DELAY/DEGRADE/REJECT、
  令牌退还、按提交方与方向隔离、指标计数。
