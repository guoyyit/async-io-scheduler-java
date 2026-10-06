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

### 读一致性

- **已确认写入立即可读**：写入的 future 一旦成功完成，同一提交方或任何其他提交方
  对该区域的重叠读取必然返回写入后的完整内容，不会返回旧值、全零或新旧混合。
- **预取不破坏一致性**：预取缓存按区间维护失效——写请求在确认完成之前，先把与其
  重叠的所有预取缓存区间（跨全部 stream，因为底层是同一数据文件）作废；预取回填
  携带代际（generation）守卫，回填前若发生过失效则直接丢弃，杜绝并发写下旧数据
  复活。因此同一组读写序列在预取开/关下的可观察结果一致，差异仅限命中率、浪费
  字节与队列等待等性能指标（有专门测试 `prefetchOnAndOffProduceSameObservableResults`
  守护）。
- **并发重叠读写原子性**：与进行中写入重叠的读取只返回完整的写入前内容或完整的
  写入后内容之一。数据文件的读与批次提交在同一把提交锁下串行，批次应用本身不可分，
  不会出现撕裂的中间状态。
- **与写缓冲重叠的读**：读路径发现与仍在写缓冲中的区间重叠时先强制 flush，
  保证 read-your-writes。

### 崩溃恢复

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
- **重启后的读取**：调度器重启后预取缓存为空，读取直接来自持久化数据，
  不会返回重启前缓存的旧值。
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

## 并发准入、背压与过载行为

- 全局待处理上限 `maxPendingRequests`，单提交方上限 `maxPendingPerSubmitter`。
  容量检查与占位在同一把容量锁下原子完成，任何时刻排队深度都有界。
- 容量耗尽时的行为由 `capacityPolicy` 选择：
  - `REJECT`（默认，与历史行为一致）：立即抛 `QueueRejectedException`，
    不入队、不分配 future。异常携带机器可读的 `reason()`：
    - `SCHEDULER_NOT_RUNNING`：调度器未启动或已关闭；
    - `GLOBAL_CAPACITY_EXHAUSTED`：全局待处理上限打满；
    - `SUBMITTER_CAPACITY_EXHAUSTED`：单提交方上限打满；
    - `WAIT_TIMEOUT`：WAIT 模式下等待超时（见下）。
  - `WAIT`：提交线程在容量锁上有限等待，已有请求完成释放容量时被唤醒并
    正常获得受理凭证；等待超过 `capacityWaitTimeout` 后以
    `WAIT_TIMEOUT` 原因的 `QueueRejectedException` 失败。等待中的提交不占用
    待处理名额，因此排队深度仍有界，调用线程也不会无限期阻塞；拒绝路径只抛
    异常，不影响 worker 继续处理已受理请求。
- 所有已受理请求必然到达唯一终态（成功 / 失败 / 关闭时以 `IOException` 完成），
  不会长期悬挂。已受理请求在调度队列中的等待超过 `maxQueueWait` 时以
  `QueueWaitTimeoutException` 失败（默认 60s 兜底；正常由老化机制保证远低于此）。
- 单个提交方的突发或慢速故障最多占用自己的配额，不会拖垮全局吞吐；
  高优先级流量持续存在时，低优先级请求由老化机制（`agingThreshold`）保证在
  可配置上限内获得服务。
- `pendingCount()` / `pendingCount(submitter)` 可查询当前积压。

## 带宽配额

- `quota/QuotaManager` 对每个提交方的读、写各维护一个令牌桶（桶容量约为 1 秒配额，
  限制突发）。
- 通过 `scheduler.setQuota(submitter, IoType, bytesPerSec)` 动态设置；
  默认使用 `scheduler.default-read-quota-bytes-per-sec` 与
  `scheduler.default-write-quota-bytes-per-sec`（默认不限速）。
- 超配额后的动作由 `OverQuotaAction` 决定，可通过
  `scheduler.setOverQuotaAction(...)` 动态调整：
  - `DELAY`（默认）：阻塞 worker 直到攒够令牌（计入 `quotaDelayedRequests`，
    累计等待纳秒计入 `totalQuotaDelayNanos`）；
  - `DEGRADE`：先把该请求降级为 `LOW` 重新排队（计入 `degradedRequests`）；
    若降级后仍超配额，则原地等待令牌（与 DELAY 相同的有界等待），避免围绕
    队列空转形成绕过容量控制的无限重试。降级重排次数超过
    `maxDegradeRequeues`（默认 16）时请求以 `QuotaExceededException` 失败
    （计入 `degradeRequeuesExhausted`），保证任何配置下重试都有界；
  - `REJECT`：请求以 `QuotaExceededException` 异常完成，不占用磁盘带宽。
- 配额造成的延迟会拖慢 worker 消费速度，从而间接占用容量名额；它不会绕过
  `maxPendingRequests` / `maxPendingPerSubmitter` 的有界约束，排队中的请求
  等待超过 `maxQueueWait`（默认 60s）时以 `QueueWaitTimeoutException` 失败，
  杜绝无限悬挂。
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
| `capacityPolicy` | REJECT | 容量耗尽策略：REJECT（立即拒绝）/ WAIT（有限等待） |
| `capacityWaitTimeout` | 5s | WAIT 模式下等待容量的最长时间，超时以 `WAIT_TIMEOUT` 失败 |
| `maxQueueWait` | 60s | 已受理请求在调度队列中的最长等待，超过以 `QueueWaitTimeoutException` 失败 |
| `maxDegradeRequeues` | 16 | DEGRADE 模式下降级重排次数上限，超过以 `QuotaExceededException` 失败 |

指标通过 `scheduler.metrics().snapshot()` 获取，涵盖：

- 提交/完成/失败/拒绝计数：`submittedReads/Writes`、`completedReads/Writes`、
  `failedRequests`、`rejectedRequests`；
- 过载与容量：`rejectedNotRunning`、`rejectedGlobalCapacity`、
  `rejectedSubmitterCapacity`（按原因分类的拒绝计数）、`capacityWaits`、
  `capacityWaitTimeouts`、`totalCapacityWaitNanos`（过载等待时长）、
  `queueWaitTimeouts`；
- 配额：`quotaDelayedRequests`、`totalQuotaDelayNanos`、`degradedRequests`、
  `degradeRequeuesExhausted`；
- 写合并：`writeBatches`、`writeBatchBytes`、`mergedWrites`；
- 预取：`prefetchesIssued`、`prefetchBytesIssued`、`prefetchHits`、
  `prefetchHitBytes`、`prefetchWastedBytes`、`prefetchInvalidatedBytes`
  （因写失效而丢弃的预取字节）、`prefetchBackoffs`。

## 测试覆盖

- `DurableBlockStoreTest`：正常提交、批内重叠覆盖、崩溃窗口后重放、撕裂尾部丢弃、
  CRC 损坏批次识别与截断。
- `AsyncIOSchedulerConcurrencyTest`：重启后已确认写入可读、16 个并发提交方互不阻塞、
  读己之写、全局/单提交方有界队列拒绝、老化防低优先级饿死。
- `ReadConsistencyTest`：顺序读触发预取后对已确认写入区域的重叠读取（必须返回
  新数据）、并发重叠读写的 before/after 原子性、预取开/关可观察结果一致。
- `CapacityPolicyTest`：WAIT 模式容量释放后接纳等待请求、WAIT 超时以
  `WAIT_TIMEOUT` 失败、REJECT 模式拒绝原因可区分（未运行/全局/单提交方）、
  单提交方突发流量下其他提交方不受影响。
- `QuotaCapacityInteractionTest`：配额 DELAY 与 WAIT 容量模式交互（延迟不绕过
  容量约束、等待与延迟均计入指标）、降级重试有界并以 `QuotaExceededException`
  失败、队列等待超过 `maxQueueWait` 以 `QueueWaitTimeoutException` 失败。
- `PrefetchManagerTest`：窗口增长、随机访问退避、命中服务、总量预算有界。
- `WriteCoalescerTest`：相邻合并、重叠后写覆盖、批量上限拆批、linger 上限、
  缓冲区间可见性。
- `QuotaManagerTest` / `SchedulerQuotaIntegrationTest`：DELAY/DEGRADE/REJECT、
  令牌退还、按提交方与方向隔离、指标计数。

## 兼容性约束

- 提交接口（`submitRead` / `submitWrite` 全部重载）、`IoTicket` 受理凭证、
  `IoCompletion` 结果结构与既有配置键全部保持不变，旧调用方无需改动。
- 新增配置项均有明确默认值，且默认值与历史行为一致：`capacityPolicy=REJECT`
  （立即拒绝）、`capacityWaitTimeout=5s`、`maxQueueWait=60s`、
  `maxDegradeRequeues=16`。默认配置下读一致性与崩溃一致性成立。
- `QueueRejectedException` 新增 `reason()` 访问器与带原因的构造器，旧构造器
  保留（原因记为 `UNSPECIFIED`）；`MetricsSnapshot` 保留旧构造器。
- DEGRADE 语义微调：首次超配额仍降级重排，再次超配额改为等待令牌（与 DELAY
  相同的有界等待），可观察差异仅为不再空转重排队。

## 本地验证

```bash
mvn test          # 全部单元与集成测试
mvn -q compile    # 仅编译
```

所有测试均有界自行结束（等待均挂在 future/条件上并带超时，无固定长睡眠），
失败断言直接指明场景与原因。
