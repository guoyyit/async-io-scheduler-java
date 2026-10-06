package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.OverQuotaAction;
import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.io.IoPriority;
import com.github.guoyyit.asyncioschedulerjava.io.IoCompletion;
import com.github.guoyyit.asyncioschedulerjava.io.IoRequest;
import com.github.guoyyit.asyncioschedulerjava.io.IoTicket;
import com.github.guoyyit.asyncioschedulerjava.io.IoType;
import com.github.guoyyit.asyncioschedulerjava.io.QueueRejectedException;
import com.github.guoyyit.asyncioschedulerjava.io.WriteSegment;
import com.github.guoyyit.asyncioschedulerjava.prefetch.PrefetchManager;
import com.github.guoyyit.asyncioschedulerjava.prefetch.PrefetchPlan;
import com.github.guoyyit.asyncioschedulerjava.quota.QuotaExceededException;
import com.github.guoyyit.asyncioschedulerjava.quota.QuotaManager;
import com.github.guoyyit.asyncioschedulerjava.scheduling.PriorityRequestQueue;
import com.github.guoyyit.asyncioschedulerjava.scheduling.SchedulerMetrics;
import com.github.guoyyit.asyncioschedulerjava.scheduling.WriteCoalescer;
import com.github.guoyyit.asyncioschedulerjava.storage.DurableBlockStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unified asynchronous block read/write entry point.
 *
 * <p>Submission never blocks on disk IO: it returns an {@link IoTicket}
 * immediately. Background workers perform scheduling, prefetching, write
 * coalescing, quota enforcement and crash-consistent batching.
 */
public class AsyncIOScheduler implements AutoCloseable {

    private static final String DEFAULT_STREAM = "default";

    private final SchedulerConfig config;
    private final Path dataFile;
    private final Path journalFile;

    private final SchedulerMetrics metrics = new SchedulerMetrics();
    private final AtomicLong requestIds = new AtomicLong();
    private final Map<Long, CompletableFuture<IoCompletion>> pending = new ConcurrentHashMap<>();
    private final AtomicInteger totalPending = new AtomicInteger();
    private final Map<String, AtomicInteger> perSubmitterPending = new ConcurrentHashMap<>();

    private DurableBlockStore store;
    private PriorityRequestQueue queue;
    private WriteCoalescer coalescer;
    private PrefetchManager prefetcher;
    private QuotaManager quotaManager;
    private volatile List<Thread> workers = List.of();
    private volatile boolean running;

    public AsyncIOScheduler(SchedulerConfig config, Path dataFile, Path journalFile) {
        this.config = config;
        this.dataFile = dataFile;
        this.journalFile = journalFile;
    }

    public synchronized void start() throws IOException {
        if (running) {
            return;
        }
        store = new DurableBlockStore(dataFile, journalFile, config.getBlockSize());
        store.open();
        queue = new PriorityRequestQueue(config.getMaxPendingRequests());
        quotaManager = new QuotaManager(config.getDefaultReadQuotaBytesPerSec(),
                config.getDefaultWriteQuotaBytesPerSec(), config.getOverQuotaAction());
        prefetcher = new PrefetchManager(config, metrics);
        coalescer = new WriteCoalescer(store, config.getMaxBatchBytes(), config.getBatchLinger(),
                (journalBytes, mergedAway) -> {
                    metrics.writeBatches++;
                    metrics.writeBatchBytes += journalBytes;
                    metrics.mergedWrites += mergedAway;
                },
                this::finishWrite);
        coalescer.setCommittedBatchListener(segments -> {
            for (WriteSegment segment : segments) {
                prefetcher.invalidate(segment.offset(), segment.length());
            }
        });

        running = true;
        List<Thread> created = new ArrayList<>();
        for (int i = 0; i < Math.max(1, config.getIoWorkers()); i++) {
            Thread worker = new Thread(this::workerLoop, "io-worker-" + i);
            worker.setDaemon(true);
            worker.start();
            created.add(worker);
        }
        workers = created;
    }

    public IoTicket submitRead(String submitter, long offset, int length) {
        return submitRead(submitter, DEFAULT_STREAM, IoPriority.NORMAL, offset, length);
    }

    public IoTicket submitRead(String submitter, String streamId, IoPriority priority,
                               long offset, int length) {
        return accept(submitter, streamId, priority, IoType.READ, offset, length, null);
    }

    public IoTicket submitWrite(String submitter, long offset, byte[] data) {
        return submitWrite(submitter, DEFAULT_STREAM, IoPriority.NORMAL, offset, data);
    }

    public IoTicket submitWrite(String submitter, String streamId, IoPriority priority,
                                long offset, byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("write data must not be null");
        }
        return accept(submitter, streamId, priority, IoType.WRITE, offset, data.length,
                ByteBuffer.wrap(data.clone()));
    }

    public int pendingCount() {
        return totalPending.get();
    }

    public int pendingCount(String submitter) {
        AtomicInteger counter = perSubmitterPending.get(submitter);
        return counter == null ? 0 : counter.get();
    }

    public SchedulerMetrics metrics() {
        return metrics;
    }

    public QuotaManager quotas() {
        return quotaManager;
    }

    public void setQuota(String submitter, IoType type, long bytesPerSec) {
        quotaManager.setQuota(submitter, type, bytesPerSec);
    }

    public void setOverQuotaAction(OverQuotaAction action) {
        quotaManager.setOverQuotaAction(action);
    }

    @Override
    public synchronized void close() throws IOException {
        if (!running) {
            return;
        }
        running = false;
        queue.close();
        for (Thread worker : workers) {
            worker.interrupt();
            try {
                worker.join(config.getShutdownTimeout().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        IOException failure = null;
        try {
            coalescer.close();
        } catch (IOException e) {
            failure = e;
        }
        try {
            prefetcher.close();
        } catch (RuntimeException ignored) {
            // best effort
        }
        try {
            store.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            }
        }
        for (Map.Entry<Long, CompletableFuture<IoCompletion>> entry : pending.entrySet()) {
            entry.getValue().complete(IoCompletion.failed(entry.getKey(),
                    new IOException("scheduler shut down")));
        }
        pending.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private IoTicket accept(String submitter, String streamId, IoPriority priority,
                            IoType type, long offset, int length, ByteBuffer data) {
        if (!running) {
            throw new QueueRejectedException("scheduler is not running");
        }
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        AtomicInteger submitterCounter =
                perSubmitterPending.computeIfAbsent(submitter, key -> new AtomicInteger());
        if (totalPending.get() >= config.getMaxPendingRequests()
                || submitterCounter.get() >= config.getMaxPendingPerSubmitter()) {
            metrics.rejectedRequests++;
            throw new QueueRejectedException("pending request limit reached for submitter '"
                    + submitter + "'");
        }
        long id = requestIds.incrementAndGet();
        CompletableFuture<IoCompletion> future = new CompletableFuture<>();
        pending.put(id, future);
        totalPending.incrementAndGet();
        submitterCounter.incrementAndGet();
        IoRequest request = new IoRequest(id, type, priority, submitter, streamId,
                offset, length, data, System.nanoTime());
        if (type == IoType.READ) {
            metrics.submittedReads++;
        } else {
            metrics.submittedWrites++;
        }
        queue.put(request);
        return new IoTicket(id, future);
    }

    private void workerLoop() {
        while (running) {
            IoRequest request;
            try {
                request = queue.poll(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS,
                        config.getFairnessPolicy(), config.getAgingThreshold().toNanos(),
                        config.getHighWeight(), config.getNormalWeight(), config.getLowWeight());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (request == null) {
                continue;
            }
            try {
                dispatch(request);
            } catch (Throwable error) {
                failRequest(request, error);
            }
        }
    }

    private void dispatch(IoRequest request) throws Exception {
        OverQuotaAction action = quotaManager.reserve(request.submitter(), request.type(),
                Math.max(1, request.length()));
        if (action == OverQuotaAction.DELAY) {
            metrics.quotaDelayedRequests++;
        } else if (action == OverQuotaAction.DEGRADE) {
            metrics.degradedRequests++;
            requeueDegraded(request);
            return;
        } else if (action == OverQuotaAction.REJECT) {
            failRequest(request, new QuotaExceededException(
                    "read/write quota exhausted for submitter '" + request.submitter() + "'"));
            return;
        }
        long waitNanos = System.nanoTime() - request.enqueueNanos();
        metrics.totalQueueWaitNanos += waitNanos;
        if (request.type() == IoType.WRITE) {
            coalescer.add(request);
        } else {
            executeRead(request);
        }
    }

    private void requeueDegraded(IoRequest request) {
        IoRequest demoted = new IoRequest(request.id(), request.type(), IoPriority.LOW,
                request.submitter(), request.streamId(), request.offset(), request.length(),
                request.data(), request.enqueueNanos());
        queue.put(demoted);
    }

    private void executeRead(IoRequest request) {
        try {
            if (!coalescer.overlapping(request.offset(), request.length()).isEmpty()) {
                coalescer.flush();
            }
        } catch (IOException | RuntimeException error) {
            quotaManager.refund(request.submitter(), IoType.READ, request.length());
            failRequest(request, error);
            return;
        }
        ByteBuffer cached = prefetcher.lookup(request.streamId(), request.offset(), request.length());
        if (cached != null) {
            succeedRequest(request, cached);
            return;
        }
        try {
            ByteBuffer data = store.read(request.offset(), request.length());
            succeedRequest(request, data);
            maybePrefetch(request);
        } catch (IOException | RuntimeException error) {
            quotaManager.refund(request.submitter(), IoType.READ, request.length());
            failRequest(request, error);
        }
    }

    private void maybePrefetch(IoRequest userRead) {
        if (coalescer.pendingCount() > 0 || !queue.isEmpty()) {
            return;
        }
        PrefetchPlan plan = prefetcher.afterUserRead(
                userRead.streamId(), userRead.offset(), userRead.length());
        if (plan.isEmpty()) {
            return;
        }
        if (!quotaManager.tryReserve(userRead.submitter(), IoType.READ, plan.length())) {
            return;
        }
        long version = prefetcher.invalidationVersion();
        try {
            ByteBuffer prefetched = store.read(plan.offset(), plan.length());
            prefetcher.fill(userRead.streamId(), plan.offset(), prefetched, version);
        } catch (IOException | RuntimeException error) {
            quotaManager.refund(userRead.submitter(), IoType.READ, plan.length());
        }
    }

    private void finishWrite(IoRequest request, Throwable error) {
        if (error == null) {
            metrics.completedWrites++;
            complete(request, IoCompletion.ok(request.id()));
        } else {
            failRequest(request, error);
        }
    }

    private void succeedRequest(IoRequest request, ByteBuffer data) {
        if (request.type() == IoType.READ) {
            metrics.completedReads++;
        }
        complete(request, IoCompletion.read(request.id(), data));
    }

    private void failRequest(IoRequest request, Throwable error) {
        metrics.failedRequests++;
        complete(request, IoCompletion.failed(request.id(), error));
    }

    private void complete(IoRequest request, IoCompletion completion) {
        CompletableFuture<IoCompletion> future = pending.remove(request.id());
        totalPending.decrementAndGet();
        AtomicInteger counter = perSubmitterPending.get(request.submitter());
        if (counter != null) {
            counter.decrementAndGet();
        }
        if (future != null) {
            future.complete(completion);
        }
    }
}
