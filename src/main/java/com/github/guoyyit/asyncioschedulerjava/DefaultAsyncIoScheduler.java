package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.api.AsyncIoScheduler;
import com.github.guoyyit.asyncioschedulerjava.api.QuotaExceededException;
import com.github.guoyyit.asyncioschedulerjava.api.SchedulerClosedException;
import com.github.guoyyit.asyncioschedulerjava.buffer.BlockCache;
import com.github.guoyyit.asyncioschedulerjava.buffer.WriteCoalescer;
import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.metrics.MetricsRegistry;
import com.github.guoyyit.asyncioschedulerjava.model.IoRequest;
import com.github.guoyyit.asyncioschedulerjava.model.MetricsSnapshot;
import com.github.guoyyit.asyncioschedulerjava.model.OperationType;
import com.github.guoyyit.asyncioschedulerjava.model.Priority;
import com.github.guoyyit.asyncioschedulerjava.model.QuotaSnapshot;
import com.github.guoyyit.asyncioschedulerjava.model.Ticket;
import com.github.guoyyit.asyncioschedulerjava.prefetch.PrefetchManager;
import com.github.guoyyit.asyncioschedulerjava.quota.QuotaManager;
import com.github.guoyyit.asyncioschedulerjava.queue.PendingRequestQueue;
import com.github.guoyyit.asyncioschedulerjava.scheduling.AgingPrioritySelector;
import com.github.guoyyit.asyncioschedulerjava.scheduling.PrioritySelector;
import com.github.guoyyit.asyncioschedulerjava.scheduling.StrictPrioritySelector;
import com.github.guoyyit.asyncioschedulerjava.storage.BlockStorage;
import com.github.guoyyit.asyncioschedulerjava.storage.FileDurableChannel;
import com.github.guoyyit.asyncioschedulerjava.storage.RecoveryResult;
import com.github.guoyyit.asyncioschedulerjava.storage.WriteAheadLog;
import com.github.guoyyit.asyncioschedulerjava.storage.WriteBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class DefaultAsyncIoScheduler implements AsyncIoScheduler {

    private static final long IDLE_WAIT_NANOS = 1_000_000L;

    private final SchedulerConfig config;
    private final BlockStorage storage;
    private final boolean ownsStorage;
    private final PendingRequestQueue queue;
    private final PrioritySelector selector;
    private final BlockCache cache;
    private final WriteCoalescer coalescer;
    private final PrefetchManager prefetchManager;
    private final QuotaManager quotas;
    private final MetricsRegistry metrics = new MetricsRegistry();

    private final AtomicLong requestIds = new AtomicLong();
    private final AtomicLong batchIds = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final Map<Long, CompletableFuture<?>> futures = new HashMap<>();
    private final Map<Long, List<Long>> pendingWriteRequestIds = new HashMap<>();
    private final Map<IoRequest, Long> quotaReadyNanos = new HashMap<>();

    private final AtomicLong pendingReads = new AtomicLong();
    private final AtomicLong pendingWrites = new AtomicLong();
    private final AtomicLong completedReads = new AtomicLong();
    private final AtomicLong completedWrites = new AtomicLong();

    private final Object stateLock = new Object();
    private Thread worker;

    public DefaultAsyncIoScheduler(Path directory, SchedulerConfig config) {
        this(openStorage(directory, config), config, true);
    }

    public DefaultAsyncIoScheduler(BlockStorage storage, SchedulerConfig config) {
        this(storage, config, false);
    }

    private DefaultAsyncIoScheduler(BlockStorage storage, SchedulerConfig config,
                                    boolean ownsStorage) {
        this.config = config;
        this.storage = storage;
        this.ownsStorage = ownsStorage;
        this.queue = new PendingRequestQueue(config.getMaxPendingRequests(),
                config.getMaxPendingPerSubmitter(), config.getOverflowPolicy());
        this.selector = config.isFairScheduling()
                ? new AgingPrioritySelector(config.getStarvationAgeThreshold())
                : new StrictPrioritySelector();
        this.cache = new BlockCache(config.getCacheBlocks(), config.getBlockSize());
        this.coalescer = new WriteCoalescer(config.getMaxWriteBatchBlocks(),
                config.getMaxBatchDelay().toNanos(), config.getBlockSize());
        this.prefetchManager = new PrefetchManager(config);
        this.quotas = new QuotaManager(config.getReadQuotaBytesPerSec(),
                config.getWriteQuotaBytesPerSec(), config.getQuotaWindow().toNanos());
        if (ownsStorage) {
            try {
                RecoveryResult recoveryResult = storage.recover();
                metrics.addRecoveredBatches(recoveryResult.getRecoveredBatches().size());
                metrics.addDiscardedBatches(recoveryResult.getDiscardedBatches());
            } catch (IOException e) {
                throw new IllegalStateException("failed to recover storage", e);
            }
        }
        startWorker();
    }

    private static BlockStorage openStorage(Path directory, SchedulerConfig config) {
        try {
            Files.createDirectories(directory);
            FileDurableChannel dataChannel =
                    FileDurableChannel.open(directory.resolve("data.blocks"));
            FileDurableChannel logChannel =
                    FileDurableChannel.open(directory.resolve("wal.log"));
            WriteAheadLog wal = new WriteAheadLog(logChannel, config.getBlockSize());
            BlockStorage blockStorage =
                    new BlockStorage(dataChannel, wal, config.getBlockSize());
            return blockStorage;
        } catch (IOException e) {
            throw new IllegalStateException("failed to open scheduler storage", e);
        }
    }

    private void startWorker() {
        worker = new Thread(this::runLoop, "async-io-scheduler");
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public Ticket<Integer> submitRead(String submitter, Priority priority, long offset,
                                      ByteBuffer dst) {
        return submit(submitter, priority, offset, dst, OperationType.READ);
    }

    @Override
    public Ticket<Void> submitWrite(String submitter, Priority priority, long offset,
                                    ByteBuffer src) {
        return submit(submitter, priority, offset, copyRemaining(src), OperationType.WRITE);
    }

    private static ByteBuffer copyRemaining(ByteBuffer source) {
        ByteBuffer duplicate = source.duplicate();
        ByteBuffer copy = ByteBuffer.allocate(duplicate.remaining());
        copy.put(duplicate);
        copy.flip();
        return copy;
    }

    private <T> Ticket<T> submit(String submitter, Priority priority, long offset,
                                 ByteBuffer buffer, OperationType type) {
        if (submitter == null || submitter.isBlank()) {
            throw new IllegalArgumentException("submitter must not be blank");
        }
        if (priority == null) {
            throw new IllegalArgumentException("priority must not be null");
        }
        if (offset < 0 || buffer.remaining() == 0) {
            throw new IllegalArgumentException("offset must be >= 0 and buffer non-empty");
        }
        if (closed.get()) {
            throw new SchedulerClosedException("scheduler is closed");
        }

        long id = requestIds.incrementAndGet();
        CompletableFuture<T> future = new CompletableFuture<>();
        IoRequest request = new IoRequest(id, submitter, type, priority, offset,
                buffer.remaining(), buffer, System.nanoTime());
        synchronized (stateLock) {
            futures.put(id, future);
            if (type == OperationType.READ) {
                pendingReads.incrementAndGet();
            } else {
                pendingWrites.incrementAndGet();
            }
        }
        try {
            queue.enqueue(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            removePending(id, type);
            future.completeExceptionally(e);
        } catch (RuntimeException e) {
            removePending(id, type);
            metrics.incrementRejected();
            future.completeExceptionally(e);
        }
        return new Ticket<>(id, future);
    }

    @Override
    public void setQuota(String submitter, long readBytesPerSec, long writeBytesPerSec) {
        if (closed.get()) {
            throw new SchedulerClosedException("scheduler is closed");
        }
        quotas.setQuota(submitter, readBytesPerSec, writeBytesPerSec);
    }

    @Override
    public QuotaSnapshot quotaSnapshot(String submitter) {
        return quotas.snapshot(submitter, System.nanoTime());
    }

    @Override
    public MetricsSnapshot metricsSnapshot() {
        MetricsSnapshot snapshot = metrics.snapshot(pendingReads.get(), pendingWrites.get(),
                completedReads.get(), completedWrites.get());
        return new MetricsSnapshot(snapshot.getPendingReads(), snapshot.getPendingWrites(),
                snapshot.getCompletedReads(), snapshot.getCompletedWrites(),
                snapshot.getRejectedRequests(), snapshot.getMergedWrites(),
                snapshot.getFlushedBatches(), prefetchManager.issuedBlocks(),
                prefetchManager.hitBlocks(), prefetchManager.wastedBlocks(),
                snapshot.getQuotaDelayedRequests(), snapshot.getRecoveredBatches(),
                snapshot.getDiscardedBatches());
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        queue.close();
        if (worker != null) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (ownsStorage) {
            try {
                storage.close();
            } catch (IOException e) {
                throw new IllegalStateException("failed to close storage", e);
            }
        }
    }

    private void removePending(long id, OperationType type) {
        synchronized (stateLock) {
            futures.remove(id);
            if (type == OperationType.READ) {
                pendingReads.decrementAndGet();
            } else {
                pendingWrites.decrementAndGet();
            }
        }
    }

    private void runLoop() {
        try {
            while (true) {
                long now = System.nanoTime();
                boolean noIncoming = closed.get() && queue.isEmpty() && quotaReadyNanos.isEmpty();
                if (noIncoming && coalescer.isEmpty()) {
                    break;
                }

                if (coalescer.shouldFlush(now)
                        || (closed.get() && !coalescer.isEmpty() && noIncoming)) {
                    flushWrites();
                    continue;
                }

                IoRequest request = takeReady(now);
                if (request == null) {
                    long wakeup = nextWakeupNanos(now);
                    long waitNanos = wakeup == Long.MAX_VALUE
                            ? IDLE_WAIT_NANOS : Math.min(IDLE_WAIT_NANOS, Math.max(1L, wakeup - now));
                    sleepNanos(waitNanos);
                    continue;
                }
                if (request.getType() == OperationType.WRITE) {
                    handleWrite(request, now);
                } else {
                    handleRead(request, now);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            failAll(t);
        }
    }

    private static void sleepNanos(long nanos) throws InterruptedException {
        Thread.sleep(nanos / 1_000_000L, (int) (nanos % 1_000_000L));
    }

    private IoRequest takeReady(long now) throws InterruptedException {
        IoRequest readyDelayed = null;
        long earliestQuotaWakeup = Long.MAX_VALUE;
        for (Map.Entry<IoRequest, Long> entry : quotaReadyNanos.entrySet()) {
            if (entry.getValue() <= now) {
                readyDelayed = entry.getKey();
                break;
            }
            earliestQuotaWakeup = Math.min(earliestQuotaWakeup, entry.getValue());
        }
        if (readyDelayed != null) {
            quotaReadyNanos.remove(readyDelayed);
            return readyDelayed;
        }

        long selectTimeout = earliestQuotaWakeup == Long.MAX_VALUE
                ? IDLE_WAIT_NANOS
                : Math.min(IDLE_WAIT_NANOS, Math.max(1L, earliestQuotaWakeup - now));
        if (!coalescer.isEmpty()) {
            long flushAt = coalescer.oldestDirtyNanos() + config.getMaxBatchDelay().toNanos();
            selectTimeout = Math.min(selectTimeout, Math.max(1L, flushAt - now));
        }

        IoRequest request = queue.select(selector, now, selectTimeout);
        if (request == null || closed.get()) {
            return request;
        }
        if (!quotas.isAllowed(request.getSubmitter(), request.getType(),
                request.getLength(), now)) {
            switch (config.getQuotaAction()) {
                case REJECT -> {
                    failRequest(request, new QuotaExceededException(
                            "quota exceeded for submitter '" + request.getSubmitter() + '\''));
                    metrics.incrementQuotaDelayed();
                    return null;
                }
                case DELAY -> {
                    long waitNanos = quotas.nanosUntilAllowed(request.getSubmitter(),
                            request.getType(), request.getLength(), now);
                    quotaReadyNanos.put(request, now + Math.max(1L, waitNanos));
                    metrics.incrementQuotaDelayed();
                    quotas.incrementDelayed(request.getSubmitter());
                    return null;
                }
                default -> throw new IllegalStateException("unknown quota action");
            }
        }
        return request;
    }

    private long nextWakeupNanos(long now) {
        long earliest = Long.MAX_VALUE;
        for (long readyAt : quotaReadyNanos.values()) {
            earliest = Math.min(earliest, readyAt);
        }
        if (!coalescer.isEmpty()) {
            earliest = Math.min(earliest,
                    coalescer.oldestDirtyNanos() + config.getMaxBatchDelay().toNanos());
        }
        return earliest;
    }

    private void handleWrite(IoRequest request, long now) {
        quotas.acquire(request.getSubmitter(), OperationType.WRITE, request.getLength(), now);
        int blockSize = config.getBlockSize();
        long firstBlock = Math.floorDiv(request.getOffset(), (long) blockSize);
        long endBlock = Math.floorDiv(request.getOffset() + request.getLength() - 1L, blockSize);
        ByteBuffer source = request.getBuffer().duplicate();

        for (long blockIndex = firstBlock; blockIndex <= endBlock; blockIndex++) {
            long blockStart = blockIndex * (long) blockSize;
            long overlapStart = Math.max(request.getOffset(), blockStart);
            long overlapEnd = Math.min(request.getOffset() + request.getLength(),
                    blockStart + blockSize);
            int within = (int) (overlapEnd - overlapStart);
            int skip = (int) (overlapStart - blockStart);

            ByteBuffer block = loadBlockForWrite(blockIndex);
            block.rewind();
            byte[] chunk = new byte[within];
            source.get(chunk);
            ByteBuffer writeView = block.duplicate();
            writeView.position(skip);
            writeView.put(chunk);
            block.rewind();

            boolean alreadyDirty = coalescer.containsBlock(blockIndex);
            coalescer.add(blockIndex, block, now);
            if (alreadyDirty) {
                metrics.addMergedWrites(1);
            }
            chargeWasted(cache.put(blockIndex, block, false));
            pendingWriteRequestIds.computeIfAbsent(blockIndex, key -> new ArrayList<>())
                    .add(request.getId());
        }

        if (coalescer.shouldFlush(now)) {
            flushWrites();
        }
    }

    private ByteBuffer loadBlockForWrite(long blockIndex) {
        ByteBuffer cached = cache.get(blockIndex);
        if (cached != null) {
            return cached;
        }
        try {
            return storage.readBlock(blockIndex);
        } catch (IOException e) {
            throw new IllegalStateException("failed to read block for RMW", e);
        }
    }

    private void handleRead(IoRequest request, long now) {
        quotas.acquire(request.getSubmitter(), OperationType.READ, request.getLength(), now);
        int blockSize = config.getBlockSize();
        long firstBlock = Math.floorDiv(request.getOffset(), (long) blockSize);
        long endBlock = Math.floorDiv(request.getOffset() + request.getLength() - 1L, blockSize);
        ByteBuffer destination = request.getBuffer();
        int produced = 0;

        for (long blockIndex = firstBlock; blockIndex <= endBlock; blockIndex++) {
            long blockStart = blockIndex * (long) blockSize;
            long overlapStart = Math.max(request.getOffset(), blockStart);
            long overlapEnd = Math.min(request.getOffset() + request.getLength(),
                    blockStart + blockSize);
            int within = (int) (overlapEnd - overlapStart);
            int skip = (int) (overlapStart - blockStart);

            boolean prefetchedResident = cache.isPrefetchResident(blockIndex);
            ByteBuffer block = cache.get(blockIndex);
            if (block == null) {
                try {
                    block = storage.readBlock(blockIndex);
                } catch (IOException e) {
                    failRequest(request, e);
                    return;
                }
                chargeWasted(cache.put(blockIndex, block, false));
            }
            if (prefetchedResident) {
                prefetchManager.markConsumed(blockIndex);
            }

            ByteBuffer slice = block.duplicate();
            slice.position(skip);
            slice.limit(skip + within);
            destination.put(slice);
            produced += within;

            maybePrefetch(request.getSubmitter(), firstBlock, blockIndex, now);
        }

        completeRead(request, produced);
    }

    private void maybePrefetch(String submitter, long firstBlock, long lastTouchedBlock,
                               long now) {
        if (lastTouchedBlock != firstBlock || !queue.isEmpty()
                || !quotaReadyNanos.isEmpty() || !coalescer.isEmpty()) {
            return;
        }
        Collection<Long> advice =
                prefetchManager.advise(streamId(submitter, firstBlock), firstBlock, now);
        if (advice.isEmpty()) {
            return;
        }
        List<Long> issued = new ArrayList<>();
        for (Long blockIndex : advice) {
            try {
                ByteBuffer block = storage.readBlock(blockIndex);
                Collection<Long> wasted = cache.put(blockIndex, block, true);
                if (!wasted.isEmpty()) {
                    prefetchManager.markWasted(wasted);
                }
                issued.add(blockIndex);
            } catch (IOException e) {
                break;
            }
        }
        prefetchManager.markIssued(issued);
    }

    private static String streamId(String submitter, long blockIndex) {
        return submitter + ':' + (blockIndex / 64L);
    }

    private void chargeWasted(Collection<Long> wastedBlocks) {
        if (!wastedBlocks.isEmpty()) {
            prefetchManager.markWasted(wastedBlocks);
        }
    }

    @SuppressWarnings("unchecked")
    private void completeRead(IoRequest request, int bytes) {
        synchronized (stateLock) {
            CompletableFuture<Integer> future =
                    (CompletableFuture<Integer>) futures.remove(request.getId());
            if (future != null) {
                future.complete(bytes);
            }
            pendingReads.decrementAndGet();
            completedReads.incrementAndGet();
        }
    }

    private void flushWrites() {
        long batchId = batchIds.incrementAndGet();
        WriteBatch batch = coalescer.drain(batchId, System.nanoTime());
        List<Long> requestIds = new ArrayList<>();
        for (WriteBatch.Entry entry : batch.getEntries()) {
            List<Long> blockRequests = pendingWriteRequestIds.remove(entry.getBlockIndex());
            if (blockRequests != null) {
                for (Long requestId : blockRequests) {
                    if (!requestIds.contains(requestId)) {
                        requestIds.add(requestId);
                    }
                }
            }
        }

        try {
            storage.writeBatch(batch);
            metrics.incrementFlushedBatches();
            synchronized (stateLock) {
                for (Long requestId : requestIds) {
                    CompletableFuture<?> future = futures.remove(requestId);
                    if (future != null) {
                        completeWriteFuture(future);
                    }
                }
                pendingWrites.addAndGet(-requestIds.size());
                completedWrites.addAndGet(requestIds.size());
            }
        } catch (IOException e) {
            synchronized (stateLock) {
                for (Long requestId : requestIds) {
                    CompletableFuture<?> future = futures.remove(requestId);
                    if (future != null) {
                        future.completeExceptionally(e);
                    }
                }
                pendingWrites.addAndGet(-requestIds.size());
            }
            throw new IllegalStateException("failed to durably flush write batch", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void completeWriteFuture(CompletableFuture<?> future) {
        ((CompletableFuture<Void>) future).complete(null);
    }

    private void failRequest(IoRequest request, Throwable error) {
        CompletableFuture<?> future;
        synchronized (stateLock) {
            future = futures.remove(request.getId());
            if (request.getType() == OperationType.READ) {
                pendingReads.decrementAndGet();
            } else {
                pendingWrites.decrementAndGet();
            }
        }
        if (future != null) {
            future.completeExceptionally(error);
        }
    }

    private void failAll(Throwable error) {
        List<CompletableFuture<?>> pending;
        synchronized (stateLock) {
            pending = new ArrayList<>(futures.values());
            futures.clear();
        }
        for (CompletableFuture<?> future : pending) {
            future.completeExceptionally(error);
        }
    }
}
