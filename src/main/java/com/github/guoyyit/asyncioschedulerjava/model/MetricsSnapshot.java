package com.github.guoyyit.asyncioschedulerjava.model;

/** Immutable point-in-time snapshot of scheduler metrics. */
public final class MetricsSnapshot {

    private final long pendingReads;
    private final long pendingWrites;
    private final long completedReads;
    private final long completedWrites;
    private final long rejectedRequests;
    private final long mergedWrites;
    private final long flushedBatches;
    private final long prefetchIssuedBlocks;
    private final long prefetchHitBlocks;
    private final long prefetchWastedBlocks;
    private final long quotaDelayedRequests;
    private final long recoveredBatches;
    private final long discardedBatches;

    public MetricsSnapshot() {
        this(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public MetricsSnapshot(long pendingReads, long pendingWrites, long completedReads,
                           long completedWrites, long rejectedRequests, long mergedWrites,
                           long flushedBatches, long prefetchIssuedBlocks,
                           long prefetchHitBlocks, long prefetchWastedBlocks,
                           long quotaDelayedRequests, long recoveredBatches,
                           long discardedBatches) {
        this.pendingReads = pendingReads;
        this.pendingWrites = pendingWrites;
        this.completedReads = completedReads;
        this.completedWrites = completedWrites;
        this.rejectedRequests = rejectedRequests;
        this.mergedWrites = mergedWrites;
        this.flushedBatches = flushedBatches;
        this.prefetchIssuedBlocks = prefetchIssuedBlocks;
        this.prefetchHitBlocks = prefetchHitBlocks;
        this.prefetchWastedBlocks = prefetchWastedBlocks;
        this.quotaDelayedRequests = quotaDelayedRequests;
        this.recoveredBatches = recoveredBatches;
        this.discardedBatches = discardedBatches;
    }

    public long getPendingReads() {
        return pendingReads;
    }

    public long getPendingWrites() {
        return pendingWrites;
    }

    public long getCompletedReads() {
        return completedReads;
    }

    public long getCompletedWrites() {
        return completedWrites;
    }

    public long getRejectedRequests() {
        return rejectedRequests;
    }

    public long getMergedWrites() {
        return mergedWrites;
    }

    public long getFlushedBatches() {
        return flushedBatches;
    }

    public long getPrefetchIssuedBlocks() {
        return prefetchIssuedBlocks;
    }

    public long getPrefetchHitBlocks() {
        return prefetchHitBlocks;
    }

    public long getPrefetchWastedBlocks() {
        return prefetchWastedBlocks;
    }

    public long getQuotaDelayedRequests() {
        return quotaDelayedRequests;
    }

    public long getRecoveredBatches() {
        return recoveredBatches;
    }

    public long getDiscardedBatches() {
        return discardedBatches;
    }
}
