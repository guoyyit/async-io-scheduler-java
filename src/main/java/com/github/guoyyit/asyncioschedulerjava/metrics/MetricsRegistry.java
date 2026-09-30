package com.github.guoyyit.asyncioschedulerjava.metrics;

import com.github.guoyyit.asyncioschedulerjava.model.MetricsSnapshot;

import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe monotonically increasing scheduler counters. */
public final class MetricsRegistry {

    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong mergedWrites = new AtomicLong();
    private final AtomicLong flushedBatches = new AtomicLong();
    private final AtomicLong prefetchIssued = new AtomicLong();
    private final AtomicLong prefetchHit = new AtomicLong();
    private final AtomicLong prefetchWasted = new AtomicLong();
    private final AtomicLong quotaDelayed = new AtomicLong();
    private final AtomicLong recoveredBatches = new AtomicLong();
    private final AtomicLong discardedBatches = new AtomicLong();

    public MetricsRegistry() {
    }

    public void incrementRejected() {
        rejected.incrementAndGet();
    }

    public void addMergedWrites(long delta) {
        mergedWrites.addAndGet(delta);
    }

    public void incrementFlushedBatches() {
        flushedBatches.incrementAndGet();
    }

    public void addPrefetchIssued(long delta) {
        prefetchIssued.addAndGet(delta);
    }

    public void addPrefetchHit(long delta) {
        prefetchHit.addAndGet(delta);
    }

    public void addPrefetchWasted(long delta) {
        prefetchWasted.addAndGet(delta);
    }

    public void incrementQuotaDelayed() {
        quotaDelayed.incrementAndGet();
    }

    public void addRecoveredBatches(long delta) {
        recoveredBatches.addAndGet(delta);
    }

    public void addDiscardedBatches(long delta) {
        discardedBatches.addAndGet(delta);
    }

    public MetricsSnapshot snapshot(long pendingReads, long pendingWrites,
                                   long completedReads, long completedWrites) {
        return new MetricsSnapshot(pendingReads, pendingWrites, completedReads,
                completedWrites, rejected.get(), mergedWrites.get(),
                flushedBatches.get(), prefetchIssued.get(), prefetchHit.get(),
                prefetchWasted.get(), quotaDelayed.get(), recoveredBatches.get(),
                discardedBatches.get());
    }
}
