package com.github.guoyyit.asyncioschedulerjava.scheduling;

/**
 * Aggregated operational counters of the scheduler.
 */
public class SchedulerMetrics {

    public long submittedReads;
    public long submittedWrites;
    public long completedReads;
    public long completedWrites;
    public long failedRequests;
    public long rejectedRequests;
    public long degradedRequests;
    public long quotaDelayedRequests;

    public long writeBatches;
    public long writeBatchBytes;
    public long mergedWrites;

    public long prefetchesIssued;
    public long prefetchBytesIssued;
    public long prefetchHits;
    public long prefetchHitBytes;
    public long prefetchWastedBytes;
    public long prefetchBackoffs;
    public long prefetchInvalidations;
    public long prefetchInvalidatedBytes;
    public long prefetchStaleDrops;

    public long capacityWaits;
    public long capacityWaitNanos;
    public long rejectedGlobalCapacity;
    public long rejectedSubmitterCapacity;
    public long rejectedWaitTimeout;
    public long agedRequests;

    public long totalQueueWaitNanos;
    public long totalServiceNanos;

    public MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
                submittedReads, submittedWrites,
                completedReads, completedWrites,
                failedRequests, rejectedRequests,
                degradedRequests, quotaDelayedRequests,
                writeBatches, writeBatchBytes, mergedWrites,
                prefetchesIssued, prefetchBytesIssued,
                prefetchHits, prefetchHitBytes,
                prefetchWastedBytes, prefetchBackoffs);
    }

    public void reset() {
        submittedReads = 0;
        submittedWrites = 0;
        completedReads = 0;
        completedWrites = 0;
        failedRequests = 0;
        rejectedRequests = 0;
        degradedRequests = 0;
        quotaDelayedRequests = 0;
        writeBatches = 0;
        writeBatchBytes = 0;
        mergedWrites = 0;
        prefetchesIssued = 0;
        prefetchBytesIssued = 0;
        prefetchHits = 0;
        prefetchHitBytes = 0;
        prefetchWastedBytes = 0;
        prefetchBackoffs = 0;
        totalQueueWaitNanos = 0;
        totalServiceNanos = 0;
    }

    /**
     * Immutable snapshot of all counters plus derived ratios.
     */
    public static final class MetricsSnapshot {

        public final long submittedReads;
        public final long submittedWrites;
        public final long completedReads;
        public final long completedWrites;
        public final long failedRequests;
        public final long rejectedRequests;
        public final long degradedRequests;
        public final long quotaDelayedRequests;
        public final long writeBatches;
        public final long writeBatchBytes;
        public final long mergedWrites;
        public final long prefetchesIssued;
        public final long prefetchBytesIssued;
        public final long prefetchHits;
        public final long prefetchHitBytes;
        public final long prefetchWastedBytes;
        public final long prefetchBackoffs;

        public MetricsSnapshot(long submittedReads, long submittedWrites,
                               long completedReads, long completedWrites,
                               long failedRequests, long rejectedRequests,
                               long degradedRequests, long quotaDelayedRequests,
                               long writeBatches, long writeBatchBytes, long mergedWrites,
                               long prefetchesIssued, long prefetchBytesIssued,
                               long prefetchHits, long prefetchHitBytes,
                               long prefetchWastedBytes, long prefetchBackoffs) {
            this.submittedReads = submittedReads;
            this.submittedWrites = submittedWrites;
            this.completedReads = completedReads;
            this.completedWrites = completedWrites;
            this.failedRequests = failedRequests;
            this.rejectedRequests = rejectedRequests;
            this.degradedRequests = degradedRequests;
            this.quotaDelayedRequests = quotaDelayedRequests;
            this.writeBatches = writeBatches;
            this.writeBatchBytes = writeBatchBytes;
            this.mergedWrites = mergedWrites;
            this.prefetchesIssued = prefetchesIssued;
            this.prefetchBytesIssued = prefetchBytesIssued;
            this.prefetchHits = prefetchHits;
            this.prefetchHitBytes = prefetchHitBytes;
            this.prefetchWastedBytes = prefetchWastedBytes;
            this.prefetchBackoffs = prefetchBackoffs;
        }
    }
}
