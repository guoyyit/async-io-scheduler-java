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
    public long rejectedNotRunning;
    public long rejectedGlobalCapacity;
    public long rejectedSubmitterCapacity;
    public long capacityWaits;
    public long capacityWaitTimeouts;
    public long totalCapacityWaitNanos;
    public long queueWaitTimeouts;
    public long degradedRequests;
    public long degradeRequeuesExhausted;
    public long quotaDelayedRequests;
    public long totalQuotaDelayNanos;

    public long writeBatches;
    public long writeBatchBytes;
    public long mergedWrites;

    public long prefetchesIssued;
    public long prefetchBytesIssued;
    public long prefetchHits;
    public long prefetchHitBytes;
    public long prefetchWastedBytes;
    public long prefetchInvalidatedBytes;
    public long prefetchBackoffs;

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
                prefetchWastedBytes, prefetchBackoffs,
                rejectedNotRunning, rejectedGlobalCapacity, rejectedSubmitterCapacity,
                capacityWaits, capacityWaitTimeouts, totalCapacityWaitNanos,
                queueWaitTimeouts, degradeRequeuesExhausted, totalQuotaDelayNanos,
                prefetchInvalidatedBytes);
    }

    public void reset() {
        submittedReads = 0;
        submittedWrites = 0;
        completedReads = 0;
        completedWrites = 0;
        failedRequests = 0;
        rejectedRequests = 0;
        rejectedNotRunning = 0;
        rejectedGlobalCapacity = 0;
        rejectedSubmitterCapacity = 0;
        capacityWaits = 0;
        capacityWaitTimeouts = 0;
        totalCapacityWaitNanos = 0;
        queueWaitTimeouts = 0;
        degradedRequests = 0;
        degradeRequeuesExhausted = 0;
        quotaDelayedRequests = 0;
        totalQuotaDelayNanos = 0;
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
        public final long rejectedNotRunning;
        public final long rejectedGlobalCapacity;
        public final long rejectedSubmitterCapacity;
        public final long capacityWaits;
        public final long capacityWaitTimeouts;
        public final long totalCapacityWaitNanos;
        public final long queueWaitTimeouts;
        public final long degradeRequeuesExhausted;
        public final long totalQuotaDelayNanos;
        public final long prefetchInvalidatedBytes;

        /**
         * Legacy constructor kept for source compatibility; the overload
         * metrics are reported as zero.
         */
        public MetricsSnapshot(long submittedReads, long submittedWrites,
                               long completedReads, long completedWrites,
                               long failedRequests, long rejectedRequests,
                               long degradedRequests, long quotaDelayedRequests,
                               long writeBatches, long writeBatchBytes, long mergedWrites,
                               long prefetchesIssued, long prefetchBytesIssued,
                               long prefetchHits, long prefetchHitBytes,
                               long prefetchWastedBytes, long prefetchBackoffs) {
            this(submittedReads, submittedWrites, completedReads, completedWrites,
                    failedRequests, rejectedRequests, degradedRequests, quotaDelayedRequests,
                    writeBatches, writeBatchBytes, mergedWrites,
                    prefetchesIssued, prefetchBytesIssued, prefetchHits, prefetchHitBytes,
                    prefetchWastedBytes, prefetchBackoffs,
                    0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        public MetricsSnapshot(long submittedReads, long submittedWrites,
                               long completedReads, long completedWrites,
                               long failedRequests, long rejectedRequests,
                               long degradedRequests, long quotaDelayedRequests,
                               long writeBatches, long writeBatchBytes, long mergedWrites,
                               long prefetchesIssued, long prefetchBytesIssued,
                               long prefetchHits, long prefetchHitBytes,
                               long prefetchWastedBytes, long prefetchBackoffs,
                               long rejectedNotRunning, long rejectedGlobalCapacity,
                               long rejectedSubmitterCapacity,
                               long capacityWaits, long capacityWaitTimeouts,
                               long totalCapacityWaitNanos,
                               long queueWaitTimeouts, long degradeRequeuesExhausted,
                               long totalQuotaDelayNanos, long prefetchInvalidatedBytes) {
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
            this.rejectedNotRunning = rejectedNotRunning;
            this.rejectedGlobalCapacity = rejectedGlobalCapacity;
            this.rejectedSubmitterCapacity = rejectedSubmitterCapacity;
            this.capacityWaits = capacityWaits;
            this.capacityWaitTimeouts = capacityWaitTimeouts;
            this.totalCapacityWaitNanos = totalCapacityWaitNanos;
            this.queueWaitTimeouts = queueWaitTimeouts;
            this.degradeRequeuesExhausted = degradeRequeuesExhausted;
            this.totalQuotaDelayNanos = totalQuotaDelayNanos;
            this.prefetchInvalidatedBytes = prefetchInvalidatedBytes;
        }
    }
}
