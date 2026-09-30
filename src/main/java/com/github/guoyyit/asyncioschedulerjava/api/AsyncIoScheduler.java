package com.github.guoyyit.asyncioschedulerjava.api;

import com.github.guoyyit.asyncioschedulerjava.model.MetricsSnapshot;
import com.github.guoyyit.asyncioschedulerjava.model.Priority;
import com.github.guoyyit.asyncioschedulerjava.model.QuotaSnapshot;
import com.github.guoyyit.asyncioschedulerjava.model.Ticket;

import java.nio.ByteBuffer;

/**
 * Unified asynchronous block IO entry point for multiple concurrent submitters.
 *
 * <p>Submission never performs disk IO; it returns a {@link Ticket} immediately
 * and a single background scheduler performs the actual work with priority,
 * fairness, prefetch, write coalescing and quota enforcement.
 */
public interface AsyncIoScheduler extends AutoCloseable {

    /**
     * Submit an asynchronous read into {@code dst} (remaining bytes starting at
     * {@code offset}). The ticket completes with the number of bytes read
     * (0 at/after EOF).
     */
    Ticket<Integer> submitRead(String submitter, Priority priority, long offset, ByteBuffer dst);

    /**
     * Submit an asynchronous write of {@code src}'s remaining bytes at
     * {@code offset}. The ticket completes only after the write is durable:
     * a completed ticket survives an immediate process crash.
     */
    Ticket<Void> submitWrite(String submitter, Priority priority, long offset, ByteBuffer src);

    /** Per-submitter bandwidth quotas; {@code 0} means unlimited. */
    void setQuota(String submitter, long readBytesPerSec, long writeBytesPerSec);

    /** Current quota usage for a submitter. */
    QuotaSnapshot quotaSnapshot(String submitter);

    /** Current scheduler-wide metrics. */
    MetricsSnapshot metricsSnapshot();

    /** Flush all accepted writes durably, then stop the scheduler. */
    @Override
    void close();
}
