package com.github.guoyyit.asyncioschedulerjava.model;

/** Point-in-time quota usage of one submitter. */
public final class QuotaSnapshot {

    private final String submitter;
    private final long readLimitBytesPerSec;
    private final long writeLimitBytesPerSec;
    private final long readUsedBytesInWindow;
    private final long writeUsedBytesInWindow;
    private final long delayedRequests;

    public QuotaSnapshot() {
        this("", 0, 0, 0, 0, 0);
    }

    public QuotaSnapshot(String submitter, long readLimitBytesPerSec, long writeLimitBytesPerSec,
                         long readUsedBytesInWindow, long writeUsedBytesInWindow,
                         long delayedRequests) {
        this.submitter = submitter;
        this.readLimitBytesPerSec = readLimitBytesPerSec;
        this.writeLimitBytesPerSec = writeLimitBytesPerSec;
        this.readUsedBytesInWindow = readUsedBytesInWindow;
        this.writeUsedBytesInWindow = writeUsedBytesInWindow;
        this.delayedRequests = delayedRequests;
    }

    public String getSubmitter() {
        return submitter;
    }

    public long getReadLimitBytesPerSec() {
        return readLimitBytesPerSec;
    }

    public long getWriteLimitBytesPerSec() {
        return writeLimitBytesPerSec;
    }

    public long getReadUsedBytesInWindow() {
        return readUsedBytesInWindow;
    }

    public long getWriteUsedBytesInWindow() {
        return writeUsedBytesInWindow;
    }

    public long getDelayedRequests() {
        return delayedRequests;
    }
}
