package com.github.guoyyit.asyncioschedulerjava.quota;

/**
 * Point-in-time quota usage of one submitter for one direction.
 */
public final class QuotaSnapshot {

    private final String submitter;
    private final boolean read;
    private final long limitBytesPerSec;
    private final double consumedBytesPerSec;
    private final double availableTokens;
    private final long delayedCount;

    public QuotaSnapshot(String submitter, boolean read, long limitBytesPerSec,
                         double consumedBytesPerSec, double availableTokens, long delayedCount) {
        this.submitter = submitter;
        this.read = read;
        this.limitBytesPerSec = limitBytesPerSec;
        this.consumedBytesPerSec = consumedBytesPerSec;
        this.availableTokens = availableTokens;
        this.delayedCount = delayedCount;
    }

    public String submitter() {
        return submitter;
    }

    public boolean isRead() {
        return read;
    }

    public long limitBytesPerSec() {
        return limitBytesPerSec;
    }

    public double consumedBytesPerSec() {
        return consumedBytesPerSec;
    }

    public double availableTokens() {
        return availableTokens;
    }

    public long delayedCount() {
        return delayedCount;
    }
}
