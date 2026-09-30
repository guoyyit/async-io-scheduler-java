package com.github.guoyyit.asyncioschedulerjava.model;

/** Per-submitter bandwidth quota; {@code 0} means unlimited. */
public final class SubmitterQuota {

    private final long readBytesPerSec;
    private final long writeBytesPerSec;

    public SubmitterQuota(long readBytesPerSec, long writeBytesPerSec) {
        this.readBytesPerSec = readBytesPerSec;
        this.writeBytesPerSec = writeBytesPerSec;
    }

    public long getReadBytesPerSec() {
        return readBytesPerSec;
    }

    public long getWriteBytesPerSec() {
        return writeBytesPerSec;
    }
}
