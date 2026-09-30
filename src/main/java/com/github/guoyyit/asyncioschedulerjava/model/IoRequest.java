package com.github.guoyyit.asyncioschedulerjava.model;

import java.nio.ByteBuffer;

/**
 * Immutable IO request accepted by the scheduler.
 * Offsets are byte offsets; reads/writes are aligned to whole blocks by the
 * caller-facing API (partial block writes are allowed and handled via RMW).
 */
public final class IoRequest {

    private final long id;
    private final String submitter;
    private final OperationType type;
    private final Priority priority;
    private final long offset;
    private final int length;
    private final ByteBuffer buffer;
    private final long enqueueNanos;

    public IoRequest(long id, String submitter, OperationType type, Priority priority,
                     long offset, int length, ByteBuffer buffer, long enqueueNanos) {
        this.id = id;
        this.submitter = submitter;
        this.type = type;
        this.priority = priority;
        this.offset = offset;
        this.length = length;
        this.buffer = buffer;
        this.enqueueNanos = enqueueNanos;
    }

    public long getId() {
        return id;
    }

    public String getSubmitter() {
        return submitter;
    }

    public OperationType getType() {
        return type;
    }

    public Priority getPriority() {
        return priority;
    }

    public long getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public long getEnqueueNanos() {
        return enqueueNanos;
    }
}
