package com.github.guoyyit.asyncioschedulerjava.io;

import java.nio.ByteBuffer;

/**
 * A unit of asynchronous IO work. Instances are immutable.
 */
public final class IoRequest {

    private final long id;
    private final IoType type;
    private final IoPriority priority;
    private final String submitter;
    private final String streamId;
    private final long offset;
    private final int length;
    private final ByteBuffer data;
    private final long enqueueNanos;
    private final int degradeCount;

    public IoRequest(long id, IoType type, IoPriority priority, String submitter, String streamId,
                     long offset, int length, ByteBuffer data, long enqueueNanos) {
        this(id, type, priority, submitter, streamId, offset, length, data, enqueueNanos, 0);
    }

    public IoRequest(long id, IoType type, IoPriority priority, String submitter, String streamId,
                     long offset, int length, ByteBuffer data, long enqueueNanos,
                     int degradeCount) {
        this.id = id;
        this.type = type;
        this.priority = priority;
        this.submitter = submitter;
        this.streamId = streamId;
        this.offset = offset;
        this.length = length;
        this.data = data;
        this.enqueueNanos = enqueueNanos;
        this.degradeCount = degradeCount;
    }

    public long id() {
        return id;
    }

    public IoType type() {
        return type;
    }

    public IoPriority priority() {
        return priority;
    }

    public String submitter() {
        return submitter;
    }

    public String streamId() {
        return streamId;
    }

    public long offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public ByteBuffer data() {
        return data;
    }

    public long enqueueNanos() {
        return enqueueNanos;
    }

    public int degradeCount() {
        return degradeCount;
    }
}
