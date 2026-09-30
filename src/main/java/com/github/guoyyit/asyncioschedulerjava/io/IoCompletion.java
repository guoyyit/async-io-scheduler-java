package com.github.guoyyit.asyncioschedulerjava.io;

import java.nio.ByteBuffer;

/**
 * Result of a completed IO request.
 */
public final class IoCompletion {

    private final long requestId;
    private final boolean success;
    private final ByteBuffer data;
    private final Throwable error;

    private IoCompletion(long requestId, boolean success, ByteBuffer data, Throwable error) {
        this.requestId = requestId;
        this.success = success;
        this.data = data;
        this.error = error;
    }

    public static IoCompletion ok(long requestId) {
        return new IoCompletion(requestId, true, null, null);
    }

    public static IoCompletion read(long requestId, ByteBuffer data) {
        return new IoCompletion(requestId, true, data, null);
    }

    public static IoCompletion failed(long requestId, Throwable error) {
        return new IoCompletion(requestId, false, null, error);
    }

    public long requestId() {
        return requestId;
    }

    public boolean success() {
        return success;
    }

    public ByteBuffer data() {
        return data;
    }

    public Throwable error() {
        return error;
    }
}
