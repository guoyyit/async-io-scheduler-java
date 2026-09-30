package com.github.guoyyit.asyncioschedulerjava.io;

import java.util.Arrays;

/**
 * An immutable contiguous byte range located at an absolute byte offset.
 */
public final class WriteSegment {

    private final long offset;
    private final byte[] data;

    public WriteSegment(long offset, byte[] data) {
        this.offset = offset;
        this.data = data == null ? new byte[0] : data.clone();
    }

    public long offset() {
        return offset;
    }

    public byte[] data() {
        return data.clone();
    }

    public int length() {
        return data.length;
    }

    public long endOffset() {
        return offset + data.length;
    }

    @Override
    public String toString() {
        return "WriteSegment{offset=" + offset + ", length=" + data.length + '}';
    }
}
