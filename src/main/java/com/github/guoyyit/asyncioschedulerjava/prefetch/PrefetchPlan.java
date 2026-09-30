package com.github.guoyyit.asyncioschedulerjava.prefetch;

/**
 * A prefetch decision produced for a user read request.
 */
public final class PrefetchPlan {

    private final long offset;
    private final int length;

    public PrefetchPlan(long offset, int length) {
        this.offset = offset;
        this.length = length;
    }

    public long offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public boolean isEmpty() {
        return length <= 0;
    }

    @Override
    public String toString() {
        return "PrefetchPlan{offset=" + offset + ", length=" + length + '}';
    }
}
