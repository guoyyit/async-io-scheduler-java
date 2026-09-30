package com.github.guoyyit.asyncioschedulerjava.quota;

/**
 * A simple thread-safe token bucket with a capacity equal to one second of
 * refill, which bounds burst size to roughly one quota window.
 */
final class TokenBucket {

    private final long rateBytesPerSec;
    private double tokens;
    private long lastRefillNanos;
    private long delayedCount;

    TokenBucket(long rateBytesPerSec, long nowNanos) {
        this.rateBytesPerSec = rateBytesPerSec;
        this.tokens = Math.max(0L, rateBytesPerSec);
        this.lastRefillNanos = nowNanos;
    }

    synchronized long rate() {
        return rateBytesPerSec;
    }

    synchronized double available(long nowNanos) {
        refill(nowNanos);
        return tokens;
    }

    synchronized long delayed() {
        return delayedCount;
    }

    synchronized boolean tryReserve(int bytes, long nowNanos) {
        refill(nowNanos);
        if (tokens >= bytes) {
            tokens -= bytes;
            return true;
        }
        return false;
    }

    synchronized long reserveBlocking(int bytes, long nowNanos) throws InterruptedException {
        long waited = 0L;
        while (true) {
            refill(nowNanos);
            if (tokens >= bytes) {
                tokens -= bytes;
                if (waited > 0L) {
                    delayedCount++;
                }
                return waited;
            }
            double deficit = bytes - tokens;
            long waitNanos = (long) Math.ceil(deficit * 1_000_000_000.0d / Math.max(1L, rateBytesPerSec));
            waitNanos = Math.max(1_000L, waitNanos);
            wait(waitNanos / 1_000_000L, (int) (waitNanos % 1_000_000L));
            waited += waitNanos;
            nowNanos = System.nanoTime();
        }
    }

    synchronized void refund(int bytes) {
        tokens = Math.min(rateBytesPerSec, tokens + bytes);
        notifyAll();
    }

    synchronized double consumedRate(long nowNanos) {
        refill(nowNanos);
        return Math.max(0.0d, rateBytesPerSec - tokens);
    }

    private void refill(long nowNanos) {
        long elapsed = nowNanos - lastRefillNanos;
        if (elapsed <= 0L) {
            return;
        }
        double added = elapsed * (rateBytesPerSec / 1_000_000_000.0d);
        tokens = Math.min(rateBytesPerSec, tokens + added);
        lastRefillNanos = nowNanos;
    }
}
