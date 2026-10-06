package com.github.guoyyit.asyncioschedulerjava.quota;

import com.github.guoyyit.asyncioschedulerjava.config.OverQuotaAction;
import com.github.guoyyit.asyncioschedulerjava.io.IoType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Token-bucket bandwidth quotas, per submitter and per direction (read/write).
 */
public class QuotaManager {

    private final long defaultReadBytesPerSec;
    private final long defaultWriteBytesPerSec;
    private volatile OverQuotaAction overQuotaAction;

    private final Map<String, BucketPair> buckets = new ConcurrentHashMap<>();

    private static final class BucketPair {
        final TokenBucket read;
        final TokenBucket write;

        BucketPair(long readRate, long writeRate) {
            long now = System.nanoTime();
            this.read = new TokenBucket(readRate, now);
            this.write = new TokenBucket(writeRate, now);
        }
    }

    public QuotaManager(long defaultReadBytesPerSec, long defaultWriteBytesPerSec,
                        OverQuotaAction overQuotaAction) {
        this.defaultReadBytesPerSec = defaultReadBytesPerSec;
        this.defaultWriteBytesPerSec = defaultWriteBytesPerSec;
        this.overQuotaAction = overQuotaAction;
    }

    public void setQuota(String submitter, IoType type, long bytesPerSec) {
        BucketPair current = buckets.get(submitter);
        long readRate = current == null ? defaultReadBytesPerSec : current.read.rate();
        long writeRate = current == null ? defaultWriteBytesPerSec : current.write.rate();
        BucketPair replacement = new BucketPair(
                type == IoType.READ ? bytesPerSec : readRate,
                type == IoType.WRITE ? bytesPerSec : writeRate);
        buckets.put(submitter, replacement);
    }

    public void removeQuota(String submitter) {
        buckets.remove(submitter);
    }

    /**
     * Reserves tokens for a request, blocking when configured to DELAY.
     * Returns the action actually applied to the request.
     */
    public OverQuotaAction reserve(String submitter, IoType type, int bytes)
            throws InterruptedException {
        BucketPair pair = pairFor(submitter);
        TokenBucket bucket = type == IoType.READ ? pair.read : pair.write;
        OverQuotaAction action = overQuotaAction;
        if (action == OverQuotaAction.DELAY) {
            long waited = bucket.reserveBlocking(bytes, System.nanoTime());
            return waited > 0L ? OverQuotaAction.DELAY : null;
        }
        if (bucket.tryReserve(bytes, System.nanoTime())) {
            return null;
        }
        return action;
    }

    /**
     * Non-blocking reservation used for background work such as prefetching.
     * Returns false when tokens are currently unavailable.
     */
    public boolean tryReserve(String submitter, IoType type, int bytes) {
        BucketPair pair = pairFor(submitter);
        TokenBucket bucket = type == IoType.READ ? pair.read : pair.write;
        return bucket.tryReserve(bytes, System.nanoTime());
    }

    /**
     * Blocks until {@code bytes} tokens are available and reserved, regardless
     * of the configured over-quota action. Returns the waited nanoseconds.
     * Used to stop degraded requests from spinning around the queue.
     */
    public long awaitTokens(String submitter, IoType type, int bytes)
            throws InterruptedException {
        BucketPair pair = pairFor(submitter);
        TokenBucket bucket = type == IoType.READ ? pair.read : pair.write;
        return bucket.reserveBlocking(bytes, System.nanoTime());
    }

    /**
     * Returns tokens to the bucket (e.g. when a request failed before consuming IO).
     */
    public void refund(String submitter, IoType type, int bytes) {
        BucketPair pair = buckets.get(submitter);
        if (pair != null) {
            (type == IoType.READ ? pair.read : pair.write).refund(bytes);
        }
    }

    public List<QuotaSnapshot> snapshots() {
        List<QuotaSnapshot> result = new ArrayList<>();
        long now = System.nanoTime();
        for (Map.Entry<String, BucketPair> entry : buckets.entrySet()) {
            result.add(toSnapshot(entry.getKey(), IoType.READ, entry.getValue().read, now));
            result.add(toSnapshot(entry.getKey(), IoType.WRITE, entry.getValue().write, now));
        }
        return result;
    }

    public QuotaSnapshot snapshot(String submitter, IoType type) {
        BucketPair pair = buckets.get(submitter);
        if (pair == null) {
            long rate = type == IoType.READ ? defaultReadBytesPerSec : defaultWriteBytesPerSec;
            return new QuotaSnapshot(submitter, type == IoType.READ, rate, rate,
                    rate, 0L);
        }
        TokenBucket bucket = type == IoType.READ ? pair.read : pair.write;
        return toSnapshot(submitter, type, bucket, System.nanoTime());
    }

    public void setOverQuotaAction(OverQuotaAction action) {
        this.overQuotaAction = action;
    }

    public OverQuotaAction overQuotaAction() {
        return overQuotaAction;
    }

    private BucketPair pairFor(String submitter) {
        return buckets.computeIfAbsent(submitter,
                key -> new BucketPair(defaultReadBytesPerSec, defaultWriteBytesPerSec));
    }

    private static QuotaSnapshot toSnapshot(String submitter, IoType type,
                                            TokenBucket bucket, long now) {
        return new QuotaSnapshot(submitter, type == IoType.READ,
                bucket.rate(), bucket.consumedRate(now),
                bucket.available(now), bucket.delayed());
    }
}
