package com.github.guoyyit.asyncioschedulerjava.prefetch;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.scheduling.SchedulerMetrics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-stream sequential access detector with adaptive window, random-access
 * backoff and a bounded LRU prefetch cache.
 */
public class PrefetchManager implements AutoCloseable {

    private static final class CachedRange {
        private final long offset;
        private final byte[] data;

        private CachedRange(long offset, byte[] data) {
            this.offset = offset;
            this.data = data;
        }

        long offset() {
            return offset;
        }

        long end() {
            return offset + data.length;
        }
    }

    private static final class StreamState {
        private long lastEnd = -1L;
        private int consecutiveSequential;
        private int currentWindow;
        private int lastPlanLength;

        private StreamState(int initialWindow) {
            this.currentWindow = initialWindow;
        }
    }

    private final SchedulerConfig config;
    private final SchedulerMetrics metrics;
    private final Map<String, StreamState> streams = new ConcurrentHashMap<>();

    private final Object cacheLock = new Object();
    private final LinkedHashMap<String, List<CachedRange>> cache =
            new LinkedHashMap<>(16, 0.75f, true);
    private int cachedBytes;

    public PrefetchManager(SchedulerConfig config, SchedulerMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    /**
     * Returns cached bytes fully covering [offset, offset+length), or null on miss.
     */
    public ByteBuffer lookup(String streamId, long offset, int length) {
        if (!config.isPrefetchEnabled() || length <= 0) {
            return null;
        }
        synchronized (cacheLock) {
            List<CachedRange> ranges = cache.get(streamId);
            if (ranges == null) {
                return null;
            }
            List<CachedRange> covering = findCovering(ranges, offset, length);
            if (covering == null) {
                return null;
            }
            byte[] result = new byte[length];
            long requestedEnd = offset + length;
            for (CachedRange range : covering) {
                int from = (int) Math.max(0L, offset - range.offset);
                int to = (int) Math.min(range.data.length, requestedEnd - range.offset);
                int targetOffset = (int) Math.max(0L, range.offset - offset);
                int copyLength = to - from;
                System.arraycopy(range.data, from, result, targetOffset, copyLength);
            }
            long hitEnd = Math.min(requestedEnd, covering.get(covering.size() - 1).end());
            long hitBytes = Math.min(length, hitEnd - offset);
            metrics.prefetchHits++;
            metrics.prefetchHitBytes += hitBytes;
            return ByteBuffer.wrap(result);
        }
    }

    /**
     * Records a user read and decides whether/how much to prefetch.
     */
    public PrefetchPlan afterUserRead(String streamId, long offset, int length) {
        if (!config.isPrefetchEnabled()) {
            return new PrefetchPlan(offset, 0);
        }
        StreamState state = streams.computeIfAbsent(streamId,
                key -> new StreamState(config.getPrefetchMinBytes()));
        synchronized (state) {
            long end = offset + length;
            boolean sequential = state.lastEnd >= 0 && offset == state.lastEnd;
            boolean alreadyPlanned = state.lastEnd >= 0 && offset > state.lastEnd - length
                    && offset < state.lastEnd + state.lastPlanLength;

            if (sequential || (alreadyPlanned && offset >= state.lastEnd - length)) {
                state.consecutiveSequential = Math.min(Integer.MAX_VALUE - 1,
                        state.consecutiveSequential + 1);
            } else if (state.lastEnd >= 0 && offset != state.lastEnd) {
                if (state.consecutiveSequential > 0) {
                    metrics.prefetchBackoffs++;
                }
                state.consecutiveSequential = 0;
                int backedOff = Math.max(config.getPrefetchMinBytes(),
                        (int) (state.currentWindow * config.getPrefetchBackoffFactor()));
                state.currentWindow = Math.min(config.getPrefetchMaxBytes(), backedOff);
                state.lastEnd = end;
                state.lastPlanLength = 0;
                return new PrefetchPlan(offset, 0);
            }

            state.lastEnd = end;
            if (state.consecutiveSequential < config.getSequentialHitThreshold()) {
                state.lastPlanLength = 0;
                return new PrefetchPlan(offset, 0);
            }

            int window = state.currentWindow;
            int availableBudget = config.getPrefetchTotalBudgetBytes() - cachedBytesTotal();
            int planLength = Math.min(window, Math.max(0, availableBudget));
            if (planLength <= 0) {
                state.lastPlanLength = 0;
                return new PrefetchPlan(offset, 0);
            }

            state.currentWindow = Math.min(config.getPrefetchMaxBytes(),
                    (int) Math.min(Integer.MAX_VALUE,
                            window * config.getPrefetchGrowthFactor()));
            state.lastPlanLength = planLength;
            metrics.prefetchesIssued++;
            metrics.prefetchBytesIssued += planLength;
            return new PrefetchPlan(end, planLength);
        }
    }

    /**
     * Stores a completed prefetch read in the cache, accounting for waste.
     */
    public void fill(String streamId, long offset, ByteBuffer data) {
        if (data == null || !data.hasRemaining()) {
            return;
        }
        byte[] bytes = new byte[data.remaining()];
        data.duplicate().get(bytes);
        synchronized (cacheLock) {
            List<CachedRange> ranges = cache.computeIfAbsent(streamId, key -> new ArrayList<>());
            ranges.add(new CachedRange(offset, bytes));
            ranges.sort((a, b) -> Long.compare(a.offset, b.offset));
            cachedBytes += bytes.length;
            evict();
        }
    }

    /**
     * Total bytes currently occupying the cache.
     */
    public int cachedBytes() {
        synchronized (cacheLock) {
            return cachedBytes;
        }
    }

    @Override
    public void close() {
        synchronized (cacheLock) {
            cache.clear();
            cachedBytes = 0;
        }
        streams.clear();
    }

    private int cachedBytesTotal() {
        synchronized (cacheLock) {
            return cachedBytes;
        }
    }

    private List<CachedRange> findCovering(List<CachedRange> ranges, long offset, int length) {
        long cursor = offset;
        long requestedEnd = offset + length;
        List<CachedRange> covering = new ArrayList<>();
        for (CachedRange range : ranges) {
            if (range.end() <= cursor) {
                continue;
            }
            if (range.offset() > cursor) {
                return null;
            }
            covering.add(range);
            cursor = Math.max(cursor, range.end());
            if (cursor >= requestedEnd) {
                return covering;
            }
        }
        return cursor >= requestedEnd ? covering : null;
    }

    private void evict() {
        Iterator<Map.Entry<String, List<CachedRange>>> iterator = cache.entrySet().iterator();
        while (cachedBytes > config.getPrefetchTotalBudgetBytes() && iterator.hasNext()) {
            Map.Entry<String, List<CachedRange>> eldest = iterator.next();
            List<CachedRange> ranges = eldest.getValue();
            if (ranges.isEmpty()) {
                iterator.remove();
                continue;
            }
            CachedRange removed = ranges.remove(0);
            cachedBytes -= removed.data.length;
            metrics.prefetchWastedBytes += removed.data.length;
            if (ranges.isEmpty()) {
                iterator.remove();
            }
        }
    }
}
