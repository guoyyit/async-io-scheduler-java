package com.github.guoyyit.asyncioschedulerjava.prefetch;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinates adaptive prefetch: per-stream sequential detection, a global
 * resident-prefetch budget, and hit/waste metrics. The scheduler only issues
 * the returned block indexes when doing so would not delay pending writes or
 * low-priority user reads.
 */
public final class PrefetchManager {

    private final int startWindow;
    private final int maxWindow;
    private final int totalBudgetBlocks;
    private final int randomThreshold;
    private final long idleResetNanos;

    private final Map<String, SequentialDetector> detectors = new ConcurrentHashMap<>();
    private final Set<Long> resident = ConcurrentHashMap.newKeySet();

    private final AtomicLong issued = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong wasted = new AtomicLong();

    public PrefetchManager(SchedulerConfig config) {
        this.startWindow = config.getPrefetchStartBlocks();
        this.maxWindow = config.getPrefetchMaxWindowBlocks();
        this.totalBudgetBlocks = config.getPrefetchTotalBudgetBlocks();
        this.randomThreshold = config.getPrefetchRandomThreshold();
        this.idleResetNanos = config.getPrefetchIdleReset().toNanos();
    }

    /** Blocks (if any) worth prefetching after a user read of {@code blockIndex}. */
    public Collection<Long> advise(String streamId, long blockIndex, long nowNanos) {
        SequentialDetector detector = detectors.computeIfAbsent(streamId,
                key -> new SequentialDetector(startWindow, maxWindow, randomThreshold,
                        idleResetNanos));
        SequentialDetector.Advice advice = detector.observe(blockIndex, nowNanos);
        if (advice.getWindowBlocks() <= 0) {
            return List.of();
        }

        int remainingBudget = totalBudgetBlocks - resident.size();
        if (remainingBudget <= 0) {
            return List.of();
        }

        List<Long> candidates = new ArrayList<>();
        for (long next = blockIndex + 1, count = 0;
             count < advice.getWindowBlocks() && count < remainingBudget;
             next++, count++) {
            if (!resident.contains(next)) {
                candidates.add(next);
            }
        }
        return candidates;
    }

    /** Mark blocks as actually issued as prefetches (charges the budget). */
    public void markIssued(Collection<Long> blocks) {
        int newlyCharged = 0;
        for (Long block : blocks) {
            if (resident.add(block)) {
                newlyCharged++;
            }
        }
        if (newlyCharged > 0) {
            issued.addAndGet(newlyCharged);
        }
    }

    /** A user read consumed a block that was loaded by prefetch (a hit). */
    public void markConsumed(long blockIndex) {
        if (resident.remove(blockIndex)) {
            hits.incrementAndGet();
        }
    }

    /** Prefetched blocks evicted/forgotten without ever being consumed (waste). */
    public void markWasted(Collection<Long> blocks) {
        int wastedCount = 0;
        for (Long block : blocks) {
            if (resident.remove(block)) {
                wastedCount++;
            }
        }
        if (wastedCount > 0) {
            wasted.addAndGet(wastedCount);
        }
    }

    public long issuedBlocks() {
        return issued.get();
    }

    public long hitBlocks() {
        return hits.get();
    }

    public long wastedBlocks() {
        return wasted.get();
    }
}
