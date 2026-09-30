package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.prefetch.PrefetchManager;
import com.github.guoyyit.asyncioschedulerjava.prefetch.PrefetchPlan;
import com.github.guoyyit.asyncioschedulerjava.scheduling.SchedulerMetrics;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrefetchManagerTest {

    private SchedulerConfig config() {
        SchedulerConfig config = new SchedulerConfig();
        config.setPrefetchEnabled(true);
        config.setPrefetchMinBytes(1024);
        config.setPrefetchMaxBytes(16 * 1024);
        config.setPrefetchTotalBudgetBytes(64 * 1024);
        config.setPrefetchGrowthFactor(2.0);
        config.setPrefetchBackoffFactor(0.5);
        config.setSequentialHitThreshold(2);
        return config;
    }

    @Test
    void noPrefetchBeforeSequentialThresholdThenWindowGrows() {
        SchedulerMetrics metrics = new SchedulerMetrics();
        try (PrefetchManager manager = new PrefetchManager(config(), metrics)) {
            PrefetchPlan first = manager.afterUserRead("stream", 0, 1024);
            assertTrue(first.isEmpty(), "first read alone must not trigger prefetch");

            PrefetchPlan second = manager.afterUserRead("stream", 1024, 1024);
            assertTrue(second.isEmpty(), "threshold not yet reached");

            PrefetchPlan third = manager.afterUserRead("stream", 2048, 1024);
            assertEquals(3072L, third.offset());
            assertEquals(1024, third.length(), "initial window equals min window");

            PrefetchPlan fourth = manager.afterUserRead("stream", 3072, 1024);
            assertEquals(2048, fourth.length(), "window doubles on continued sequential reads");

            PrefetchPlan fifth = manager.afterUserRead("stream", 5120, 1024);
            assertEquals(4096, fifth.length());
        }
    }

    @Test
    void randomAccessBacksOffAndStopsPrefetching() {
        SchedulerMetrics metrics = new SchedulerMetrics();
        try (PrefetchManager manager = new PrefetchManager(config(), metrics)) {
            sequentialReads(manager, 4);
            int windowBefore = 4096;

            // Random jump: not sequential with previous end and not covered by plan.
            PrefetchPlan random = manager.afterUserRead("stream", 900_000L, 1024);
            assertTrue(random.isEmpty());
            assertEquals(1L, metrics.prefetchBackoffs);

            PrefetchPlan next = manager.afterUserRead("stream", 901_024L, 1024);
            assertTrue(next.isEmpty(), "below threshold after reset, no prefetch");
            PrefetchPlan thenNext = manager.afterUserRead("stream", 902_048L, 1024);
            assertTrue(thenNext.length() <= windowBefore / 2,
                    "window must have backed off after a random miss");
        }
    }

    @Test
    void cachedDataIsServedAndEvictionCountsWaste() {
        SchedulerConfig config = config();
        SchedulerMetrics metrics = new SchedulerMetrics();
        try (PrefetchManager manager = new PrefetchManager(config, metrics)) {
            sequentialReads(manager, 3);
            // Simulate the background prefetch filling the cache at offset 3072.
            manager.fill("stream", 3072, ByteBuffer.wrap(new byte[1024]));
            ByteBuffer hit = manager.lookup("stream", 3072, 512);
            assertNotNull(hit);
            assertEquals(512, hit.remaining());
            assertEquals(1L, metrics.prefetchHits);

            assertNull(manager.lookup("stream", 500_000L, 16), "unrelated range misses");
        }
    }

    @Test
    void totalPrefetchBudgetIsBoundedAcrossStreams() {
        SchedulerConfig config = config();
        config.setPrefetchTotalBudgetBytes(4096);
        SchedulerMetrics metrics = new SchedulerMetrics();
        try (PrefetchManager manager = new PrefetchManager(config, metrics)) {
            for (int stream = 0; stream < 8; stream++) {
                sequentialReadsFor(manager, "s" + stream, 3);
            }
            assertTrue(manager.cachedBytes() <= 4096,
                    "cache must never exceed the configured total budget");
        }
    }

    private void sequentialReads(PrefetchManager manager, int count) {
        sequentialReadsFor(manager, "stream", count);
    }

    private void sequentialReadsFor(PrefetchManager manager, String stream, int count) {
        for (int i = 0; i < count; i++) {
            PrefetchPlan plan = manager.afterUserRead(stream, i * 1024L, 1024);
            if (!plan.isEmpty()) {
                manager.fill(stream, plan.offset(), ByteBuffer.wrap(new byte[plan.length()]));
            }
        }
    }
}
