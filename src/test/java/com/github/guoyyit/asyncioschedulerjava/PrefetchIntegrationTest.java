package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.model.Priority;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PrefetchIntegrationTest {

    private SchedulerConfig config() {
        SchedulerConfig config = new SchedulerConfig();
        config.setBlockSize(64);
        config.setMaxBatchDelay(Duration.ofMillis(5));
        config.setPrefetchStartBlocks(2);
        config.setPrefetchMaxWindowBlocks(8);
        config.setPrefetchTotalBudgetBlocks(64);
        config.setPrefetchRandomThreshold(3);
        config.setPrefetchIdleReset(Duration.ofMillis(100));
        return config;
    }

    @Test
    void sequentialReadsIssuePrefetchAndProduceHits(@TempDir Path directory) throws Exception {
        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config())) {
            for (int block = 0; block < 12; block++) {
                ByteBuffer dst = ByteBuffer.allocate(64);
                scheduler.submitRead("seq", Priority.NORMAL, block * 64L, dst)
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
            }
            var snapshot = scheduler.metricsSnapshot();
            assertTrue(snapshot.getPrefetchIssuedBlocks() > 0,
                    "expected prefetched blocks, got " + snapshot.getPrefetchIssuedBlocks());
            assertTrue(snapshot.getPrefetchHitBlocks() > 0,
                    "expected prefetch hits, got " + snapshot.getPrefetchHitBlocks());
        }
    }

    @Test
    void randomAccessBacksOffPrefetch(@TempDir Path directory) throws Exception {
        long[] pattern = {0L, 900L, 5L, 800L, 3L, 700L, 1L, 600L};
        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config())) {
            for (long block : pattern) {
                ByteBuffer dst = ByteBuffer.allocate(64);
                scheduler.submitRead("rnd", Priority.NORMAL, block * 64L, dst)
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
            }
            var snapshot = scheduler.metricsSnapshot();
            int issued = (int) snapshot.getPrefetchIssuedBlocks();
            int hits = (int) snapshot.getPrefetchHitBlocks();
            assertTrue(issued <= 4, "random access should back off, issued=" + issued);
            assertTrue(hits <= 1, "random access should rarely hit, hits=" + hits);
        }
    }
}
