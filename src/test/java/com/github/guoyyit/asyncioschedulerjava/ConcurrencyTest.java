package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.model.Priority;
import com.github.guoyyit.asyncioschedulerjava.model.Ticket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrencyTest {

    @Test
    void manySubmittersConcurrentlyDoNotBlockEachOther(@TempDir Path directory)
            throws Exception {
        SchedulerConfig config = new SchedulerConfig();
        config.setBlockSize(64);
        config.setMaxBatchDelay(Duration.ofMillis(5));
        config.setMaxWriteBatchBlocks(128);

        int threads = 12;
        int perThread = 40;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> results = new ArrayList<>();

        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            for (int t = 0; t < threads; t++) {
                final int submitterId = t;
                results.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        long offset = (submitterId * 10_000L + i) * 64L;
                        byte[] payload = new byte[64];
                        payload[0] = (byte) (submitterId + 1);
                        payload[1] = (byte) i;
                        scheduler.submitWrite("s" + submitterId, Priority.NORMAL,
                                offset, ByteBuffer.wrap(payload)).get(10, TimeUnit.SECONDS);
                        ByteBuffer dst = ByteBuffer.allocate(64);
                        Integer read = scheduler.submitRead("s" + submitterId,
                                Priority.NORMAL, offset, dst).get(10, TimeUnit.SECONDS);
                        assertEquals(64, read);
                        assertEquals((byte) (submitterId + 1), dst.array()[0]);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> result : results) {
                result.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void lowPriorityRequestsAreNotStarvedByHighPriorityStream(@TempDir Path directory)
            throws Exception {
        SchedulerConfig config = new SchedulerConfig();
        config.setBlockSize(64);
        config.setMaxBatchDelay(Duration.ofMillis(2));
        config.setMaxWriteBatchBlocks(4);
        config.setStarvationAgeThreshold(Duration.ofMillis(5));

        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            ExecutorService highProducer = Executors.newSingleThreadExecutor();
            CountDownLatch stopHigh = new CountDownLatch(1);
            Future<?> highFuture = highProducer.submit(() -> {
                int i = 0;
                while (stopHigh.getCount() > 0) {
                    scheduler.submitWrite("hi", Priority.HIGH, 10_000_000L + (long) i * 64,
                            ByteBuffer.wrap(new byte[64]));
                    i++;
                    if (i % 20 == 0) {
                        Thread.sleep(1);
                    }
                }
                return null;
            });

            Ticket<Void> lowTicket = scheduler.submitWrite("low", Priority.LOW, 0,
                    ByteBuffer.wrap(new byte[64]));
            try {
                lowTicket.get(10, TimeUnit.SECONDS);
            } finally {
                stopHigh.countDown();
                highFuture.get(5, TimeUnit.SECONDS);
                highProducer.shutdownNow();
            }
        }
    }
}
