package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.io.IoCompletion;
import com.github.guoyyit.asyncioschedulerjava.io.IoPriority;
import com.github.guoyyit.asyncioschedulerjava.io.IoTicket;
import com.github.guoyyit.asyncioschedulerjava.io.QueueRejectedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncIOSchedulerConcurrencyTest {

    @TempDir
    Path tempDir;

    private SchedulerConfig fastConfig() {
        SchedulerConfig config = new SchedulerConfig();
        config.setBatchLinger(java.time.Duration.ofMillis(2));
        config.setMaxBatchBytes(16 * 1024);
        config.setIoWorkers(4);
        return config;
    }

    @Test
    void confirmedWritesAreReadableAfterRestart() throws Exception {
        Path data = tempDir.resolve("data.bin");
        Path journal = tempDir.resolve("data.journal");
        SchedulerConfig config = fastConfig();
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(config, data, journal)) {
            scheduler.start();
            IoTicket ticket = scheduler.submitWrite("app", 0, "payload".getBytes());
            await(ticket.completion());
        }
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(fastConfig(), data, journal)) {
            scheduler.start();
            IoTicket read = scheduler.submitRead("app", 0, 7);
            assertArrayEquals("payload".getBytes(), toArray(await(read.completion())));
        }
    }

    @Test
    void manyConcurrentSubmittersDoNotBlockEachOther() throws Exception {
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                fastConfig(), tempDir.resolve("c.bin"), tempDir.resolve("c.journal"))) {
            scheduler.start();
            int submitterCount = 16;
            int writesPerSubmitter = 50;
            ExecutorService pool = Executors.newFixedThreadPool(submitterCount);
            CountDownLatch done = new CountDownLatch(submitterCount);
            AtomicInteger failures = new AtomicInteger();
            for (int s = 0; s < submitterCount; s++) {
                long baseOffset = (long) s * writesPerSubmitter * 128;
                pool.submit(() -> {
                    try {
                        List<IoTicket> tickets = new ArrayList<>();
                        for (int i = 0; i < writesPerSubmitter; i++) {
                            tickets.add(scheduler.submitWrite("s" + baseOffset,
                                    baseOffset + i * 128, bytes(i)));
                        }
                        for (IoTicket ticket : tickets) {
                            await(ticket.completion());
                        }
                    } catch (Throwable t) {
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS));
            pool.shutdownNow();
            assertTrue(failures.get() == 0, "concurrent failures: " + failures.get());
            assertTrue(scheduler.pendingCount() == 0);
        }
    }

    @Test
    void readsObservePreviouslyConfirmedWrites() throws Exception {
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                fastConfig(), tempDir.resolve("rw.bin"), tempDir.resolve("rw.journal"))) {
            scheduler.start();
            for (int i = 0; i < 20; i++) {
                await(scheduler.submitWrite("app", i * 64, bytes(i + 100)).completion());
                IoTicket read = scheduler.submitRead("app", i * 64, 8);
                assertArrayEquals(bytes(i + 100), toArray(await(read.completion())));
            }
        }
    }

    @Test
    void globalAndPerSubmitterQueueLimitsAreBounded() throws Exception {
        SchedulerConfig config = fastConfig();
        config.setMaxBatchBytes(Integer.MAX_VALUE);
        config.setBatchLinger(java.time.Duration.ofSeconds(60));
        config.setMaxPendingRequests(8);
        config.setMaxPendingPerSubmitter(4);
        config.setIoWorkers(1);
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("q.bin"), tempDir.resolve("q.journal"))) {
            scheduler.start();
            AtomicInteger rejected = new AtomicInteger();
            for (int i = 0; i < 20; i++) {
                try {
                    scheduler.submitWrite("hog", i * 64, bytes(i));
                } catch (QueueRejectedException e) {
                    rejected.incrementAndGet();
                }
            }
            assertTrue(rejected.get() > 0, "slow submitter must be rejected once limits fill");
            assertTrue(scheduler.pendingCount("hog") <= config.getMaxPendingPerSubmitter());
        }
    }

    @Test
    void agingPreventsLowPriorityStarvation() throws Exception {
        SchedulerConfig config = fastConfig();
        config.setIoWorkers(1);
        config.setFairnessPolicy(com.github.guoyyit.asyncioschedulerjava.config.FairnessPolicy.AGING_STRICT);
        config.setAgingThreshold(java.time.Duration.ofMillis(100));
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("p.bin"), tempDir.resolve("p.journal"))) {
            scheduler.start();
            // Continuous high-priority traffic must not permanently starve the
            // one low-priority read: aging promotes it after the threshold.
            ExecutorService producer = Executors.newSingleThreadExecutor();
            producer.submit(() -> {
                try {
                    for (int i = 0; i < 500 && !Thread.currentThread().isInterrupted(); i++) {
                        IoTicket high = scheduler.submitRead("hot", "hot-" + i,
                                IoPriority.HIGH, 1_000_000L + i * 8, 8);
                        await(high.completion());
                    }
                } catch (InterruptedException ignored) {
                    // shutdown
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            IoTicket lowRead = scheduler.submitRead("cold", "cold-stream",
                    IoPriority.LOW, 0, 8);
            await(lowRead.completion());
            producer.shutdownNow();
            producer.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static byte[] bytes(int value) {
        return new byte[] {
                (byte) value, (byte) (value + 1), (byte) (value + 2), (byte) (value + 3),
                (byte) (value + 4), (byte) (value + 5), (byte) (value + 6), (byte) (value + 7)};
    }

    private static IoCompletion await(CompletableFuture<IoCompletion> future) throws Exception {
        IoCompletion completion = future.get(30, TimeUnit.SECONDS);
        if (!completion.success()) {
            throw new AssertionError("request failed", completion.error());
        }
        return completion;
    }

    private static byte[] toArray(IoCompletion completion) {
        java.nio.ByteBuffer buffer = completion.data();
        byte[] result = new byte[buffer.remaining()];
        buffer.duplicate().get(result);
        return result;
    }
}
