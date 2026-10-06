package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.CapacityExceededPolicy;
import com.github.guoyyit.asyncioschedulerjava.config.FairnessPolicy;
import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.io.IoCompletion;
import com.github.guoyyit.asyncioschedulerjava.io.IoPriority;
import com.github.guoyyit.asyncioschedulerjava.io.IoTicket;
import com.github.guoyyit.asyncioschedulerjava.io.QueueRejectedException;
import com.github.guoyyit.asyncioschedulerjava.io.RejectionReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Overload behaviour: bounded waiting vs immediate rejection, distinguishable
 * rejection reasons, submitter isolation, aging and quota/capacity interplay.
 */
class CapacityPolicyTest {

    @TempDir
    Path tempDir;

    private SchedulerConfig baseConfig() {
        SchedulerConfig config = new SchedulerConfig();
        config.setIoWorkers(1);
        config.setBatchLinger(Duration.ofMillis(2));
        config.setMaxBatchBytes(16 * 1024);
        config.setShutdownTimeout(Duration.ofMillis(500));
        return config;
    }

    @Test
    void waitModeAdmitsSubmitterAfterCapacityIsFreed() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setMaxPendingRequests(1);
        config.setMaxPendingPerSubmitter(1);
        config.setCapacityExceededPolicy(CapacityExceededPolicy.WAIT);
        config.setCapacityWaitTimeout(Duration.ofSeconds(5));
        config.setBatchLinger(Duration.ofMillis(400));
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("wait.bin"), tempDir.resolve("wait.journal"))) {
            scheduler.start();
            IoTicket first = scheduler.submitWrite("a", 0, new byte[8]);

            AtomicReference<IoTicket> admitted = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                try {
                    admitted.set(scheduler.submitWrite("a", 64, new byte[8]));
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            waiter.start();

            IoTicket second = pollFor(admitted, 10_000);
            assertNull(failure.get(), "waiting submitter must not fail while capacity frees up");
            await(first.completion());
            await(second.completion());
            waiter.join(5_000);
            assertTrue(scheduler.metrics().capacityWaits >= 1,
                    "second submission must have waited for capacity");
            assertTrue(scheduler.metrics().capacityWaitNanos > 0,
                    "wait time must be accounted");
            assertEquals(0, scheduler.pendingCount());
        }
    }

    @Test
    void waitModeTimesOutWithDistinguishableReason() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setMaxPendingRequests(1);
        config.setMaxPendingPerSubmitter(1);
        config.setCapacityExceededPolicy(CapacityExceededPolicy.WAIT);
        config.setCapacityWaitTimeout(Duration.ofMillis(300));
        config.setBatchLinger(Duration.ofSeconds(60));
        config.setMaxBatchBytes(Integer.MAX_VALUE);
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("timeout.bin"), tempDir.resolve("timeout.journal"))) {
            scheduler.start();
            scheduler.submitWrite("a", 0, new byte[8]);
            long start = System.nanoTime();
            try {
                scheduler.submitWrite("a", 64, new byte[8]);
                fail("second submission must time out while capacity is held");
            } catch (QueueRejectedException e) {
                assertEquals(RejectionReason.WAIT_TIMEOUT, e.reason(),
                        "timeout rejection must carry WAIT_TIMEOUT reason");
            }
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;
            assertTrue(elapsedMillis >= 200L, "must wait up to the configured timeout");
            assertTrue(elapsedMillis < 10_000L, "waiting must be bounded");
            assertEquals(1L, scheduler.metrics().rejectedWaitTimeout);
        }
    }

    @Test
    void rejectModeDistinguishesGlobalAndSubmitterCapacity() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setMaxPendingRequests(4);
        config.setMaxPendingPerSubmitter(2);
        config.setBatchLinger(Duration.ofSeconds(60));
        config.setMaxBatchBytes(Integer.MAX_VALUE);
        List<IoTicket> accepted = new ArrayList<>();
        AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("reject.bin"), tempDir.resolve("reject.journal"));
        scheduler.start();
        accepted.add(scheduler.submitWrite("hog", 0, new byte[8]));
        accepted.add(scheduler.submitWrite("hog", 64, new byte[8]));
        QueueRejectedException submitterLimited = expectReject(
                () -> scheduler.submitWrite("hog", 128, new byte[8]));
        assertEquals(RejectionReason.SUBMITTER_CAPACITY, submitterLimited.reason(),
                "per-submitter limit must be distinguishable");

        accepted.add(scheduler.submitWrite("calm", 192, new byte[8]));
        accepted.add(scheduler.submitWrite("calm", 256, new byte[8]));
        QueueRejectedException globalLimited = expectReject(
                () -> scheduler.submitWrite("other", 320, new byte[8]));
        assertEquals(RejectionReason.GLOBAL_CAPACITY, globalLimited.reason(),
                "global limit must be distinguishable");
        assertEquals(1L, scheduler.metrics().rejectedSubmitterCapacity);
        assertEquals(1L, scheduler.metrics().rejectedGlobalCapacity);

        // Rejections must not stop the scheduler from completing accepted work:
        // close() flushes the buffered writes and every accepted request
        // reaches a successful terminal state.
        scheduler.close();
        for (IoTicket ticket : accepted) {
            await(ticket.completion());
        }
    }

    @Test
    void burstSubmitterCannotStarveOthers() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setMaxPendingRequests(8);
        config.setMaxPendingPerSubmitter(2);
        config.setBatchLinger(Duration.ofSeconds(60));
        config.setMaxBatchBytes(Integer.MAX_VALUE);
        AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("burst.bin"), tempDir.resolve("burst.journal"));
        scheduler.start();

        AtomicInteger hogRejected = new AtomicInteger();
        List<IoTicket> hogAccepted = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            try {
                hogAccepted.add(scheduler.submitWrite("hog", i * 64L, new byte[8]));
            } catch (QueueRejectedException e) {
                assertEquals(RejectionReason.SUBMITTER_CAPACITY, e.reason());
                hogRejected.incrementAndGet();
            }
        }
        assertEquals(18, hogRejected.get(), "hog is capped by its own quota");
        assertTrue(scheduler.pendingCount("hog") <= 2);

        // The hog's burst must not consume the global capacity others need:
        // calm still gets its own per-submitter allowance (2 slots) and the
        // global limit (8) is nowhere near exhausted by the hog alone.
        List<IoTicket> calmAccepted = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            calmAccepted.add(scheduler.submitWrite("calm", 4096 + i * 64L, new byte[8]));
        }
        assertEquals(2, calmAccepted.size(), "calm submitter keeps global capacity access");
        assertEquals(4, scheduler.pendingCount());

        scheduler.close();
        for (IoTicket ticket : hogAccepted) {
            await(ticket.completion());
        }
        for (IoTicket ticket : calmAccepted) {
            await(ticket.completion());
        }
    }

    @Test
    void agedLowPriorityRequestCompletesAndIsObservable() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setFairnessPolicy(FairnessPolicy.AGING_STRICT);
        config.setAgingThreshold(Duration.ofMillis(50));
        config.setMaxPendingRequests(32);
        config.setMaxPendingPerSubmitter(16);
        // Throttle reads so the single worker stays busy and HIGH traffic
        // piles up ahead of the LOW request deterministically.
        config.setDefaultReadQuotaBytesPerSec(64);
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("aging.bin"), tempDir.resolve("aging.journal"))) {
            scheduler.start();
            ExecutorService producer = Executors.newSingleThreadExecutor();
            CountDownLatch stop = new CountDownLatch(1);
            producer.submit(() -> {
                int i = 0;
                while (stop.getCount() > 0 && !Thread.currentThread().isInterrupted()) {
                    try {
                        scheduler.submitRead("hot", "hot-" + (i % 8),
                                IoPriority.HIGH, 1_000_000L + (i++ % 64) * 8L, 8);
                    } catch (QueueRejectedException full) {
                        Thread.yield();
                    }
                }
            });
            // Wait until the throttled worker is actually delaying requests,
            // i.e. HIGH reads are queueing up faster than they are served.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (scheduler.metrics().quotaDelayedRequests < 1) {
                assertTrue(System.nanoTime() < deadline, "quota throttling never kicked in");
                Thread.sleep(5L);
            }
            IoTicket low = scheduler.submitRead("cold", "cold", IoPriority.LOW, 0, 8);
            await(low.completion());
            stop.countDown();
            producer.shutdownNow();
            assertTrue(producer.awaitTermination(10, TimeUnit.SECONDS));
            assertTrue(scheduler.metrics().agedRequests >= 1,
                    "aging promotion must be observable via metrics");
        }
    }

    @Test
    void quotaDelayAndCapacityLimitStayConsistent() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setMaxPendingRequests(3);
        config.setMaxPendingPerSubmitter(10);
        config.setDefaultWriteQuotaBytesPerSec(8192);
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("quota.bin"), tempDir.resolve("quota.journal"))) {
            scheduler.start();
            List<IoTicket> accepted = new ArrayList<>();
            AtomicInteger rejected = new AtomicInteger();
            for (int i = 0; i < 5; i++) {
                try {
                    accepted.add(scheduler.submitWrite("heavy", i * 16384L, new byte[16384]));
                } catch (QueueRejectedException e) {
                    assertEquals(RejectionReason.GLOBAL_CAPACITY, e.reason(),
                            "quota-delayed backlog must surface as capacity rejection");
                    rejected.incrementAndGet();
                }
            }
            assertTrue(rejected.get() >= 1,
                    "slow quota must push submissions against the bounded capacity");
            assertEquals(rejected.get(), (int) scheduler.metrics().rejectedGlobalCapacity);
            for (IoTicket ticket : accepted) {
                await(ticket.completion());
            }
            assertTrue(scheduler.metrics().quotaDelayedRequests >= 1,
                    "quota delay must be observable");
            assertEquals(0, scheduler.metrics().failedRequests,
                    "no accepted request may fail in DELAY mode");
            assertEquals(0, scheduler.pendingCount());
        }
    }

    private static IoTicket pollFor(AtomicReference<IoTicket> reference, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            IoTicket ticket = reference.get();
            if (ticket != null) {
                return ticket;
            }
            Thread.sleep(5L);
        }
        fail("submission was not admitted within " + timeoutMillis + " ms");
        return null;
    }

    private static QueueRejectedException expectReject(Runnable action) {
        try {
            action.run();
        } catch (QueueRejectedException e) {
            return e;
        }
        fail("expected QueueRejectedException");
        return null;
    }

    private static IoCompletion await(CompletableFuture<IoCompletion> future) throws Exception {
        IoCompletion completion = future.get(30, TimeUnit.SECONDS);
        if (!completion.success()) {
            throw new AssertionError("request failed", completion.error());
        }
        return completion;
    }
}
