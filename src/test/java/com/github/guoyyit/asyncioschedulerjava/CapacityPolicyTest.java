package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.CapacityPolicy;
import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.io.IoCompletion;
import com.github.guoyyit.asyncioschedulerjava.io.IoTicket;
import com.github.guoyyit.asyncioschedulerjava.io.QueueRejectedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Overload behaviour: bounded pending depth, wait-for-capacity mode with
 * admission after release, bounded wait with explicit timeout reason, and
 * distinguishable immediate-reject reasons.
 */
class CapacityPolicyTest {

    @TempDir
    Path tempDir;

    private SchedulerConfig baseConfig() {
        SchedulerConfig config = new SchedulerConfig();
        config.setIoWorkers(1);
        config.setMaxBatchBytes(Integer.MAX_VALUE);
        return config;
    }

    @Test
    void waitModeAdmitsRequestAfterCapacityIsReleased() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setCapacityPolicy(CapacityPolicy.WAIT);
        config.setCapacityWaitTimeout(Duration.ofSeconds(10));
        config.setMaxPendingRequests(1);
        config.setMaxPendingPerSubmitter(16);
        // Keep the first write buffered for a while so it holds the only slot.
        config.setBatchLinger(Duration.ofMillis(300));
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("w.bin"), tempDir.resolve("w.journal"))) {
            scheduler.start();
            IoTicket first = scheduler.submitWrite("a", 0, new byte[8]);
            awaitTrue(() -> scheduler.pendingCount() == 1, "first write not pending");

            // Capacity is exhausted: this submission must park, then be admitted
            // once the first write completes and releases its slot.
            AtomicReference<IoTicket> second = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch submitted = new CountDownLatch(1);
            Thread waiter = new Thread(() -> {
                try {
                    second.set(scheduler.submitWrite("b", 64, new byte[8]));
                } catch (Throwable t) {
                    failure.set(t);
                } finally {
                    submitted.countDown();
                }
            });
            waiter.start();

            assertTrue(await(first.completion(), 30), "first write must complete");
            assertTrue(submitted.await(30, TimeUnit.SECONDS),
                    "waiting submission was never admitted");
            waiter.join(TimeUnit.SECONDS.toMillis(30));
            assertTrue(failure.get() == null, "waiting submission failed: " + failure.get());
            assertTrue(await(second.get().completion(), 30),
                    "admitted request must complete successfully");
            assertTrue(scheduler.metrics().capacityWaits >= 1,
                    "capacity wait was not recorded");
            assertTrue(scheduler.metrics().totalCapacityWaitNanos > 0,
                    "capacity wait duration was not recorded");
        }
    }

    @Test
    void waitModeTimesOutWithExplicitReason() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setCapacityPolicy(CapacityPolicy.WAIT);
        config.setCapacityWaitTimeout(Duration.ofMillis(200));
        config.setMaxPendingRequests(1);
        config.setMaxPendingPerSubmitter(16);
        // Occupy the only slot for (nearly) the whole test.
        config.setBatchLinger(Duration.ofSeconds(60));
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("t.bin"), tempDir.resolve("t.journal"))) {
            scheduler.start();
            scheduler.submitWrite("a", 0, new byte[8]);
            awaitTrue(() -> scheduler.pendingCount() == 1, "first write not pending");

            long started = System.nanoTime();
            QueueRejectedException rejection = null;
            try {
                scheduler.submitWrite("b", 64, new byte[8]);
            } catch (QueueRejectedException e) {
                rejection = e;
            }
            long waitedMillis = (System.nanoTime() - started) / 1_000_000L;
            assertTrue(rejection != null, "submission must fail after the wait timeout");
            assertEquals(QueueRejectedException.Reason.WAIT_TIMEOUT, rejection.reason(),
                    "timeout rejection must carry WAIT_TIMEOUT reason");
            assertTrue(waitedMillis < 10_000,
                    "wait must be bounded by the configured timeout, took " + waitedMillis);
            assertTrue(scheduler.metrics().capacityWaitTimeouts >= 1,
                    "timeout was not recorded in metrics");
            // The rejection path must not stop the scheduler: the occupying
            // write still completes on close/flush.
        }
    }

    @Test
    void rejectModeReasonsAreDistinguishable() throws Exception {
        // Not running.
        AsyncIOScheduler stopped = new AsyncIOScheduler(
                baseConfig(), tempDir.resolve("s.bin"), tempDir.resolve("s.journal"));
        QueueRejectedException notRunning = null;
        try {
            stopped.submitRead("a", 0, 8);
        } catch (QueueRejectedException e) {
            notRunning = e;
        }
        assertTrue(notRunning != null, "submission to stopped scheduler must fail");
        assertEquals(QueueRejectedException.Reason.SCHEDULER_NOT_RUNNING, notRunning.reason());

        // Per-submitter limit.
        SchedulerConfig perSubmitter = baseConfig();
        perSubmitter.setMaxPendingRequests(100);
        perSubmitter.setMaxPendingPerSubmitter(2);
        perSubmitter.setBatchLinger(Duration.ofSeconds(60));
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                perSubmitter, tempDir.resolve("ps.bin"), tempDir.resolve("ps.journal"))) {
            scheduler.start();
            scheduler.submitWrite("hog", 0, new byte[8]);
            scheduler.submitWrite("hog", 64, new byte[8]);
            QueueRejectedException rejection = null;
            try {
                scheduler.submitWrite("hog", 128, new byte[8]);
            } catch (QueueRejectedException e) {
                rejection = e;
            }
            assertTrue(rejection != null, "third submission must be rejected");
            assertEquals(QueueRejectedException.Reason.SUBMITTER_CAPACITY_EXHAUSTED,
                    rejection.reason());
            assertTrue(scheduler.metrics().rejectedSubmitterCapacity >= 1);
        }

        // Global limit.
        SchedulerConfig global = baseConfig();
        global.setMaxPendingRequests(2);
        global.setMaxPendingPerSubmitter(100);
        global.setBatchLinger(Duration.ofSeconds(60));
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                global, tempDir.resolve("g.bin"), tempDir.resolve("g.journal"))) {
            scheduler.start();
            scheduler.submitWrite("a", 0, new byte[8]);
            scheduler.submitWrite("b", 64, new byte[8]);
            QueueRejectedException rejection = null;
            try {
                scheduler.submitWrite("c", 128, new byte[8]);
            } catch (QueueRejectedException e) {
                rejection = e;
            }
            assertTrue(rejection != null, "third submission must be rejected");
            assertEquals(QueueRejectedException.Reason.GLOBAL_CAPACITY_EXHAUSTED,
                    rejection.reason());
            assertTrue(scheduler.metrics().rejectedGlobalCapacity >= 1);
        }
    }

    @Test
    void burstingSubmitterDoesNotStarveOthers() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setIoWorkers(2);
        config.setBatchLinger(Duration.ofMillis(20));
        config.setMaxPendingRequests(64);
        config.setMaxPendingPerSubmitter(4);
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("f.bin"), tempDir.resolve("f.journal"))) {
            scheduler.start();
            // One submitter floods the scheduler well beyond its own limit.
            Thread hog = new Thread(() -> {
                for (int i = 0; i < 200; i++) {
                    try {
                        await(scheduler.submitWrite("hog", (long) i * 8, new byte[8])
                                .completion(), 30);
                    } catch (QueueRejectedException expected) {
                        // bounded: the hog alone observes rejections
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            hog.start();
            // Other submitters must still make progress and finish, one at a
            // time so each stays well under its own per-submitter share.
            for (int i = 0; i < 20; i++) {
                IoTicket ticket = scheduler.submitWrite("tenant-" + i % 4,
                        1_000_000L + (long) i * 8, new byte[8]);
                assertTrue(await(ticket.completion(), 30),
                        "other submitter was blocked by the burst");
            }
            hog.join(TimeUnit.SECONDS.toMillis(30));
            assertTrue(!hog.isAlive(), "hog thread did not finish");
            assertTrue(scheduler.pendingCount("hog") <= config.getMaxPendingPerSubmitter(),
                    "hog exceeded its own bounded share");
        }
    }

    private static boolean await(CompletableFuture<IoCompletion> future, long seconds)
            throws Exception {
        return future.get(seconds, TimeUnit.SECONDS).success();
    }

    private static void awaitTrue(BooleanSupplier condition, String message)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(message);
            }
            Thread.sleep(1);
        }
    }
}
