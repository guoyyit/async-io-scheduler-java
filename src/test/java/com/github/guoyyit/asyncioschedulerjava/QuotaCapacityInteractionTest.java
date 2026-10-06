package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.CapacityPolicy;
import com.github.guoyyit.asyncioschedulerjava.config.OverQuotaAction;
import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.io.IoCompletion;
import com.github.guoyyit.asyncioschedulerjava.io.IoTicket;
import com.github.guoyyit.asyncioschedulerjava.io.IoType;
import com.github.guoyyit.asyncioschedulerjava.io.QueueWaitTimeoutException;
import com.github.guoyyit.asyncioschedulerjava.quota.QuotaExceededException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Interaction between bandwidth quota delays and bounded capacity: quota
 * delays are accounted, never bypass capacity control, and degraded retries
 * are bounded.
 */
class QuotaCapacityInteractionTest {

    @TempDir
    Path tempDir;

    private SchedulerConfig baseConfig() {
        SchedulerConfig config = new SchedulerConfig();
        config.setIoWorkers(1);
        config.setBatchLinger(Duration.ofMillis(2));
        return config;
    }

    @Test
    void quotaDelayDoesNotBypassCapacityControl() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setCapacityPolicy(CapacityPolicy.WAIT);
        config.setCapacityWaitTimeout(Duration.ofSeconds(15));
        config.setMaxPendingRequests(1);
        config.setMaxPendingPerSubmitter(16);
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("q.bin"), tempDir.resolve("q.journal"))) {
            scheduler.start();
            scheduler.setQuota("slow", IoType.WRITE, 4096);

            AtomicInteger maxObservedPending = new AtomicInteger();
            java.util.concurrent.atomic.AtomicBoolean observing =
                    new java.util.concurrent.atomic.AtomicBoolean(true);
            Thread observer = new Thread(() -> {
                while (observing.get()) {
                    maxObservedPending.accumulateAndGet(scheduler.pendingCount(), Math::max);
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            observer.setDaemon(true);
            observer.start();

            IoTicket first = scheduler.submitWrite("slow", 0, new byte[4096]);
            awaitTrue(() -> scheduler.pendingCount() == 1, "first write not pending");
            // Capacity full: this submission parks until the first completes;
            // afterwards it is throttled by the write quota before executing.
            IoTicket second = scheduler.submitWrite("slow", 4096, new byte[4096]);

            assertTrue(await(first.completion(), 30).success());
            assertTrue(await(second.completion(), 30).success(),
                    "quota-delayed request admitted after capacity release must complete");
            observing.set(false);
            observer.join(TimeUnit.SECONDS.toMillis(10));
            assertTrue(scheduler.metrics().capacityWaits >= 1,
                    "capacity wait not recorded");
            assertTrue(scheduler.metrics().quotaDelayedRequests >= 1,
                    "quota delay not recorded");
            assertTrue(scheduler.metrics().totalQuotaDelayNanos > 0,
                    "quota delay duration not recorded");
            assertTrue(maxObservedPending.get() <= config.getMaxPendingRequests(),
                    "pending depth exceeded the bound: " + maxObservedPending.get());
        }
    }

    @Test
    void degradedRetriesAreBoundedWithClearFailure() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setOverQuotaAction(OverQuotaAction.DEGRADE);
        config.setMaxDegradeRequeues(0);
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("d.bin"), tempDir.resolve("d.journal"))) {
            scheduler.start();
            scheduler.setQuota("limited", IoType.READ, 64);

            assertTrue(await(scheduler.submitRead("limited", 0, 64).completion(), 30).success());
            IoCompletion overQuota =
                    await(scheduler.submitRead("limited", 0, 64).completion(), 30);
            assertFalse(overQuota.success(), "over-quota request must fail when retries exhaust");
            assertTrue(overQuota.error() instanceof QuotaExceededException,
                    "failure reason must be QuotaExceededException, was " + overQuota.error());
            assertTrue(scheduler.metrics().degradeRequeuesExhausted >= 1,
                    "exhausted degrade retries not recorded");
        }
    }

    @Test
    void requestsWaitingBeyondMaxQueueWaitFailWithClearReason() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setMaxQueueWait(Duration.ofMillis(100));
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("w.bin"), tempDir.resolve("w.journal"))) {
            scheduler.start();
            scheduler.setQuota("slow", IoType.READ, 1024);

            // First read drains the bucket; the second blocks the only worker
            // in quota DELAY for roughly a second.
            assertTrue(await(scheduler.submitRead("slow", 0, 1024).completion(), 30).success());
            IoTicket throttled = scheduler.submitRead("slow", 1024, 1024);
            IoTicket queued = scheduler.submitRead("other", 2048, 8);

            IoCompletion queuedResult = await(queued.completion(), 30);
            assertFalse(queuedResult.success(),
                    "request stuck behind quota delay must fail, not wait forever");
            assertTrue(queuedResult.error() instanceof QueueWaitTimeoutException,
                    "failure reason must be QueueWaitTimeoutException, was "
                            + queuedResult.error());
            assertTrue(scheduler.metrics().queueWaitTimeouts >= 1,
                    "queue wait timeout not recorded");
            // The throttled request itself is bounded by its quota and completes.
            assertTrue(await(throttled.completion(), 30).success());
        }
    }

    private static IoCompletion await(CompletableFuture<IoCompletion> future, long seconds)
            throws Exception {
        return future.get(seconds, TimeUnit.SECONDS);
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
