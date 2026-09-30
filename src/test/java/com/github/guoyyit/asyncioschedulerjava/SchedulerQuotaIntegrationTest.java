package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.OverQuotaAction;
import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.io.IoCompletion;
import com.github.guoyyit.asyncioschedulerjava.io.IoTicket;
import com.github.guoyyit.asyncioschedulerjava.io.IoType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerQuotaIntegrationTest {

    @TempDir
    Path tempDir;

    private SchedulerConfig config() {
        SchedulerConfig config = new SchedulerConfig();
        config.setBatchLinger(Duration.ofMillis(2));
        config.setIoWorkers(2);
        return config;
    }

    @Test
    void rejectPolicyCompletesOverQuotaRequestsExceptionally() throws Exception {
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(config(),
                tempDir.resolve("r.bin"), tempDir.resolve("r.journal"))) {
            scheduler.start();
            scheduler.setOverQuotaAction(OverQuotaAction.REJECT);
            scheduler.setQuota("limited", IoType.READ, 64);

            IoTicket first = scheduler.submitRead("limited", 0, 64);
            await(first.completion());

            IoTicket rejected = scheduler.submitRead("limited", 0, 64);
            IoCompletion completion = rejected.completion().get(10, TimeUnit.SECONDS);
            assertFalse(completion.success());

            IoTicket other = scheduler.submitRead("unlimited", 0, 8);
            assertTrue(other.completion().get(10, TimeUnit.SECONDS).success(),
                    "other submitters must not be affected");
        }
    }

    @Test
    void delayPolicyEventuallyServesThrottledRequests() throws Exception {
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(config(),
                tempDir.resolve("d.bin"), tempDir.resolve("d.journal"))) {
            scheduler.start();
            scheduler.setOverQuotaAction(OverQuotaAction.DELAY);
            scheduler.setQuota("limited", IoType.WRITE, 256);

            IoTicket first = scheduler.submitWrite("limited", 0, new byte[256]);
            await(first.completion());

            long start = System.nanoTime();
            IoTicket second = scheduler.submitWrite("limited", 256, new byte[256]);
            await(second.completion());
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMs >= 800, "expected roughly 1s of throttling, was " + elapsedMs);
            assertTrue(scheduler.metrics().quotaDelayedRequests >= 1L);
        }
    }

    @Test
    void degradePolicyStillServesAndCountsDegradation() throws Exception {
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(config(),
                tempDir.resolve("g.bin"), tempDir.resolve("g.journal"))) {
            scheduler.start();
            scheduler.setOverQuotaAction(OverQuotaAction.DEGRADE);
            scheduler.setQuota("limited", IoType.READ, 64);

            await(scheduler.submitRead("limited", 0, 64).completion());
            IoTicket degraded = scheduler.submitRead("limited", 0, 64);
            assertTrue(await(degraded.completion()).success());
            assertTrue(scheduler.metrics().degradedRequests >= 1L);
        }
    }

    private static IoCompletion await(CompletableFuture<IoCompletion> future) throws Exception {
        IoCompletion completion = future.get(30, TimeUnit.SECONDS);
        if (!completion.success()) {
            throw new AssertionError("request failed", completion.error());
        }
        return completion;
    }
}
