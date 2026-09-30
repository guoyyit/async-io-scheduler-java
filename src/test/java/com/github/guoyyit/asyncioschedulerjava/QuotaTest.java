package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.api.QuotaExceededException;
import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.model.Priority;
import com.github.guoyyit.asyncioschedulerjava.model.QuotaAction;
import com.github.guoyyit.asyncioschedulerjava.model.Ticket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuotaTest {

    private SchedulerConfig config(QuotaAction action) {
        SchedulerConfig config = new SchedulerConfig();
        config.setBlockSize(64);
        config.setMaxBatchDelay(Duration.ofMillis(5));
        config.setQuotaWindow(Duration.ofMillis(200));
        config.setQuotaAction(action);
        return config;
    }

    @Test
    void rejectPolicyFailsTicketWhenReadQuotaExceeded(@TempDir Path directory) throws Exception {
        SchedulerConfig config = config(QuotaAction.REJECT);
        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            scheduler.setQuota("limited", 64, 0);

            ByteBuffer first = ByteBuffer.allocate(64);
            scheduler.submitRead("limited", Priority.NORMAL, 0, first)
                    .get(5, TimeUnit.SECONDS);

            Ticket<Integer> second = scheduler.submitRead("limited", Priority.NORMAL,
                    64, ByteBuffer.allocate(64));
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> second.get(5, TimeUnit.SECONDS));
            assertTrue(error.getCause() instanceof QuotaExceededException);
            assertTrue(scheduler.metricsSnapshot().getQuotaDelayedRequests() >= 1);
        }
    }

    @Test
    void delayPolicyHonoursQuotaAfterWindowRefill(@TempDir Path directory) throws Exception {
        SchedulerConfig config = config(QuotaAction.DELAY);
        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            scheduler.setQuota("limited", 64, 64);

            scheduler.submitRead("limited", Priority.NORMAL, 0, ByteBuffer.allocate(64))
                    .get(5, TimeUnit.SECONDS);

            long start = System.nanoTime();
            scheduler.submitRead("limited", Priority.NORMAL, 64, ByteBuffer.allocate(64))
                    .get(5, TimeUnit.SECONDS);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;
            assertTrue(elapsedMillis >= 150,
                    "expected quota delay, elapsed=" + elapsedMillis);

            var snapshot = scheduler.quotaSnapshot("limited");
            assertEquals(64, snapshot.getReadLimitBytesPerSec());
            assertTrue(snapshot.getReadUsedBytesInWindow() <= 64);
            assertTrue(snapshot.getDelayedRequests() >= 1);
        }
    }

    @Test
    void otherSubmittersAreNotBlockedByOneSubmitterOverQuota(@TempDir Path directory)
            throws Exception {
        SchedulerConfig config = config(QuotaAction.DELAY);
        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            scheduler.setQuota("limited", 64, 64);

            scheduler.submitRead("limited", Priority.HIGH, 0, ByteBuffer.allocate(64))
                    .get(5, TimeUnit.SECONDS);
            Ticket<Integer> blocked = scheduler.submitRead("limited", Priority.HIGH,
                    64, ByteBuffer.allocate(64));

            long start = System.nanoTime();
            scheduler.submitRead("free", Priority.NORMAL, 0, ByteBuffer.allocate(64))
                    .get(5, TimeUnit.SECONDS);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;
            assertTrue(elapsedMillis < 100,
                    "free submitter must not wait for throttled one, elapsed="
                            + elapsedMillis);
            blocked.get(5, TimeUnit.SECONDS);
        }
    }
}
