package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.io.IoCompletion;
import com.github.guoyyit.asyncioschedulerjava.io.IoPriority;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Read-consistency semantics: confirmed writes must be observed by
 * overlapping reads, with or without prefetch, and concurrent overlapping
 * reads/writes must yield whole before- or after-images.
 */
class ReadConsistencyTest {

    @TempDir
    Path tempDir;

    private SchedulerConfig baseConfig() {
        SchedulerConfig config = new SchedulerConfig();
        config.setIoWorkers(1);
        config.setBatchLinger(Duration.ofMillis(2));
        config.setPrefetchMinBytes(4096);
        config.setPrefetchMaxBytes(4096);
        config.setSequentialHitThreshold(2);
        return config;
    }

    @Test
    void overlappingReadAfterConfirmedWriteBypassesStalePrefetch() throws Exception {
        SchedulerConfig config = baseConfig();
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("p.bin"), tempDir.resolve("p.journal"))) {
            scheduler.start();
            byte[] initial = filled(16384, 0x0A);
            await(scheduler.submitWrite("app", 0, initial).completion());

            // Three sequential reads arm the prefetcher (threshold = 2); the
            // third one issues a prefetch of [12288, 16384).
            await(scheduler.submitRead("app", "s", IoPriority.NORMAL, 0, 4096).completion());
            await(scheduler.submitRead("app", "s", IoPriority.NORMAL, 4096, 4096).completion());
            await(scheduler.submitRead("app", "s", IoPriority.NORMAL, 8192, 4096).completion());
            // Single worker: once this unrelated read finishes, the prefetch
            // fill above has completed too, so the cache holds stale 0x0A bytes.
            await(scheduler.submitRead("app", "barrier", IoPriority.NORMAL, 0, 1).completion());
            assertTrue(scheduler.metrics().prefetchBytesIssued > 0,
                    "prefetch must have been issued before the overwrite");

            byte[] fresh = filled(4096, 0x0B);
            await(scheduler.submitWrite("app", 12288, fresh).completion());

            IoCompletion after = await(
                    scheduler.submitRead("app", "s", IoPriority.NORMAL, 12288, 4096).completion());
            assertArrayEquals(fresh, toArray(after),
                    "read after confirmed write must not return stale prefetched bytes");
            assertTrue(scheduler.metrics().prefetchInvalidations > 0,
                    "write commit must invalidate overlapping prefetch cache");

            // A second read observes the same confirmed content.
            IoCompletion again = await(
                    scheduler.submitRead("app", "s", IoPriority.NORMAL, 12288, 4096).completion());
            assertArrayEquals(fresh, toArray(again));
        }
    }

    @Test
    void concurrentOverlappingReadWriteYieldsWholeSnapshots() throws Exception {
        SchedulerConfig config = baseConfig();
        config.setIoWorkers(2);
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config, tempDir.resolve("rw.bin"), tempDir.resolve("rw.journal"))) {
            scheduler.start();
            int region = 2048;
            await(scheduler.submitWrite("app", 0, filled(region, 0x0A)).completion());

            AtomicReference<Throwable> failure = new AtomicReference<>();
            ExecutorService pool = Executors.newFixedThreadPool(3);
            CountDownLatch done = new CountDownLatch(3);

            pool.submit(() -> {
                try {
                    for (int i = 0; i < 30; i++) {
                        await(scheduler.submitWrite("writer", 0, filled(region, 0x0B)).completion());
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });
            for (int r = 0; r < 2; r++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 60; i++) {
                            IoCompletion c = await(scheduler
                                    .submitRead("reader-" + Thread.currentThread().getId(),
                                            "shared", IoPriority.NORMAL, 0, region)
                                    .completion());
                            byte[] data = toArray(c);
                            boolean allA = true;
                            boolean allB = true;
                            for (byte b : data) {
                                allA &= b == (byte) 0x0A;
                                allB &= b == (byte) 0x0B;
                            }
                            if (!allA && !allB) {
                                throw new AssertionError(
                                        "torn read: neither full before- nor after-image");
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(60, TimeUnit.SECONDS), "readers/writer must finish");
            pool.shutdownNow();
            assertNull(failure.get(), "concurrent overlapping IO failed");
        }
    }

    @Test
    void prefetchOnAndOffProduceSameObservableResults() throws Exception {
        List<byte[]> withPrefetch = runScript(true, "on");
        List<byte[]> withoutPrefetch = runScript(false, "off");
        assertEquals(withPrefetch.size(), withoutPrefetch.size());
        for (int i = 0; i < withPrefetch.size(); i++) {
            assertArrayEquals(withPrefetch.get(i), withoutPrefetch.get(i),
                    "observable read result " + i + " differs between prefetch on/off");
        }
    }

    private List<byte[]> runScript(boolean prefetch, String name) throws Exception {
        SchedulerConfig config = baseConfig();
        config.setPrefetchEnabled(prefetch);
        List<byte[]> observed = new ArrayList<>();
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(config,
                tempDir.resolve("script-" + name + ".bin"),
                tempDir.resolve("script-" + name + ".journal"))) {
            scheduler.start();
            await(scheduler.submitWrite("app", 0, filled(16384, 0x01)).completion());
            observed.add(toArray(await(
                    scheduler.submitRead("app", "s", IoPriority.NORMAL, 0, 4096).completion())));
            observed.add(toArray(await(
                    scheduler.submitRead("app", "s", IoPriority.NORMAL, 4096, 4096).completion())));
            observed.add(toArray(await(
                    scheduler.submitRead("app", "s", IoPriority.NORMAL, 8192, 4096).completion())));
            await(scheduler.submitRead("app", "barrier", IoPriority.NORMAL, 0, 1).completion());
            await(scheduler.submitWrite("app", 12288, filled(4096, 0x02)).completion());
            observed.add(toArray(await(
                    scheduler.submitRead("app", "s", IoPriority.NORMAL, 12288, 4096).completion())));
            observed.add(toArray(await(
                    scheduler.submitRead("app", "s", IoPriority.NORMAL, 0, 16384).completion())));
        }
        return observed;
    }

    @Test
    void confirmedWriteSurvivesRestartWithPrefetchingActive() throws Exception {
        Path data = tempDir.resolve("restart.bin");
        Path journal = tempDir.resolve("restart.journal");
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(baseConfig(), data, journal)) {
            scheduler.start();
            await(scheduler.submitWrite("app", 0, filled(8192, 0x0A)).completion());
            await(scheduler.submitRead("app", "s", IoPriority.NORMAL, 0, 4096).completion());
            await(scheduler.submitRead("app", "s", IoPriority.NORMAL, 4096, 4096).completion());
            await(scheduler.submitWrite("app", 4096, filled(4096, 0x0C)).completion());
        }
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(baseConfig(), data, journal)) {
            scheduler.start();
            IoCompletion c = await(
                    scheduler.submitRead("app", "s", IoPriority.NORMAL, 0, 8192).completion());
            byte[] expected = filled(8192, 0x0A);
            Arrays.fill(expected, 4096, 8192, (byte) 0x0C);
            assertArrayEquals(expected, toArray(c),
                    "post-restart read must return persisted confirmed content");
        }
    }

    private static byte[] filled(int length, int value) {
        byte[] data = new byte[length];
        Arrays.fill(data, (byte) value);
        return data;
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
