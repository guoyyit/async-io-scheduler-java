package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.io.IoCompletion;
import com.github.guoyyit.asyncioschedulerjava.io.IoTicket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Read-consistency semantics: a read overlapping a confirmed write must
 * observe the written content, with prefetch enabled or disabled, and a read
 * concurrent with an in-flight write must observe a complete before- or
 * after-image, never a mix.
 */
class ReadConsistencyTest {

    @TempDir
    Path tempDir;

    private SchedulerConfig config(boolean prefetch) {
        SchedulerConfig config = new SchedulerConfig();
        config.setBatchLinger(java.time.Duration.ofMillis(2));
        config.setIoWorkers(2);
        config.setPrefetchEnabled(prefetch);
        config.setSequentialHitThreshold(2);
        config.setPrefetchMinBytes(4096);
        config.setPrefetchMaxBytes(8192);
        return config;
    }

    @Test
    void readAfterConfirmedWriteSeesNewDataEvenWhenPrefetched() throws Exception {
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config(true), tempDir.resolve("p.bin"), tempDir.resolve("p.journal"))) {
            scheduler.start();
            byte[] original = pattern(0x10, 4096);
            await(scheduler.submitWrite("app", 0, original).completion());

            // Drive sequential reads so the prefetcher caches [4096, 8192).
            await(scheduler.submitRead("app", "s", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, 0, 1024).completion());
            await(scheduler.submitRead("app", "s", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, 1024, 1024).completion());
            await(scheduler.submitRead("app", "s", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, 2048, 1024).completion());
            awaitTrue(() -> scheduler.metrics().prefetchesIssued > 0,
                    "prefetch was never issued");

            // Confirm the prefetched range is served from the cache.
            awaitTrue(() -> {
                try {
                    await(scheduler.submitRead("app", "s", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, 4096, 1024).completion());
                    return scheduler.metrics().prefetchHits > 0;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "prefetched range was never hit from cache");

            // Overwrite the cached range and confirm the write.
            byte[] updated = pattern(0x70, 4096);
            await(scheduler.submitWrite("app", 4096, updated).completion());

            // The overlapping read must return the confirmed content, not the
            // stale prefetched copy.
            IoCompletion read = await(scheduler.submitRead("app", "s", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, 4096, 4096).completion());
            assertArrayEquals(updated, toArray(read),
                    "read after confirmed write returned stale prefetched data");
        }
    }

    @Test
    void concurrentOverlappingReadWriteReturnsWholeBeforeOrAfterImage() throws Exception {
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(
                config(true), tempDir.resolve("c.bin"), tempDir.resolve("c.journal"))) {
            scheduler.start();
            byte[] before = pattern(0x20, 8192);
            byte[] after = pattern(0x60, 8192);
            await(scheduler.submitWrite("app", 0, before).completion());

            List<Throwable> violations = new ArrayList<>();
            List<Thread> readers = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                Thread reader = new Thread(() -> {
                    try {
                        for (int i = 0; i < 300; i++) {
                            IoCompletion done = await(
                                    scheduler.submitRead("reader-" + Thread.currentThread().getId(),
                                            0, 8192).completion());
                            byte[] got = toArray(done);
                            if (!java.util.Arrays.equals(got, before)
                                    && !java.util.Arrays.equals(got, after)) {
                                synchronized (violations) {
                                    violations.add(new AssertionError(
                                            "read returned mixed/torn data"));
                                }
                                return;
                            }
                        }
                    } catch (Exception e) {
                        synchronized (violations) {
                            violations.add(e);
                        }
                    }
                });
                reader.start();
                readers.add(reader);
            }
            // Overlapping write while reads are in flight.
            await(scheduler.submitWrite("writer", 0, after).completion());
            for (Thread reader : readers) {
                reader.join(TimeUnit.SECONDS.toMillis(30));
            }
            assertTrue(violations.isEmpty(),
                    "concurrent read/write violations: " + violations);
            // After the write is confirmed, every read must see the new image.
            IoCompletion read = await(scheduler.submitRead("app", 0, 8192).completion());
            assertArrayEquals(after, toArray(read));
        }
    }

    @Test
    void prefetchOnAndOffProduceSameObservableResults() throws Exception {
        List<byte[]> withPrefetch = runScript(true);
        List<byte[]> withoutPrefetch = runScript(false);
        assertTrue(withPrefetch.size() == withoutPrefetch.size(),
                "script size mismatch");
        for (int i = 0; i < withPrefetch.size(); i++) {
            assertArrayEquals(withPrefetch.get(i), withoutPrefetch.get(i),
                    "read " + i + " diverged between prefetch on/off");
        }
    }

    private List<byte[]> runScript(boolean prefetch) throws Exception {
        Path data = tempDir.resolve(prefetch ? "on.bin" : "off.bin");
        Path journal = tempDir.resolve(prefetch ? "on.journal" : "off.journal");
        List<byte[]> results = new ArrayList<>();
        try (AsyncIOScheduler scheduler = new AsyncIOScheduler(config(prefetch), data, journal)) {
            scheduler.start();
            await(scheduler.submitWrite("app", 0, pattern(0x30, 16384)).completion());
            // Sequential warm-up to arm the prefetcher.
            for (int i = 0; i < 4; i++) {
                results.add(toArray(await(
                        scheduler.submitRead("app", "stream", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, i * 1024, 1024).completion())));
            }
            // Overwrite a region that may have been prefetched, then read it back.
            await(scheduler.submitWrite("app", 4096, pattern(0x40, 2048)).completion());
            results.add(toArray(await(
                    scheduler.submitRead("app", "stream", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, 4096, 2048).completion())));
            results.add(toArray(await(
                    scheduler.submitRead("app", "stream", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, 6144, 2048).completion())));
            // Another confirmed write overlapping both cached and uncached parts.
            await(scheduler.submitWrite("app", 2048, pattern(0x50, 4096)).completion());
            results.add(toArray(await(
                    scheduler.submitRead("app", "stream", com.github.guoyyit.asyncioschedulerjava.io.IoPriority.NORMAL, 0, 8192).completion())));
        }
        return results;
    }

    private static byte[] pattern(int seed, int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (seed + i % 13);
        }
        return data;
    }

    private static IoCompletion await(CompletableFuture<IoCompletion> future) throws Exception {
        IoCompletion completion = future.get(30, TimeUnit.SECONDS);
        if (!completion.success()) {
            throw new AssertionError("request failed", completion.error());
        }
        return completion;
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

    private static byte[] toArray(IoCompletion completion) {
        ByteBuffer buffer = completion.data().duplicate();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
