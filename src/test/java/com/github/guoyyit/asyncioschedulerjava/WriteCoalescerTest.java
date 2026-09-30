package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.io.IoPriority;
import com.github.guoyyit.asyncioschedulerjava.io.IoRequest;
import com.github.guoyyit.asyncioschedulerjava.io.IoType;
import com.github.guoyyit.asyncioschedulerjava.scheduling.WriteCoalescer;
import com.github.guoyyit.asyncioschedulerjava.storage.DurableBlockStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteCoalescerTest {

    @TempDir
    Path tempDir;

    private DurableBlockStore openStore(String name) throws Exception {
        DurableBlockStore store = new DurableBlockStore(
                tempDir.resolve(name + ".bin"), tempDir.resolve(name + ".journal"), 4096);
        store.open();
        return store;
    }

    private IoRequest write(long id, long offset, byte[] data) {
        return new IoRequest(id, IoType.WRITE, IoPriority.NORMAL, "t", "s",
                offset, data.length, ByteBuffer.wrap(data), System.nanoTime());
    }

    @Test
    void adjacentWritesMergeIntoOneDurableSegment() throws Exception {
        DurableBlockStore store = openStore("adj");
        List<Long> batches = new ArrayList<>();
        try (WriteCoalescer coalescer = new WriteCoalescer(store, 16 * 1024,
                Duration.ofMillis(20),
                (journalBytes, mergedAway) -> batches.add(journalBytes),
                (request, error) -> { })) {
            coalescer.add(write(1, 0, "ab".getBytes()));
            coalescer.add(write(2, 2, "cd".getBytes()));
            coalescer.flush();
        }
        assertEquals(1, batches.size());
        assertArrayEquals("abcd".getBytes(), toArray(store.read(0, 4)));
    }

    @Test
    void overlappingWritesApplyLastValueWithinSingleBatch() throws Exception {
        DurableBlockStore store = openStore("overlap");
        try (WriteCoalescer coalescer = new WriteCoalescer(store, 16 * 1024,
                Duration.ofMillis(20), (b, m) -> { }, (r, e) -> { })) {
            coalescer.add(write(1, 0, "aaaa".getBytes()));
            coalescer.add(write(2, 1, "BB".getBytes()));
            coalescer.flush();
        }
        assertArrayEquals("aBBa".getBytes(), toArray(store.read(0, 4)));
    }

    @Test
    void batchesAreSplitAtConfiguredByteLimit() throws Exception {
        DurableBlockStore store = openStore("split");
        List<Long> batches = new ArrayList<>();
        try (WriteCoalescer coalescer = new WriteCoalescer(store, 8,
                Duration.ofHours(1),
                (journalBytes, mergedAway) -> batches.add(journalBytes),
                (request, error) -> { })) {
            coalescer.add(write(1, 0, new byte[5]));
            coalescer.add(write(2, 100, new byte[5]));
            coalescer.flush();
        }
        assertEquals(2, batches.size(), "10 bytes over an 8-byte limit require two batches");
        assertEquals(5, toArray(store.read(0, 5)).length);
        assertEquals(5, toArray(store.read(100, 5)).length);
    }

    @Test
    void lingerCapFlushesEvenWhenBatchIsSmall() throws Exception {
        DurableBlockStore store = openStore("linger");
        CompletableFuture<Long> completed = new CompletableFuture<>();
        try (WriteCoalescer coalescer = new WriteCoalescer(store, 1024 * 1024,
                Duration.ofMillis(30), (b, m) -> { },
                (request, error) -> completed.complete(request.id()))) {
            long start = System.nanoTime();
            coalescer.add(write(42, 0, "x".getBytes()));
            assertEquals(42L, completed.get(5, TimeUnit.SECONDS));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMs >= 20, "should wait roughly the linger period, was " + elapsedMs);
        }
    }

    @Test
    void pendingSegmentsAreVisibleToOverlapCheck() throws Exception {
        DurableBlockStore store = openStore("visible");
        try (WriteCoalescer coalescer = new WriteCoalescer(store, 1024 * 1024,
                Duration.ofHours(1), (b, m) -> { }, (r, e) -> { })) {
            coalescer.add(write(1, 10, new byte[8]));
            assertEquals(1, coalescer.overlapping(12, 4).size());
            assertTrue(coalescer.overlapping(100, 4).isEmpty());
        }
    }

    private static byte[] toArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.asReadOnlyBuffer();
        duplicate.rewind();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
