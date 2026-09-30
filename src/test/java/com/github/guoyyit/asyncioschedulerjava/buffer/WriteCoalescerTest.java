package com.github.guoyyit.asyncioschedulerjava.buffer;

import com.github.guoyyit.asyncioschedulerjava.storage.WriteBatch;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteCoalescerTest {

    private static ByteBuffer block(int marker) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.put((byte) marker);
        buffer.rewind();
        return buffer;
    }

    @Test
    void flushesWhenBatchBlockLimitReached() {
        WriteCoalescer coalescer = new WriteCoalescer(4, 1_000_000L, 16);
        long now = 1000L;
        coalescer.add(0L, block(1), now);
        coalescer.add(1L, block(2), now);
        coalescer.add(2L, block(3), now);
        assertFalse(coalescer.shouldFlush(now + 1));
        coalescer.add(3L, block(4), now);
        assertTrue(coalescer.shouldFlush(now + 1));

        WriteBatch batch = coalescer.drain(7L, now + 1);
        assertEquals(4, batch.getEntries().size());
        assertEquals(7L, batch.getBatchId());
        assertEquals(0L, batch.getEntries().get(0).getBlockIndex());
        assertEquals(3L, batch.getEntries().get(3).getBlockIndex());
        assertTrue(coalescer.isEmpty());
    }

    @Test
    void flushesWhenDelayCapReached() {
        long delayNanos = 10_000L;
        WriteCoalescer coalescer = new WriteCoalescer(8, delayNanos, 16);
        long start = 5_000_000L;
        coalescer.add(0L, block(1), start);
        assertFalse(coalescer.shouldFlush(start + delayNanos - 1));
        assertTrue(coalescer.shouldFlush(start + delayNanos));
    }

    @Test
    void repeatedWritesToSameBlockOverwriteAndStayOneBlock() {
        WriteCoalescer coalescer = new WriteCoalescer(8, 1_000_000L, 16);
        long now = 0L;
        coalescer.add(5L, block(1), now);
        coalescer.add(5L, block(9), now + 1);
        assertEquals(1, coalescer.dirtyBlocks());

        WriteBatch batch = coalescer.drain(1L, now + 2);
        assertEquals(1, batch.getEntries().size());
        assertEquals(5L, batch.getEntries().get(0).getBlockIndex());
        assertEquals(9, batch.getEntries().get(0).getData().get(0));
    }
}
