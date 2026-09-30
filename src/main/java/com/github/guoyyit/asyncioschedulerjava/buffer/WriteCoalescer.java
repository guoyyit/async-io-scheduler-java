package com.github.guoyyit.asyncioschedulerjava.buffer;

import com.github.guoyyit.asyncioschedulerjava.storage.WriteBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Merges adjacent/repeated small block writes into one ordered batch.
 * Blocks are kept in first-dirtied order; repeated writes to the same block
 * overwrite in place. A flush is due when the batch reaches the configured
 * block count or its oldest dirty write reaches the delay cap.
 */
public final class WriteCoalescer {

    private final int maxBatchBlocks;
    private final long maxBatchDelayNanos;
    private final int blockSize;

    private final TreeMap<Long, ByteBuffer> dirtyBlocks = new TreeMap<>();
    private int acceptedWrites;
    private long oldestDirtyNanos = Long.MAX_VALUE;

    public WriteCoalescer(int maxBatchBlocks, long maxBatchDelayNanos, int blockSize) {
        if (maxBatchBlocks <= 0) {
            throw new IllegalArgumentException("maxBatchBlocks must be positive");
        }
        this.maxBatchBlocks = maxBatchBlocks;
        this.maxBatchDelayNanos = maxBatchDelayNanos;
        this.blockSize = blockSize;
    }

    /** Add one fully-resolved dirty block (caller has done any RMW overlay). */
    public void add(long blockIndex, ByteBuffer blockData, long nowNanos) {
        if (blockData.remaining() != blockSize) {
            throw new IllegalArgumentException("blockData must contain exactly one block");
        }
        acceptedWrites++;
        if (!dirtyBlocks.containsKey(blockIndex)) {
            oldestDirtyNanos = Math.min(oldestDirtyNanos, nowNanos);
        }
        ByteBuffer copy = ByteBuffer.allocate(blockSize);
        copy.put(blockData.duplicate());
        copy.flip();
        dirtyBlocks.put(blockIndex, copy);
    }

    public boolean shouldFlush(long nowNanos) {
        if (dirtyBlocks.isEmpty()) {
            return false;
        }
        if (dirtyBlocks.size() >= maxBatchBlocks) {
            return true;
        }
        return nowNanos - oldestDirtyNanos >= maxBatchDelayNanos;
    }

    public boolean isEmpty() {
        return dirtyBlocks.isEmpty();
    }

    /** Enqueue time of the oldest currently dirty block ({@code MAX_VALUE} if empty). */
    public long oldestDirtyNanos() {
        return oldestDirtyNanos;
    }

    public int dirtyBlocks() {
        return dirtyBlocks.size();
    }

    /** Whether {@code blockIndex} already has buffered dirty data. */
    public boolean containsBlock(long blockIndex) {
        return dirtyBlocks.containsKey(blockIndex);
    }

    public int bufferedWrites() {
        return acceptedWrites;
    }

    /** Remove and return all dirty blocks in ascending block-index order. */
    public WriteBatch drain(long batchId, long nowNanos) {
        List<WriteBatch.Entry> entries = new ArrayList<>(dirtyBlocks.size());
        for (Map.Entry<Long, ByteBuffer> entry : dirtyBlocks.entrySet()) {
            ByteBuffer copy = ByteBuffer.allocate(blockSize);
            copy.put(entry.getValue().duplicate());
            copy.flip();
            entries.add(new WriteBatch.Entry(entry.getKey(), copy));
        }
        entries.sort(Comparator.comparingLong(WriteBatch.Entry::getBlockIndex));
        dirtyBlocks.clear();
        acceptedWrites = 0;
        oldestDirtyNanos = Long.MAX_VALUE;
        return new WriteBatch(batchId, entries);
    }
}
