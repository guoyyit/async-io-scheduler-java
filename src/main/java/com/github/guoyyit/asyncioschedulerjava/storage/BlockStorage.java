package com.github.guoyyit.asyncioschedulerjava.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fixed-block storage over a data file plus a WAL.
 *
 * <p>Durability contract: {@link #writeBatch} returns only after the batch is
 * committed to the WAL and applied + forced to the data file. Reads never block
 * on WAL IO and bypass the OS page cache for miss loads when configured.
 */
public final class BlockStorage implements Closeable {

    private final DurableChannel dataChannel;
    private final WriteAheadLog wal;
    private final int blockSize;
    private final AtomicLong blockCount = new AtomicLong();

    public BlockStorage(DurableChannel dataChannel, WriteAheadLog wal, int blockSize)
            throws IOException {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive");
        }
        this.dataChannel = dataChannel;
        this.wal = wal;
        this.blockSize = blockSize;
        this.blockCount.set((dataChannel.size() + blockSize - 1) / blockSize);
    }

    /**
     * Replay committed WAL batches into the data file (idempotent), force it,
     * then truncate the log. Must be called once before serving requests.
     */
    public RecoveryResult recover() throws IOException {
        RecoveryResult result = wal.recover();
        long maxBatchId = Long.MIN_VALUE;
        for (WriteBatch batch : result.getRecoveredBatches()) {
            applyBatch(batch);
            maxBatchId = Math.max(maxBatchId, batch.getBatchId());
        }
        if (!result.getRecoveredBatches().isEmpty()) {
            dataChannel.force();
            wal.discardCommitted(maxBatchId);
        }
        return result;
    }

    /** Read exactly one block (zero-filled beyond EOF). */
    public ByteBuffer readBlock(long blockIndex) throws IOException {
        if (blockIndex < 0) {
            throw new IllegalArgumentException("blockIndex must be non-negative");
        }
        ByteBuffer block = ByteBuffer.allocate(blockSize);
        long position = blockIndex * (long) blockSize;
        long readCursor = position;
        while (block.hasRemaining()) {
            int read = dataChannel.read(block, readCursor);
            if (read < 0) {
                break;
            }
            readCursor += read;
        }
        while (block.hasRemaining()) {
            block.put((byte) 0);
        }
        block.flip();
        return block;
    }

    /** Durably apply a batch: WAL commit, data force, ack, then log truncate. */
    public void writeBatch(WriteBatch batch) throws IOException {
        wal.appendDataAndForce(batch);
        wal.commitAndForce(batch.getBatchId());
        applyBatch(batch);
        dataChannel.force();
        wal.discardCommitted(batch.getBatchId());
    }

    public int getBlockSize() {
        return blockSize;
    }

    public long getBlockCount() {
        return blockCount.get();
    }

    private void applyBatch(WriteBatch batch) throws IOException {
        for (WriteBatch.Entry entry : batch.getEntries()) {
            long position = entry.getBlockIndex() * (long) blockSize;
            dataChannel.writeAt(entry.getData().duplicate(), position);
            blockCount.accumulateAndGet(entry.getBlockIndex() + 1, Math::max);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            dataChannel.force();
        } finally {
            dataChannel.close();
        }
    }
}
