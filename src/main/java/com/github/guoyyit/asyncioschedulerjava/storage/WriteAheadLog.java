package com.github.guoyyit.asyncioschedulerjava.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Write-ahead log that makes a batch of block writes atomic across crashes.
 *
 * <p>Two-phase commit per batch:
 * <ol>
 *   <li>append one or more DATA records and force the log,</li>
 *   <li>append a COMMIT record and force the log;</li>
 *   <li>caller applies the batch to the data file and forces it;</li>
 *   <li>{@link #discardCommitted(long)} truncates no-longer-needed prefix.</li>
 * </ol>
 * Replay applies complete (committed) batches in batch-id order and discards
 * the trailing records of every unfinished batch.
 */
public final class WriteAheadLog {

    private final DurableChannel logChannel;
    private final int blockSize;
    private final List<Long> retainedBatchIds = new ArrayList<>();

    public WriteAheadLog(DurableChannel logChannel, int blockSize) {
        this.logChannel = logChannel;
        this.blockSize = blockSize;
    }

    /**
     * Read the whole log and return committed batches in order; truncate the
     * channel after the last good record so torn suffixes are forgotten.
     */
    public RecoveryResult recover() throws IOException {
        long logSize = logChannel.size();
        if (logSize == 0) {
            return new RecoveryResult(List.of(), 0, 0);
        }

        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(logSize, Integer.MAX_VALUE));
        readFully(logChannel, buffer, 0);
        buffer.flip();

        Map<Long, List<WriteBatch.Entry>> pendingData = new LinkedHashMap<>();
        List<WriteBatch> recovered = new ArrayList<>();
        int committedEnd = 0;

        while (buffer.remaining() >= WalRecord.HEADER_BYTES + WalRecord.CRC_BYTES) {
            int frameStart = buffer.position();
            int magic = buffer.getInt();
            if (magic != WalRecord.MAGIC) {
                break;
            }
            byte typeCode = (byte) buffer.getInt();
            WalRecord.Type type;
            try {
                type = WalRecord.Type.fromCode(typeCode);
            } catch (IllegalArgumentException e) {
                break;
            }
            long batchId = buffer.getLong();
            int payloadLength = buffer.getInt();
            if (payloadLength < 0
                    || buffer.remaining() < payloadLength + WalRecord.CRC_BYTES) {
                break;
            }
            int payloadOffset = buffer.position();
            ByteBuffer payload = ByteBuffer.allocate(payloadLength);
            for (int i = 0; i < payloadLength; i++) {
                payload.put(buffer.get());
            }
            payload.flip();
            int storedCrc = buffer.getInt();
            int coveredLength = WalRecord.HEADER_BYTES + payloadLength;
            int actualCrc = WalRecord.crc32(buffer, frameStart, coveredLength);
            if (actualCrc != storedCrc) {
                break;
            }

            if (type == WalRecord.Type.DATA) {
                if (payloadLength != Long.BYTES + blockSize) {
                    break;
                }
                long blockIndex = payload.getLong();
                byte[] blockBytes = new byte[blockSize];
                payload.get(blockBytes);
                pendingData.computeIfAbsent(batchId, key -> new ArrayList<>())
                        .add(new WriteBatch.Entry(blockIndex, ByteBuffer.wrap(blockBytes)));
            } else {
                List<WriteBatch.Entry> entries = pendingData.remove(batchId);
                if (entries != null && !entries.isEmpty()) {
                    recovered.add(new WriteBatch(batchId, List.copyOf(entries)));
                }
                committedEnd = frameStart + coveredLength + WalRecord.CRC_BYTES;
            }
        }

        int discardedRecords = 0;
        for (List<WriteBatch.Entry> entries : pendingData.values()) {
            discardedRecords += entries.size();
        }
        int discardedBatches = pendingData.size();

        if (committedEnd < logSize) {
            logChannel.truncate(committedEnd);
        }
        retainedBatchIds.clear();
        for (WriteBatch batch : recovered) {
            retainedBatchIds.add(batch.getBatchId());
        }
        return new RecoveryResult(List.copyOf(recovered), discardedBatches, discardedRecords);
    }

    /** Phase 1: append all DATA records for the batch and force. */
    public void appendDataAndForce(WriteBatch batch) throws IOException {
        for (WriteBatch.Entry entry : batch.getEntries()) {
            ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + blockSize);
            payload.putLong(entry.getBlockIndex());
            ByteBuffer blockData = entry.getData().duplicate();
            if (blockData.remaining() != blockSize) {
                throw new IllegalArgumentException(
                        "block entry must contain exactly " + blockSize + " bytes");
            }
            payload.put(blockData);
            payload.flip();
            WalRecord record = new WalRecord(WalRecord.Type.DATA, batch.getBatchId(), payload);
            logChannel.append(record.encode());
        }
        logChannel.force();
    }

    /** Phase 2: append the COMMIT record for {@code batchId} and force. */
    public void commitAndForce(long batchId) throws IOException {
        WalRecord commit = new WalRecord(WalRecord.Type.COMMIT, batchId,
                ByteBuffer.allocate(0));
        logChannel.append(commit.encode());
        logChannel.force();
        if (!retainedBatchIds.contains(batchId)) {
            retainedBatchIds.add(batchId);
        }
    }

    /** Truncate log prefix belonging to batch ids at or below {@code maxBatchId}. */
    public void discardCommitted(long maxBatchId) throws IOException {
        retainedBatchIds.removeIf(id -> id <= maxBatchId);
        if (retainedBatchIds.isEmpty() && logChannel.size() > 0) {
            logChannel.truncate(0);
        }
    }

    /** Batches still present in the log after recovery (ids). */
    public List<Long> retainedBatchIds() {
        return List.copyOf(retainedBatchIds);
    }

    private static void readFully(DurableChannel channel, ByteBuffer dst, long position)
            throws IOException {
        long cursor = position;
        while (dst.hasRemaining()) {
            int read = channel.read(dst, cursor);
            if (read < 0) {
                throw new IOException("unexpected EOF reading WAL at " + cursor);
            }
            cursor += read;
        }
    }
}
