package com.github.guoyyit.asyncioschedulerjava.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteAheadLogTest {

    private static WriteBatch batch(long id, byte marker, int blockSize) {
        byte[] data = new byte[blockSize];
        data[0] = marker;
        return new WriteBatch(id,
                List.of(new WriteBatch.Entry(0L, ByteBuffer.wrap(data))));
    }

    @Test
    void dataWithoutCommitIsAnUnfinishedBatchAndIsDiscarded(@TempDir Path directory)
            throws Exception {
        int blockSize = 32;
        Path logPath = directory.resolve("wal.log");
        try (FileDurableChannel channel = FileDurableChannel.open(logPath)) {
            WriteAheadLog wal = new WriteAheadLog(channel, blockSize);
            wal.appendDataAndForce(batch(1L, (byte) 5, blockSize));
            // Deliberately do not commit: simulates a crash after phase 1.
        }

        try (FileDurableChannel channel = FileDurableChannel.open(logPath)) {
            WriteAheadLog wal = new WriteAheadLog(channel, blockSize);
            RecoveryResult result = wal.recover();
            assertEquals(0, result.getRecoveredBatches().size());
            assertEquals(1, result.getDiscardedBatches());
            assertEquals(1, result.getDiscardedRecords());
            assertEquals(0L, channel.size(), "torn log tail must be truncated");
        }
    }

    @Test
    void committedBatchIsRecoveredThenTruncatedAfterApply(@TempDir Path directory)
            throws Exception {
        int blockSize = 32;
        Path logPath = directory.resolve("wal.log");
        try (FileDurableChannel channel = FileDurableChannel.open(logPath)) {
            WriteAheadLog wal = new WriteAheadLog(channel, blockSize);
            WriteBatch batch = batch(9L, (byte) 8, blockSize);
            wal.appendDataAndForce(batch);
            wal.commitAndForce(9L);
        }

        try (FileDurableChannel channel = FileDurableChannel.open(logPath)) {
            WriteAheadLog wal = new WriteAheadLog(channel, blockSize);
            RecoveryResult result = wal.recover();
            assertEquals(1, result.getRecoveredBatches().size());
            assertEquals(9L, result.getRecoveredBatches().get(0).getBatchId());
            assertEquals((byte) 8,
                    result.getRecoveredBatches().get(0).getEntries().get(0).getData().get(0));

            wal.discardCommitted(9L);
            assertEquals(0L, channel.size());
        }
    }

    @Test
    void corruptedTrailingBytesAreTruncated(@TempDir Path directory) throws Exception {
        int blockSize = 32;
        Path logPath = directory.resolve("wal.log");
        try (FileDurableChannel channel = FileDurableChannel.open(logPath)) {
            WriteAheadLog wal = new WriteAheadLog(channel, blockSize);
            wal.appendDataAndForce(batch(3L, (byte) 2, blockSize));
            wal.commitAndForce(3L);
        }
        try (RandomAccessFile raf = new RandomAccessFile(logPath.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[]{0x12, 0x34, 0x56, 0x78});
            raf.getFD().sync();
        }

        try (FileDurableChannel channel = FileDurableChannel.open(logPath)) {
            WriteAheadLog wal = new WriteAheadLog(channel, blockSize);
            RecoveryResult result = wal.recover();
            assertEquals(1, result.getRecoveredBatches().size());
        }
    }
}
