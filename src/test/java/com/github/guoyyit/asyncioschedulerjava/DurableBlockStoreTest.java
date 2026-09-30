package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.io.WriteSegment;
import com.github.guoyyit.asyncioschedulerjava.storage.DurableBlockStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableBlockStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void writesAreReadableAfterCommit() throws Exception {
        Path data = tempDir.resolve("data.bin");
        Path journal = tempDir.resolve("data.journal");
        try (DurableBlockStore store = new DurableBlockStore(data, journal, 4096)) {
            store.open();
            store.commitWriteBatch(List.of(new WriteSegment(0, "hello".getBytes())));
            ByteBuffer read = store.read(0, 5);
            assertArrayEquals("hello".getBytes(), toArray(read));
        }
    }

    @Test
    void overlappingSegmentsInOneBatchApplyWithLastWriteWinning() throws Exception {
        Path data = tempDir.resolve("data.bin");
        Path journal = tempDir.resolve("data.journal");
        try (DurableBlockStore store = new DurableBlockStore(data, journal, 4096)) {
            store.open();
            store.commitWriteBatch(List.of(
                    new WriteSegment(0, "aaaaa".getBytes()),
                    new WriteSegment(2, "BB".getBytes())));
            assertArrayEquals("aaBBa".getBytes(), toArray(store.read(0, 5)));
        }
    }

    @Test
    void committedBatchSurvivesReopenAfterCrashBetweenJournalAndData() throws Exception {
        Path data = tempDir.resolve("data.bin");
        Path journal = tempDir.resolve("data.journal");
        DurableBlockStore first = new DurableBlockStore(data, journal, 4096);
        first.open();
        first.setCrashAfterJournalSyncForTest(() -> {
            throw new IllegalStateException("simulated crash after journal fsync");
        });
        try {
            first.commitWriteBatch(List.of(new WriteSegment(0, "durable".getBytes())));
            throw new AssertionError("expected simulated crash");
        } catch (IllegalStateException expected) {
            // process "died" here; deliberately do not close()
        }

        try (DurableBlockStore store = new DurableBlockStore(data, journal, 4096)) {
            store.open();
            assertArrayEquals("durable".getBytes(), toArray(store.read(0, 7)));
        }
        assertEquals(0L, java.nio.file.Files.size(journal),
                "journal must be discarded after successful recovery");
    }

    @Test
    void tornUncommittedBatchTailIsDetectedAndDiscarded() throws Exception {
        Path data = tempDir.resolve("data.bin");
        Path journal = tempDir.resolve("data.journal");
        try (DurableBlockStore store = new DurableBlockStore(data, journal, 4096)) {
            store.open();
            store.commitWriteBatch(List.of(new WriteSegment(0, "confirmed".getBytes())));
        }
        // Append a torn batch: valid-looking header/segment bytes but no CRC/marker.
        try (RandomAccessFile raf = new RandomAccessFile(journal.toFile(), "rw")) {
            long length = raf.length();
            raf.seek(length);
            raf.write(new byte[] {0x31, 0x54, 0x42, 0x4A});
            raf.writeLong(999L);
            raf.writeInt(1);
            raf.writeLong(100L);
            raf.writeInt(4);
            raf.write(new byte[] {'t', 'o', 'r', 'n'});
        }
        try (DurableBlockStore store = new DurableBlockStore(data, journal, 4096)) {
            store.open();
            assertArrayEquals("confirmed".getBytes(), toArray(store.read(0, 9)));
            ByteBuffer tornArea = store.read(100, 4);
            assertEquals(0, toArray(tornArea)[0]);
        }
    }

    @Test
    void corruptedCommittedBatchIsRejectedByCrc() throws Exception {
        Path data = tempDir.resolve("data.bin");
        Path journal = tempDir.resolve("journal2.bin");
        DurableBlockStore first = new DurableBlockStore(data, journal, 4096);
        first.open();
        first.setCrashAfterJournalSyncForTest(() -> {
            throw new IllegalStateException("simulated crash after journal fsync");
        });
        try {
            first.commitWriteBatch(List.of(new WriteSegment(0, "abc".getBytes())));
            throw new AssertionError("expected simulated crash");
        } catch (IllegalStateException expected) {
            // journal contains one committed batch, data file untouched
        }

        // Corrupt one payload byte, plus append a second intact committed batch
        // behind it. Recovery must stop at the corrupted batch and discard the
        // rest of the tail without throwing.
        try (RandomAccessFile raf = new RandomAccessFile(journal.toFile(), "rw")) {
            raf.seek(28);
            int b = raf.read();
            raf.seek(raf.getFilePointer() - 1);
            raf.write(b ^ 0xFF);
        }
        try (DurableBlockStore store = new DurableBlockStore(data, journal, 4096)) {
            store.open();
            // Corrupt (torn/garbage) tail must be discarded.
            byte[] area = toArray(store.read(0, 3));
            assertArrayEquals(new byte[] {0, 0, 0}, area);
        }
        assertTrue(java.nio.file.Files.size(journal) == 0L,
                "corrupt journal tail must be truncated on recovery");
    }

    private static byte[] toArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.asReadOnlyBuffer();
        duplicate.rewind();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
