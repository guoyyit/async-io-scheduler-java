package com.github.guoyyit.asyncioschedulerjava.journal;

import com.github.guoyyit.asyncioschedulerjava.io.WriteSegment;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Append-only write-ahead journal.
 *
 * <p>On-disk layout per batch (little endian):
 * <pre>
 *   int  magic = 0x4A425431 ("JBT1")
 *   long sequence
 *   int  segmentCount
 *   per segment: long offset, int length, byte[length]
 *   int  crc32c (CRC of every byte from magic up to but excluding this field)
 *   int  commitMarker = 0x434F4D49 ("COMI")
 * </pre>
 * A batch is only considered committed when both the CRC matches and the
 * trailing commit marker is present.
 */
public class Journal implements AutoCloseable {

    public static final int MAGIC = 0x4A425431;
    public static final int COMMIT_MARKER = 0x434F4D49;
    private static final int MAX_SEGMENTS_PER_BATCH = 1_048_576;
    private static final int HEADER_FIXED_BYTES = Integer.BYTES + Long.BYTES + Integer.BYTES;
    private static final int SEGMENT_HEADER_BYTES = Long.BYTES + Integer.BYTES;
    private static final int TRAILER_BYTES = Integer.BYTES + Integer.BYTES;

    private final Path journalPath;
    private RandomAccessFile file;
    private volatile long fileSize;

    public Journal(Path journalPath) {
        this.journalPath = journalPath;
    }

    public void open() throws IOException {
        if (file != null) {
            return;
        }
        file = new RandomAccessFile(journalPath.toFile(), "rw");
        fileSize = file.length();
    }

    /**
     * Appends a batch, forces it to stable storage, and returns its byte length.
     */
    public long appendCommit(long sequence, List<WriteSegment> segments) throws IOException {
        CRC32C crc = new CRC32C();
        int payloadLength = HEADER_FIXED_BYTES;
        for (WriteSegment segment : segments) {
            payloadLength += SEGMENT_HEADER_BYTES + segment.length();
        }
        byte[] block = new byte[payloadLength + TRAILER_BYTES];
        int pos = 0;
        pos = putInt(block, pos, MAGIC);
        pos = putLong(block, pos, sequence);
        pos = putInt(block, pos, segments.size());
        for (WriteSegment segment : segments) {
            pos = putLong(block, pos, segment.offset());
            pos = putInt(block, pos, segment.length());
            byte[] data = segment.data();
            System.arraycopy(data, 0, block, pos, data.length);
            pos += data.length;
        }
        crc.update(block, 0, payloadLength);
        pos = putInt(block, pos, (int) crc.getValue());
        putInt(block, pos, COMMIT_MARKER);

        synchronized (this) {
            file.seek(fileSize);
            file.write(block);
            file.getFD().sync();
            fileSize += block.length;
        }
        return block.length;
    }

    /**
     * Scans the journal and returns all intact, committed batches in order.
     * Stops at the first torn/corrupt/trailing-garbage batch.
     */
    public List<RecoveredBatch> scanCommitted() throws IOException {
        List<RecoveredBatch> recovered = new ArrayList<>();
        byte[] header = new byte[HEADER_FIXED_BYTES];
        long position = 0;
        while (position < fileSize) {
            if (readExact(position, header, HEADER_FIXED_BYTES) < HEADER_FIXED_BYTES) {
                break;
            }
            CRC32C crc = new CRC32C();
            crc.update(header, 0, HEADER_FIXED_BYTES);
            int magic = getInt(header, 0);
            if (magic != MAGIC) {
                break;
            }
            long sequence = getLong(header, Integer.BYTES);
            int segmentCount = getInt(header, Integer.BYTES + Long.BYTES);
            if (segmentCount < 0 || segmentCount > MAX_SEGMENTS_PER_BATCH) {
                break;
            }
            long cursor = position + HEADER_FIXED_BYTES;
            List<WriteSegment> segments = new ArrayList<>(segmentCount);
            boolean intact = true;
            for (int i = 0; i < segmentCount; i++) {
                byte[] segmentHeader = new byte[SEGMENT_HEADER_BYTES];
                if (readExact(cursor, segmentHeader, SEGMENT_HEADER_BYTES) < SEGMENT_HEADER_BYTES) {
                    intact = false;
                    break;
                }
                long offset = getLong(segmentHeader, 0);
                int length = getInt(segmentHeader, Long.BYTES);
                if (offset < 0 || length < 0) {
                    intact = false;
                    break;
                }
                cursor += SEGMENT_HEADER_BYTES;
                if (length > fileSize - cursor - TRAILER_BYTES) {
                    intact = false;
                    break;
                }
                byte[] data = new byte[length];
                if (length > 0 && readExact(cursor, data, length) < length) {
                    intact = false;
                    break;
                }
                crc.update(segmentHeader, 0, SEGMENT_HEADER_BYTES);
                crc.update(data, 0, length);
                segments.add(new WriteSegment(offset, data));
                cursor += length;
            }
            if (!intact) {
                break;
            }
            byte[] trailer = new byte[TRAILER_BYTES];
            if (readExact(cursor, trailer, TRAILER_BYTES) < TRAILER_BYTES) {
                break;
            }
            int storedCrc = getInt(trailer, 0);
            int marker = getInt(trailer, Integer.BYTES);
            if (marker != COMMIT_MARKER || storedCrc != (int) crc.getValue()) {
                break;
            }
            long batchEnd = cursor + TRAILER_BYTES;
            recovered.add(new RecoveredBatch(sequence, segments, batchEnd));
            position = batchEnd;
        }
        lastValidBytes = position;
        return recovered;
    }

    private long lastValidBytes;

    /**
     * Byte position one past the last intact committed batch found by the
     * most recent {@link #scanCommitted()} call.
     */
    public long lastValidBytes() {
        return lastValidBytes;
    }

    /**
     * Removes all journal content after the given number of intact bytes.
     */
    public void truncateAt(long validBytes) throws IOException {
        synchronized (this) {
            file.seek(validBytes);
            file.setLength(validBytes);
            file.getFD().sync();
            fileSize = validBytes;
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (file != null) {
                file.close();
                file = null;
            }
        }
    }

    private int readExact(long position, byte[] buffer, int required) throws IOException {
        synchronized (this) {
            if (position + required > fileSize) {
                return -1;
            }
            file.seek(position);
            file.readFully(buffer, 0, required);
            return required;
        }
    }

    static int putInt(byte[] buffer, int pos, int value) {
        buffer[pos] = (byte) value;
        buffer[pos + 1] = (byte) (value >>> 8);
        buffer[pos + 2] = (byte) (value >>> 16);
        buffer[pos + 3] = (byte) (value >>> 24);
        return pos + Integer.BYTES;
    }

    static int putLong(byte[] buffer, int pos, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            buffer[pos + i] = (byte) (value >>> (8 * i));
        }
        return pos + Long.BYTES;
    }

    static int getInt(byte[] buffer, int pos) {
        return (buffer[pos] & 0xFF)
                | ((buffer[pos + 1] & 0xFF) << 8)
                | ((buffer[pos + 2] & 0xFF) << 16)
                | ((buffer[pos + 3] & 0xFF) << 24);
    }

    static long getLong(byte[] buffer, int pos) {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value |= (buffer[pos + i] & 0xFFL) << (8 * i);
        }
        return value;
    }
}
