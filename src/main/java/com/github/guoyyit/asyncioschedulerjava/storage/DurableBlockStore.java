package com.github.guoyyit.asyncioschedulerjava.storage;

import com.github.guoyyit.asyncioschedulerjava.io.WriteSegment;
import com.github.guoyyit.asyncioschedulerjava.journal.Journal;
import com.github.guoyyit.asyncioschedulerjava.journal.RecoveredBatch;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

/**
 * Crash-consistent block store: a data file plus an append-only journal.
 *
 * <p>Writes become durable atomically per batch: the batch is first forced to
 * the journal, then applied to the data file. On recovery, every committed
 * journal batch is re-applied, after which the journal is discarded.
 */
public class DurableBlockStore implements AutoCloseable {

    private final Path dataPath;
    private final Path journalPath;
    private final int blockSize;

    private RandomAccessFile dataFile;
    private Journal journal;
    private long dataSize;
    private long nextSequence;
    private final Object commitLock = new Object();
    private volatile Runnable crashAfterJournalSync;

    public DurableBlockStore(Path dataPath, Path journalPath, int blockSize) {
        this.dataPath = dataPath;
        this.journalPath = journalPath;
        this.blockSize = blockSize;
    }

    public void open() throws IOException {
        dataFile = new RandomAccessFile(dataPath.toFile(), "rw");
        dataSize = dataFile.length();
        journal = new Journal(journalPath);
        journal.open();

        List<RecoveredBatch> recovered = journal.scanCommitted();
        long lastSequence = 0;
        for (RecoveredBatch batch : recovered) {
            applySegments(batch.segments());
            lastSequence = batch.sequence();
        }
        dataFile.getFD().sync();
        journal.truncateAt(recovered.isEmpty() ? journal.lastValidBytes() : 0L);
        nextSequence = lastSequence + 1;
    }

    /**
     * Test hook: invoked after the journal batch is forced to stable storage
     * but before the segments are applied to the data file. Simulates an
     * immediate process crash inside that window.
     */
    public void setCrashAfterJournalSyncForTest(Runnable hook) {
        this.crashAfterJournalSync = hook;
    }

    /**
     * Reads {@code length} bytes starting at {@code offset}; bytes past the end
     * of the data file are returned as zeros.
     */
    public ByteBuffer read(long offset, int length) throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        byte[] buffer = new byte[length];
        synchronized (commitLock) {
            if (dataFile != null && offset < dataSize) {
                int readable = (int) Math.min(length, dataSize - offset);
                dataFile.seek(offset);
                dataFile.readFully(buffer, 0, readable);
            }
        }
        return ByteBuffer.wrap(buffer);
    }

    /**
     * Commits a batch of adjacent or non-adjacent writes atomically and durably.
     * Returns only after the batch is crash-safe.
     */
    public long commitWriteBatch(List<WriteSegment> segments) throws IOException {
        if (segments.isEmpty()) {
            return 0;
        }
        synchronized (commitLock) {
            long sequence = nextSequence++;
            long journalBytes = journal.appendCommit(sequence, segments);
            Runnable hook = crashAfterJournalSync;
            if (hook != null) {
                hook.run();
            }
            applySegments(segments);
            dataFile.getFD().sync();
            journal.truncateAt(0);
            return journalBytes;
        }
    }

    public int blockSize() {
        return blockSize;
    }

    Journal journal() {
        return journal;
    }

    @Override
    public void close() throws IOException {
        synchronized (commitLock) {
            IOException failure = null;
            if (dataFile != null) {
                try {
                    dataFile.close();
                } catch (IOException e) {
                    failure = e;
                }
                dataFile = null;
            }
            if (journal != null) {
                try {
                    journal.close();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    }
                }
                journal = null;
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private void applySegments(List<WriteSegment> segments) throws IOException {
        for (WriteSegment segment : segments) {
            byte[] bytes = segment.data();
            dataFile.seek(segment.offset());
            dataFile.write(bytes);
            long end = segment.endOffset();
            if (end > dataSize) {
                dataSize = end;
            }
        }
    }
}
