package com.github.guoyyit.asyncioschedulerjava.scheduling;

import com.github.guoyyit.asyncioschedulerjava.io.IoRequest;
import com.github.guoyyit.asyncioschedulerjava.io.WriteSegment;
import com.github.guoyyit.asyncioschedulerjava.storage.DurableBlockStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.time.Duration;
import java.util.function.BiConsumer;

/**
 * Collects small writes and flushes them as merged, ordered batches.
 */
public class WriteCoalescer implements AutoCloseable {

    private static final class BufferedEntry {
        private final IoRequest request;
        private final WriteSegment segment;

        private BufferedEntry(IoRequest request, WriteSegment segment) {
            this.request = request;
            this.segment = segment;
        }
    }

    private final DurableBlockStore store;
    private final int maxBatchBytes;
    private final long maxLingerNanos;
    private final BiConsumer<IoRequest, Throwable> completionCallback;
    private final BiConsumer<Long, Integer> batchFlushListener;

    private final Object bufferLock = new Object();
    private final TreeMapBuffer buffer = new TreeMapBuffer();
    private long firstEnqueueNanos;
    private IOException flushFailure;
    private boolean closed;

    private final Thread flusher;

    public WriteCoalescer(DurableBlockStore store, int maxBatchBytes, Duration maxLinger,
                          BiConsumer<Long, Integer> batchFlushListener,
                          BiConsumer<IoRequest, Throwable> completionCallback) {
        this.store = store;
        this.maxBatchBytes = Math.max(1, maxBatchBytes);
        this.maxLingerNanos = Math.max(1L, maxLinger.toNanos());
        this.batchFlushListener = batchFlushListener;
        this.completionCallback = completionCallback;
        this.flusher = new Thread(this::flusherLoop, "write-flusher");
        this.flusher.setDaemon(true);
        this.flusher.start();
    }

    /**
     * Buffers one write. The returned segment offset/length are as submitted.
     */
    public void add(IoRequest request) {
        WriteSegment segment = new WriteSegment(request.offset(), toArray(request.data()));
        BufferedEntry entry = new BufferedEntry(request, segment);
        synchronized (bufferLock) {
            if (flushFailure != null) {
                completionCallback.accept(request, flushFailure);
                return;
            }
            if (firstEnqueueNanos == 0L) {
                firstEnqueueNanos = System.nanoTime();
            }
            buffer.add(entry);
            bufferLock.notifyAll();
        }
    }

    private static byte[] toArray(java.nio.ByteBuffer buffer) {
        java.nio.ByteBuffer duplicate = buffer.asReadOnlyBuffer();
        duplicate.rewind();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    /**
     * Returns all buffered segments overlapping the given range, sorted by offset.
     */
    public List<WriteSegment> overlapping(long offset, int length) {
        long end = offset + length;
        List<WriteSegment> result = new ArrayList<>();
        synchronized (bufferLock) {
            for (BufferedEntry entry : buffer.values()) {
                WriteSegment segment = entry.segment;
                if (segment.offset() < end && segment.endOffset() > offset) {
                    result.add(segment);
                }
            }
        }
        return result;
    }

    public int pendingCount() {
        synchronized (bufferLock) {
            return buffer.size();
        }
    }

    public int pendingBytes() {
        int total = 0;
        synchronized (bufferLock) {
            for (BufferedEntry entry : buffer.values()) {
                total += entry.segment.length();
            }
        }
        return total;
    }

    /**
     * Force-flushes whatever is buffered, merging adjacent/overlapping segments.
     */
    public void flush() throws IOException {
        synchronized (bufferLock) {
            if (buffer.isEmpty()) {
                return;
            }
            bufferLock.notifyAll();
        }
        while (true) {
            synchronized (bufferLock) {
                if (buffer.isEmpty()) {
                    if (flushFailure != null) {
                        throw flushFailure;
                    }
                    return;
                }
                try {
                    bufferLock.wait(5_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("flush interrupted", e);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (bufferLock) {
            closed = true;
            bufferLock.notifyAll();
        }
        try {
            flusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("close interrupted", e);
        }
        if (flushFailure != null) {
            throw flushFailure;
        }
    }

    private void flusherLoop() {
        try {
            while (true) {
                List<BufferedEntry> drained;
                synchronized (bufferLock) {
                    while (!closed && buffer.isEmpty()) {
                        bufferLock.wait();
                    }
                    while (!closed && !flushDue()) {
                        long remaining = maxLingerNanos -
                                (System.nanoTime() - firstEnqueueNanos);
                        if (remaining <= 0L) {
                            break;
                        }
                        bufferLock.wait(remaining / 1_000_000L,
                                (int) (remaining % 1_000_000L));
                    }
                    if (closed && buffer.isEmpty()) {
                        return;
                    }
                    drained = buffer.drainAll();
                    firstEnqueueNanos = 0L;
                }
                flushDrained(drained);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            synchronized (bufferLock) {
                flushFailure = e;
                List<BufferedEntry> drained = buffer.drainAll();
                failAll(drained, e);
                bufferLock.notifyAll();
            }
        }
    }

    private boolean flushDue() {
        if (buffer.isEmpty()) {
            return false;
        }
        if (System.nanoTime() - firstEnqueueNanos >= maxLingerNanos) {
            return true;
        }
        int bytes = 0;
        for (BufferedEntry entry : buffer.values()) {
            bytes += entry.segment.length();
            if (bytes >= maxBatchBytes) {
                return true;
            }
        }
        return false;
    }

    private void flushDrained(List<BufferedEntry> drained) throws IOException {
        List<List<BufferedEntry>> batches = splitIntoBatches(drained);
        for (List<BufferedEntry> batch : batches) {
            List<WriteSegment> segments = new ArrayList<>(batch.size());
            for (BufferedEntry entry : batch) {
                segments.add(entry.segment);
            }
            List<WriteSegment> merged = mergeSegments(segments);
            long journalBytes = store.commitWriteBatch(merged);
            int mergedAway = segments.size() - merged.size();
            if (batchFlushListener != null) {
                batchFlushListener.accept(journalBytes, mergedAway);
            }
            for (BufferedEntry entry : batch) {
                completionCallback.accept(entry.request, null);
            }
        }
        synchronized (bufferLock) {
            bufferLock.notifyAll();
        }
    }

    private void failAll(List<BufferedEntry> drained, IOException error) {
        for (BufferedEntry entry : drained) {
            completionCallback.accept(entry.request, error);
        }
    }

    private List<List<BufferedEntry>> splitIntoBatches(List<BufferedEntry> drained) {
        List<List<BufferedEntry>> batches = new ArrayList<>();
        List<BufferedEntry> current = new ArrayList<>();
        int currentBytes = 0;
        for (BufferedEntry entry : drained) {
            int length = entry.segment.length();
            if (!current.isEmpty() && currentBytes + length > maxBatchBytes) {
                batches.add(current);
                current = new ArrayList<>();
                currentBytes = 0;
            }
            current.add(entry);
            currentBytes += length;
        }
        if (!current.isEmpty()) {
            batches.add(current);
        }
        return batches;
    }

    static List<WriteSegment> mergeSegments(List<WriteSegment> segments) {
        List<WriteSegment> sorted = new ArrayList<>(segments);
        sorted.sort((a, b) -> Long.compare(a.offset(), b.offset()));
        List<WriteSegment> merged = new ArrayList<>();
        for (WriteSegment segment : sorted) {
            if (merged.isEmpty()) {
                merged.add(segment);
                continue;
            }
            WriteSegment previous = merged.get(merged.size() - 1);
            if (segment.offset() <= previous.endOffset()) {
                merged.set(merged.size() - 1, overwriteMerge(previous, segment));
            } else {
                merged.add(segment);
            }
        }
        return merged;
    }

    private static WriteSegment overwriteMerge(WriteSegment first, WriteSegment second) {
        long start = Math.min(first.offset(), second.offset());
        long end = Math.max(first.endOffset(), second.endOffset());
        byte[] combined = new byte[(int) (end - start)];
        System.arraycopy(first.data(), 0, combined, (int) (first.offset() - start), first.length());
        System.arraycopy(second.data(), 0, combined, (int) (second.offset() - start), second.length());
        return new WriteSegment(start, combined);
    }

    private static final class TreeMapBuffer {
        private final java.util.TreeMap<Long, List<BufferedEntry>> entries = new java.util.TreeMap<>();

        void add(BufferedEntry entry) {
            entries.computeIfAbsent(entry.segment.offset(), key -> new ArrayList<>()).add(entry);
        }

        int size() {
            int count = 0;
            for (List<BufferedEntry> group : entries.values()) {
                count += group.size();
            }
            return count;
        }

        boolean isEmpty() {
            return entries.isEmpty();
        }

        Iterable<BufferedEntry> values() {
            List<BufferedEntry> all = new ArrayList<>();
            for (List<BufferedEntry> group : entries.values()) {
                all.addAll(group);
            }
            return all;
        }

        List<BufferedEntry> drainAll() {
            List<BufferedEntry> drained = new ArrayList<>();
            for (List<BufferedEntry> group : entries.values()) {
                drained.addAll(group);
            }
            entries.clear();
            return drained;
        }
    }
}
