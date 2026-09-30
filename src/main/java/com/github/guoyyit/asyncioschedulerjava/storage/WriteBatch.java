package com.github.guoyyit.asyncioschedulerjava.storage;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * One ordered group of block writes applied and acknowledged atomically:
 * either every write in the batch is visible after recovery or none is.
 */
public final class WriteBatch {

    public static final class Entry {
        private final long blockIndex;
        private final ByteBuffer data;

        public Entry(long blockIndex, ByteBuffer data) {
            this.blockIndex = blockIndex;
            this.data = data;
        }

        public long getBlockIndex() {
            return blockIndex;
        }

        public ByteBuffer getData() {
            return data;
        }
    }

    private final long batchId;
    private final List<Entry> entries;

    public WriteBatch(long batchId, List<Entry> entries) {
        this.batchId = batchId;
        this.entries = entries;
    }

    public long getBatchId() {
        return batchId;
    }

    public List<Entry> getEntries() {
        return entries;
    }
}
