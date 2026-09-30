package com.github.guoyyit.asyncioschedulerjava.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded LRU cache of block data. Blocks loaded speculatively by prefetch are
 * flagged so the scheduler can credit prefetch hits and charge prefetch waste
 * when they are evicted before the first user access.
 */
public final class BlockCache {

    private final int capacityBlocks;
    private final int blockSize;
    private final LinkedHashMap<Long, Entry> entries;

    private static final class Entry {
        private final ByteBuffer data;
        private boolean prefetched;

        private Entry(ByteBuffer data, boolean prefetched) {
            this.data = data;
            this.prefetched = prefetched;
        }
    }

    public BlockCache(int capacityBlocks, int blockSize) {
        if (capacityBlocks <= 0) {
            throw new IllegalArgumentException("capacityBlocks must be positive");
        }
        this.capacityBlocks = capacityBlocks;
        this.blockSize = blockSize;
        this.entries = new LinkedHashMap<>(16, 0.75f, true);
    }

    /** Lookup a block; marks it accessed and clears its "prefetched" flag. */
    public ByteBuffer get(long blockIndex) {
        synchronized (entries) {
            Entry entry = entries.get(blockIndex);
            if (entry == null) {
                return null;
            }
            entry.prefetched = false;
            return copyOf(entry.data);
        }
    }

    /** Insert/replace a block; returns prefetched blocks wasted by this insert. */
    public Collection<Long> put(long blockIndex, ByteBuffer data, boolean prefetched) {
        if (data.remaining() != blockSize) {
            throw new IllegalArgumentException("data must contain exactly one block");
        }
        List<Long> wasted = new ArrayList<>();
        synchronized (entries) {
            Entry previous = entries.put(blockIndex,
                    new Entry(copyOf(data), prefetched));
            if (previous != null && previous.prefetched) {
                wasted.add(blockIndex);
            }
            while (entries.size() > capacityBlocks) {
                Map.Entry<Long, Entry> eldest = entries.entrySet().iterator().next();
                entries.remove(eldest.getKey());
                if (eldest.getValue().prefetched) {
                    wasted.add(eldest.getKey());
                }
            }
        }
        return Collections.unmodifiableList(wasted);
    }

    /** Was this block loaded by prefetch and not yet consumed by a user read? */
    public boolean isPrefetchResident(long blockIndex) {
        synchronized (entries) {
            Entry entry = entries.get(blockIndex);
            return entry != null && entry.prefetched;
        }
    }

    public int size() {
        synchronized (entries) {
            return entries.size();
        }
    }

    public void clear() {
        synchronized (entries) {
            entries.clear();
        }
    }

    private static ByteBuffer copyOf(ByteBuffer source) {
        ByteBuffer duplicate = source.duplicate();
        ByteBuffer copy = ByteBuffer.allocate(duplicate.remaining());
        copy.put(duplicate);
        copy.flip();
        return copy;
    }
}
