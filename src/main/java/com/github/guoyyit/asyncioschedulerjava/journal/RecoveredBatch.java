package com.github.guoyyit.asyncioschedulerjava.journal;

import com.github.guoyyit.asyncioschedulerjava.io.WriteSegment;

import java.util.List;

/**
 * A write batch recovered from the journal during startup.
 */
public final class RecoveredBatch {

    private final long sequence;
    private final List<WriteSegment> segments;
    private final long endOffset;

    public RecoveredBatch(long sequence, List<WriteSegment> segments, long endOffset) {
        this.sequence = sequence;
        this.segments = List.copyOf(segments);
        this.endOffset = endOffset;
    }

    public long sequence() {
        return sequence;
    }

    public List<WriteSegment> segments() {
        return segments;
    }

    public long endOffset() {
        return endOffset;
    }
}
