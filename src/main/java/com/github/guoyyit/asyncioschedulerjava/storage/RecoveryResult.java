package com.github.guoyyit.asyncioschedulerjava.storage;

import java.util.List;

/** Outcome of WAL replay: committed batches applied and torn batches discarded. */
public final class RecoveryResult {

    private final List<WriteBatch> recoveredBatches;
    private final int discardedBatches;
    private final int discardedRecords;

    public RecoveryResult(List<WriteBatch> recoveredBatches, int discardedBatches,
                          int discardedRecords) {
        this.recoveredBatches = recoveredBatches;
        this.discardedBatches = discardedBatches;
        this.discardedRecords = discardedRecords;
    }

    public List<WriteBatch> getRecoveredBatches() {
        return recoveredBatches;
    }

    public int getDiscardedBatches() {
        return discardedBatches;
    }

    public int getDiscardedRecords() {
        return discardedRecords;
    }
}
