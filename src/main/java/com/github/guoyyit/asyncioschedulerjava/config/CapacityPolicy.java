package com.github.guoyyit.asyncioschedulerjava.config;

/**
 * What happens when a submission arrives while the pending-request capacity
 * (global or per-submitter) is exhausted.
 */
public enum CapacityPolicy {
    /**
     * Reject immediately with a {@code QueueRejectedException} carrying a
     * machine-readable reason. Historical default.
     */
    REJECT,
    /**
     * Block the submitting thread until capacity becomes available or the
     * configured wait timeout elapses, then fail with a timeout reason.
     */
    WAIT
}
