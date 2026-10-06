package com.github.guoyyit.asyncioschedulerjava.config;

/**
 * Behaviour when the bounded pending-request capacity is exhausted.
 */
public enum CapacityExceededPolicy {
    /** Reject immediately with {@code QueueRejectedException} (legacy default). */
    REJECT,
    /**
     * Block the submitting thread up to the configured wait timeout for free
     * capacity; fail with a timeout rejection if none becomes available.
     */
    WAIT
}
