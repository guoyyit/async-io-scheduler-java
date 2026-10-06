package com.github.guoyyit.asyncioschedulerjava.io;

/**
 * Thrown when a request cannot be accepted because the bounded pending queue
 * is full (or the scheduler is not running). Carries a machine-readable
 * {@link Reason} so callers can distinguish overload causes.
 */
public class QueueRejectedException extends RuntimeException {

    /**
     * Why a submission was rejected.
     */
    public enum Reason {
        /** Scheduler was never started or is already shut down. */
        SCHEDULER_NOT_RUNNING,
        /** Global pending-request limit reached (immediate-reject mode). */
        GLOBAL_CAPACITY_EXHAUSTED,
        /** Per-submitter pending-request limit reached (immediate-reject mode). */
        SUBMITTER_CAPACITY_EXHAUSTED,
        /** Waited for capacity longer than the configured wait timeout. */
        WAIT_TIMEOUT,
        /** Reason not specified (legacy construction). */
        UNSPECIFIED
    }

    private final Reason reason;

    public QueueRejectedException(String message) {
        this(message, Reason.UNSPECIFIED);
    }

    public QueueRejectedException(String message, Reason reason) {
        super(message);
        this.reason = reason == null ? Reason.UNSPECIFIED : reason;
    }

    public Reason reason() {
        return reason;
    }
}
