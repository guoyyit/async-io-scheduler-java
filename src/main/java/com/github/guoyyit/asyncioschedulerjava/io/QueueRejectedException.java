package com.github.guoyyit.asyncioschedulerjava.io;

/**
 * Thrown when a request cannot be accepted because the bounded pending queue is full.
 */
public class QueueRejectedException extends RuntimeException {

    private final RejectionReason reason;

    public QueueRejectedException(String message) {
        this(message, RejectionReason.UNSPECIFIED);
    }

    public QueueRejectedException(String message, RejectionReason reason) {
        super(message);
        this.reason = reason;
    }

    /**
     * Machine-readable cause of this rejection (never null).
     */
    public RejectionReason reason() {
        return reason;
    }
}
