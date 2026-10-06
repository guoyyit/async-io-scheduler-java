package com.github.guoyyit.asyncioschedulerjava.io;

/**
 * Machine-readable reason why a submission was rejected, attached to
 * {@link QueueRejectedException} so callers can distinguish causes.
 */
public enum RejectionReason {
    /** Global pending-request capacity was exhausted. */
    GLOBAL_CAPACITY,
    /** The submitter's own pending-request quota was exhausted. */
    SUBMITTER_CAPACITY,
    /** Waiting for free capacity exceeded the configured wait timeout. */
    WAIT_TIMEOUT,
    /** The scheduler was not running when the request was submitted. */
    NOT_RUNNING,
    /** Legacy rejection without a specific reason. */
    UNSPECIFIED
}
