package com.github.guoyyit.asyncioschedulerjava.model;

/** Behaviour when the bounded pending queue is full. */
public enum OverflowPolicy {
    /** Reject immediately by completing the ticket exceptionally. */
    REJECT,
    /** Block the submitter until queue space is available. */
    BLOCK
}
