package com.github.guoyyit.asyncioschedulerjava.model;

/** Behaviour when a submitter exceeds its bandwidth quota. */
public enum QuotaAction {
    /** Keep the request queued until quota is replenished. */
    DELAY,
    /** Reject the request immediately. */
    REJECT
}
