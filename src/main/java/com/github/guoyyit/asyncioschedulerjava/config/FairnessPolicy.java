package com.github.guoyyit.asyncioschedulerjava.config;

/**
 * How the scheduler trades strict priority ordering against starvation protection.
 */
public enum FairnessPolicy {
    /**
     * Always serve the highest available priority. Low priority requests can starve.
     */
    STRICT_PRIORITY,
    /**
     * Strict priority, but a request is promoted to the highest priority once it has
     * waited longer than the configured aging threshold. Prevents long-term starvation.
     */
    AGING_STRICT,
    /**
     * Per-priority weighted round-robin. Each priority receives a configured share of
     * service; higher priorities get larger shares but nobody starves.
     */
    WEIGHTED_FAIR
}
