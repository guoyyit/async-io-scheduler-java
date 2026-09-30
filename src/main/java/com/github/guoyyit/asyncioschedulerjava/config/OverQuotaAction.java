package com.github.guoyyit.asyncioschedulerjava.config;

/**
 * What happens to a request that is over its configured bandwidth quota.
 */
public enum OverQuotaAction {
    /** Request is delayed until quota tokens are available. */
    DELAY,
    /** Request is still served but temporarily demoted to lower priority. */
    DEGRADE,
    /** Request is rejected immediately. */
    REJECT
}
