package com.github.guoyyit.asyncioschedulerjava.quota;

/**
 * Completed exceptionally when a request was executed under the REJECT
 * over-quota policy and the submitter had no tokens available.
 */
public class QuotaExceededException extends RuntimeException {

    public QuotaExceededException(String message) {
        super(message);
    }
}
