package com.github.guoyyit.asyncioschedulerjava.api;

/** Used to fail a ticket when a request exceeds its quota and policy is REJECT. */
public class QuotaExceededException extends RuntimeException {

    public QuotaExceededException(String message) {
        super(message);
    }
}
