package com.github.guoyyit.asyncioschedulerjava.io;

/**
 * Completed exceptionally when an accepted request waited in the scheduling
 * queue longer than the configured maximum queue wait. Bounds the service
 * time of aged-out requests under sustained overload.
 */
public class QueueWaitTimeoutException extends RuntimeException {

    public QueueWaitTimeoutException(String message) {
        super(message);
    }
}
