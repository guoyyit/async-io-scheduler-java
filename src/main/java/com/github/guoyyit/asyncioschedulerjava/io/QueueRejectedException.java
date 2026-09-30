package com.github.guoyyit.asyncioschedulerjava.io;

/**
 * Thrown when a request cannot be accepted because the bounded pending queue is full.
 */
public class QueueRejectedException extends RuntimeException {

    public QueueRejectedException(String message) {
        super(message);
    }
}
