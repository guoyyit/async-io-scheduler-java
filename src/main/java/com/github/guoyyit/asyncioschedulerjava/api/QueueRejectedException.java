package com.github.guoyyit.asyncioschedulerjava.api;

/** Thrown (or used to fail a ticket) when the bounded queue rejects a request. */
public class QueueRejectedException extends RuntimeException {

    public QueueRejectedException() {
        super();
    }

    public QueueRejectedException(String message) {
        super(message);
    }
}
