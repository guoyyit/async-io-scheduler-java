package com.github.guoyyit.asyncioschedulerjava.api;

/** Thrown when submitting to a scheduler that has been closed. */
public class SchedulerClosedException extends IllegalStateException {

    public SchedulerClosedException() {
        super();
    }

    public SchedulerClosedException(String message) {
        super(message);
    }
}
