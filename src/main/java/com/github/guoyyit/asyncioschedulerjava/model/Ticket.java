package com.github.guoyyit.asyncioschedulerjava.model;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Acceptance credential returned immediately after request submission.
 * The request is executed asynchronously; callers may poll or block on it.
 */
public final class Ticket<T> {

    private final long requestId;
    private final CompletableFuture<T> future;

    public Ticket(long requestId, CompletableFuture<T> future) {
        this.requestId = requestId;
        this.future = future;
    }

    public long getRequestId() {
        return requestId;
    }

    public boolean isDone() {
        return future.isDone();
    }

    public boolean isCompletedExceptionally() {
        return future.isCompletedExceptionally();
    }

    public T get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    public CompletableFuture<T> asFuture() {
        return future;
    }
}
