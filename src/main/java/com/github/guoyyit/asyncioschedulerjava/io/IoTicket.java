package com.github.guoyyit.asyncioschedulerjava.io;

import java.util.concurrent.CompletableFuture;

/**
 * Acceptance credential returned immediately upon request submission.
 */
public final class IoTicket {

    private final long requestId;
    private final CompletableFuture<IoCompletion> completion;

    public IoTicket(long requestId, CompletableFuture<IoCompletion> completion) {
        this.requestId = requestId;
        this.completion = completion;
    }

    public long requestId() {
        return requestId;
    }

    public CompletableFuture<IoCompletion> completion() {
        return completion;
    }
}
