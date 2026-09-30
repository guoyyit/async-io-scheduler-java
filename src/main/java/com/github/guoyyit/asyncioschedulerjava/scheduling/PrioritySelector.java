package com.github.guoyyit.asyncioschedulerjava.scheduling;

import com.github.guoyyit.asyncioschedulerjava.model.IoRequest;

import java.util.List;

/**
 * Chooses the next request to execute among all pending requests.
 * Implementations must be non-blocking.
 */
@FunctionalInterface
public interface PrioritySelector {

    /**
     * @param requests immutable snapshot of every pending request
     * @param nowNanos scheduler clock
     * @return chosen request (one of {@code requests}) or {@code null}
     */
    IoRequest select(List<IoRequest> requests, long nowNanos);
}
