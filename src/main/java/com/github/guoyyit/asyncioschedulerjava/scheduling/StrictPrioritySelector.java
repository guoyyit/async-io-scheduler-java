package com.github.guoyyit.asyncioschedulerjava.scheduling;

import com.github.guoyyit.asyncioschedulerjava.model.IoRequest;

import java.util.List;

/** Always picks the highest-priority pending request; older first per level. */
public final class StrictPrioritySelector implements PrioritySelector {

    @Override
    public IoRequest select(List<IoRequest> requests, long nowNanos) {
        IoRequest best = null;
        for (IoRequest request : requests) {
            if (best == null || isBefore(request, best)) {
                best = request;
            }
        }
        return best;
    }

    private static boolean isBefore(IoRequest candidate, IoRequest current) {
        int byPriority = Integer.compare(candidate.getPriority().ordinal(),
                current.getPriority().ordinal());
        if (byPriority != 0) {
            return byPriority > 0;
        }
        return candidate.getId() < current.getId();
    }
}
