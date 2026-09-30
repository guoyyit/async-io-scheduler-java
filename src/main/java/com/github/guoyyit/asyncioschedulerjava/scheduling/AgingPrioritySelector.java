package com.github.guoyyit.asyncioschedulerjava.scheduling;

import com.github.guoyyit.asyncioschedulerjava.model.IoRequest;

import java.time.Duration;
import java.util.List;

/**
 * Priority-with-aging selector: requests older than the starvation threshold
 * gain one effective priority level per threshold interval, so a continuous
 * high-priority stream cannot permanently starve low-priority requests.
 */
public final class AgingPrioritySelector implements PrioritySelector {

    private final long thresholdNanos;

    public AgingPrioritySelector(Duration starvationAgeThreshold) {
        this.thresholdNanos = starvationAgeThreshold.toNanos();
    }

    @Override
    public IoRequest select(List<IoRequest> requests, long nowNanos) {
        IoRequest best = null;
        long bestEffective = Long.MIN_VALUE;
        for (IoRequest request : requests) {
            long effective = effectiveLevel(request, nowNanos);
            if (best == null
                    || effective > bestEffective
                    || (effective == bestEffective && request.getId() < best.getId())) {
                best = request;
                bestEffective = effective;
            }
        }
        return best;
    }

    private long effectiveLevel(IoRequest request, long nowNanos) {
        long ageNanos = Math.max(0L, nowNanos - request.getEnqueueNanos());
        long promotions = ageNanos / thresholdNanos;
        return request.getPriority().ordinal() + promotions;
    }
}
