package com.github.guoyyit.asyncioschedulerjava.scheduling;

import com.github.guoyyit.asyncioschedulerjava.config.FairnessPolicy;
import com.github.guoyyit.asyncioschedulerjava.io.IoPriority;
import com.github.guoyyit.asyncioschedulerjava.io.IoRequest;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Bounded, per-priority queue with pluggable starvation protection.
 */
public class PriorityRequestQueue {

    private final int capacity;
    private final EnumMap<IoPriority, BlockingQueue<IoRequest>> queues =
            new EnumMap<>(IoPriority.class);
    private final Object lock = new Object();
    private volatile boolean closed;

    private final double[] deficits = new double[IoPriority.values().length];
    private volatile Runnable agingListener;

    public PriorityRequestQueue(int capacity) {
        this.capacity = capacity;
        for (IoPriority priority : IoPriority.values()) {
            queues.put(priority, new ArrayBlockingQueue<>(capacity));
        }
    }

    /**
     * Registers a callback fired each time a queued request is promoted by
     * aging (selected ahead of higher-priority requests).
     */
    public void setAgingListener(Runnable listener) {
        this.agingListener = listener;
    }

    public int size() {
        int total = 0;
        for (BlockingQueue<IoRequest> queue : queues.values()) {
            total += queue.size();
        }
        return total;
    }

    public boolean isEmpty() {
        for (BlockingQueue<IoRequest> queue : queues.values()) {
            if (!queue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Enqueues a request; the caller must already have enforced capacity.
     */
    public void put(IoRequest request) {
        if (!queues.get(request.priority()).offer(request)) {
            throw new IllegalStateException("per-priority queue capacity exceeded");
        }
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /**
     * Blocks until a request is available according to the current fairness
     * policy. Returns null if the queue was closed while waiting.
     */
    public IoRequest poll(long timeout, TimeUnit unit,
                         FairnessPolicy policy, long agingThresholdNanos,
                         double highWeight, double normalWeight, double lowWeight)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (lock) {
            while (!closed) {
                IoRequest selected = select(policy, agingThresholdNanos,
                        highWeight, normalWeight, lowWeight);
                if (selected != null) {
                    return selected;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                long sleepNanos = Math.min(remaining, 1_000_000_000L);
                long millis = sleepNanos / 1_000_000L;
                int nanos = (int) (sleepNanos % 1_000_000L);
                lock.wait(millis, nanos);
            }
            return select(FairnessPolicy.STRICT_PRIORITY, agingThresholdNanos,
                    highWeight, normalWeight, lowWeight);
        }
    }

    /**
     * Removes and returns one request of the exact given priority if present.
     */
    public IoRequest pollExact(IoPriority priority) {
        return queues.get(priority).poll();
    }

    public void close() {
        closed = true;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    private IoRequest select(FairnessPolicy policy, long agingThresholdNanos,
                             double highWeight, double normalWeight, double lowWeight) {
        long now = System.nanoTime();
        if (policy == FairnessPolicy.AGING_STRICT) {
            IoRequest oldestLow = oldestPromotable(now, agingThresholdNanos);
            if (oldestLow != null) {
                return oldestLow;
            }
            return pollHighestPriority();
        }
        if (policy == FairnessPolicy.WEIGHTED_FAIR) {
            return selectWeighted(highWeight, normalWeight, lowWeight);
        }
        return pollHighestPriority();
    }

    private IoRequest pollHighestPriority() {
        for (IoPriority priority : IoPriority.values()) {
            IoRequest request = queues.get(priority).poll();
            if (request != null) {
                return request;
            }
        }
        return null;
    }

    private IoRequest oldestPromotable(long now, long agingThresholdNanos) {
        IoRequest candidate = null;
        for (Map.Entry<IoPriority, BlockingQueue<IoRequest>> entry : queues.entrySet()) {
            if (entry.getKey() == IoPriority.HIGH) {
                continue;
            }
            IoRequest head = entry.getValue().peek();
            if (head != null && now - head.enqueueNanos() >= agingThresholdNanos) {
                if (candidate == null || head.enqueueNanos() < candidate.enqueueNanos()) {
                    candidate = head;
                }
            }
        }
        if (candidate == null) {
            return null;
        }
        Runnable listener = agingListener;
        if (listener != null) {
            listener.run();
        }
        return queues.get(candidate.priority()).poll();
    }

    private IoRequest selectWeighted(double highWeight, double normalWeight, double lowWeight) {
        double[] weights = {highWeight, normalWeight, lowWeight};
        refillIfAllDeficitsExhausted(weights);
        for (IoPriority priority : IoPriority.values()) {
            BlockingQueue<IoRequest> queue = queues.get(priority);
            int index = priority.value();
            if (!queue.isEmpty() && deficits[index] >= 1.0d) {
                deficits[index] -= 1.0d;
                return queue.poll();
            }
        }
        for (IoPriority priority : IoPriority.values()) {
            BlockingQueue<IoRequest> queue = queues.get(priority);
            if (queue.isEmpty()) {
                continue;
            }
            int index = priority.value();
            for (int rounds = 0; rounds < 64 && deficits[index] < 1.0d; rounds++) {
                refillDeficits(weights);
            }
            if (deficits[index] >= 1.0d) {
                deficits[index] -= 1.0d;
                return queue.poll();
            }
        }
        return null;
    }

    private void refillIfAllDeficitsExhausted(double[] weights) {
        for (IoPriority priority : IoPriority.values()) {
            if (!queues.get(priority).isEmpty() && deficits[priority.value()] >= 1.0d) {
                return;
            }
        }
        refillDeficits(weights);
    }

    private void refillDeficits(double[] weights) {
        for (IoPriority priority : IoPriority.values()) {
            deficits[priority.value()] += Math.max(0.0d, weights[priority.value()]);
        }
    }
}
