package com.github.guoyyit.asyncioschedulerjava.queue;

import com.github.guoyyit.asyncioschedulerjava.api.QueueRejectedException;
import com.github.guoyyit.asyncioschedulerjava.model.IoRequest;
import com.github.guoyyit.asyncioschedulerjava.model.OverflowPolicy;
import com.github.guoyyit.asyncioschedulerjava.scheduling.PrioritySelector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded multi-priority pending queue shared by all submitters.
 *
 * <p>Bounds: a global cap plus a per-submitter cap so a single slow/failing
 * submitter cannot exhaust the queue. {@link OverflowPolicy#BLOCK} parks the
 * submitting thread; {@link OverflowPolicy#REJECT} fails fast with
 * {@link QueueRejectedException}.
 */
public final class PendingRequestQueue {

    private final int maxTotal;
    private final int maxPerSubmitter;
    private final OverflowPolicy overflowPolicy;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition workAvailable = lock.newCondition();

    private final List<IoRequest> pending = new ArrayList<>();
    private final Map<String, Integer> perSubmitter = new HashMap<>();
    private boolean closed;

    public PendingRequestQueue(int maxTotal, int maxPerSubmitter,
                               OverflowPolicy overflowPolicy) {
        if (maxTotal <= 0 || maxPerSubmitter <= 0) {
            throw new IllegalArgumentException("queue bounds must be positive");
        }
        this.maxTotal = maxTotal;
        this.maxPerSubmitter = Math.min(maxPerSubmitter, maxTotal);
        this.overflowPolicy = overflowPolicy;
    }

    /** Enqueue honoring the overflow policy; wakes the scheduler thread. */
    public void enqueue(IoRequest request) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                if (closed) {
                    throw new com.github.guoyyit.asyncioschedulerjava.api
                            .SchedulerClosedException("scheduler is closed");
                }
                int submitterCount = perSubmitter.getOrDefault(request.getSubmitter(), 0);
                boolean full = pending.size() >= maxTotal
                        || submitterCount >= maxPerSubmitter;
                if (!full) {
                    pending.add(request);
                    perSubmitter.put(request.getSubmitter(), submitterCount + 1);
                    workAvailable.signal();
                    return;
                }
                if (overflowPolicy == OverflowPolicy.REJECT) {
                    throw new QueueRejectedException(
                            "pending queue full for submitter '" + request.getSubmitter() + '\'');
                }
                notFull.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Select and remove one request using {@code selector}, waiting up to
     * {@code timeoutNanos} for work when none is eligible. Returns null on
     * timeout or after close once the queue is empty.
     */
    public IoRequest select(PrioritySelector selector, long nowNanos, long timeoutNanos)
            throws InterruptedException {
        lock.lockInterruptibly();
        try {
            long remainingNanos = timeoutNanos;
            while (pending.isEmpty()) {
                if (closed || remainingNanos <= 0) {
                    return null;
                }
                remainingNanos = workAvailable.awaitNanos(remainingNanos);
            }

            IoRequest chosen = selector.select(List.copyOf(pending), nowNanos);
            if (chosen == null) {
                return null;
            }
            if (!pending.remove(chosen)) {
                return null;
            }
            int count = perSubmitter.getOrDefault(chosen.getSubmitter(), 0) - 1;
            if (count <= 0) {
                perSubmitter.remove(chosen.getSubmitter());
            } else {
                perSubmitter.put(chosen.getSubmitter(), count);
            }
            notFull.signalAll();
            return chosen;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return pending.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Re-insert a request previously taken by {@link #select} (quota delay
     * retry). Used only by the scheduler thread; it does not re-charge
     * submitter space because the request remains counted as pending work.
     */
    public void requeue(IoRequest request) {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            pending.add(0, request);
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    public int sizeOf(String submitter) {
        lock.lock();
        try {
            return perSubmitter.getOrDefault(submitter, 0);
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return pending.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /** Wake blocked submitters and let the scheduler drain remaining work. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notFull.signalAll();
            workAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
