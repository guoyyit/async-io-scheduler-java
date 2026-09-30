package com.github.guoyyit.asyncioschedulerjava.quota;

import com.github.guoyyit.asyncioschedulerjava.model.OperationType;
import com.github.guoyyit.asyncioschedulerjava.model.QuotaSnapshot;
import com.github.guoyyit.asyncioschedulerjava.model.SubmitterQuota;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-submitter fixed-window bandwidth quotas. Limits are bytes per window;
 * usage resets at each window boundary. A zero limit means unlimited. Global
 * defaults apply to submitters without an explicit quota.
 */
public final class QuotaManager {

    private static final class State {
        private long readLimit;
        private long writeLimit;
        private long readUsed;
        private long writeUsed;
        private long windowStartNanos;
        private final AtomicLong delayed = new AtomicLong();
    }

    private final long defaultReadLimit;
    private final long defaultWriteLimit;
    private final long windowNanos;
    private final Map<String, State> states = new ConcurrentHashMap<>();

    public QuotaManager(long defaultReadBytesPerSec, long defaultWriteBytesPerSec,
                        long windowNanos) {
        this.defaultReadLimit = Math.max(0L, defaultReadBytesPerSec);
        this.defaultWriteLimit = Math.max(0L, defaultWriteBytesPerSec);
        this.windowNanos = windowNanos;
    }

    public void setQuota(String submitter, long readBytesPerSec, long writeBytesPerSec) {
        State state = stateFor(submitter);
        synchronized (state) {
            state.readLimit = Math.max(0L, readBytesPerSec);
            state.writeLimit = Math.max(0L, writeBytesPerSec);
        }
    }

    public SubmitterQuota quotaOf(String submitter) {
        State state = stateFor(submitter);
        synchronized (state) {
            return new SubmitterQuota(state.readLimit, state.writeLimit);
        }
    }

    /** Roll the window if needed, then check capacity without consuming. */
    public boolean isAllowed(String submitter, OperationType type, int bytes, long nowNanos) {
        State state = stateFor(submitter);
        synchronized (state) {
            rollWindow(state, nowNanos);
            long limit = type == OperationType.READ ? state.readLimit : state.writeLimit;
            long used = type == OperationType.READ ? state.readUsed : state.writeUsed;
            return limit == 0 || used + bytes <= limit;
        }
    }

    /** Charge {@code bytes}; caller must have checked {@link #isAllowed}. */
    public void acquire(String submitter, OperationType type, int bytes, long nowNanos) {
        State state = stateFor(submitter);
        synchronized (state) {
            rollWindow(state, nowNanos);
            if (type == OperationType.READ) {
                state.readUsed += bytes;
            } else {
                state.writeUsed += bytes;
            }
        }
    }

    /** Time until enough quota is available for {@code bytes}. */
    public long nanosUntilAllowed(String submitter, OperationType type, int bytes,
                                  long nowNanos) {
        State state = stateFor(submitter);
        synchronized (state) {
            rollWindow(state, nowNanos);
            long limit = type == OperationType.READ ? state.readLimit : state.writeLimit;
            if (limit == 0) {
                return 0L;
            }
            long used = type == OperationType.READ ? state.readUsed : state.writeUsed;
            if (used + bytes <= limit) {
                return 0L;
            }
            return state.windowStartNanos + windowNanos - nowNanos;
        }
    }

    public void incrementDelayed(String submitter) {
        stateFor(submitter).delayed.incrementAndGet();
    }

    public QuotaSnapshot snapshot(String submitter, long nowNanos) {
        State state = stateFor(submitter);
        synchronized (state) {
            rollWindow(state, nowNanos);
            return new QuotaSnapshot(submitter, state.readLimit, state.writeLimit,
                    state.readUsed, state.writeUsed, state.delayed.get());
        }
    }

    private State stateFor(String submitter) {
        return states.computeIfAbsent(submitter, key -> {
            State state = new State();
            state.readLimit = defaultReadLimit;
            state.writeLimit = defaultWriteLimit;
            return state;
        });
    }

    private void rollWindow(State state, long nowNanos) {
        if (state.windowStartNanos == 0L) {
            state.windowStartNanos = nowNanos;
            return;
        }
        if (nowNanos - state.windowStartNanos >= windowNanos) {
            state.readUsed = 0L;
            state.writeUsed = 0L;
            state.windowStartNanos = nowNanos;
        }
    }
}
