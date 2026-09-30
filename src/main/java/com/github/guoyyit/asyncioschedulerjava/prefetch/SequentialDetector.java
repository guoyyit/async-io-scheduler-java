package com.github.guoyyit.asyncioschedulerjava.prefetch;

/**
 * Per-stream access-pattern detector. A stream is identified by the scheduler
 * (submitter + coarse address region). Consecutive forward accesses grow the
 * detected sequential window; jumps that break the forward stride count as
 * random accesses and shrink/reset prefetch after the configured threshold.
 */
public final class SequentialDetector {

    /** Forward strides larger than this many blocks count as a random jump. */
    private static final int SEQUENTIAL_STRIDE_LIMIT = 2;

    public enum Mode {
        COLD,
        SEQUENTIAL,
        RANDOM
    }

    public static final class Advice {
        private final Mode mode;
        private final int windowBlocks;

        public Advice(Mode mode, int windowBlocks) {
            this.mode = mode;
            this.windowBlocks = windowBlocks;
        }

        public Mode getMode() {
            return mode;
        }

        public int getWindowBlocks() {
            return windowBlocks;
        }
    }

    private final int startWindow;
    private final int maxWindow;
    private final int randomThreshold;
    private final long idleResetNanos;

    private Mode mode = Mode.COLD;
    private long lastBlock = Long.MIN_VALUE;
    private long lastAccessNanos = Long.MIN_VALUE;
    private int window;
    private int randomStreak;
    private int sequentialStreak;

    public SequentialDetector(int startWindow, int maxWindow, int randomThreshold,
                              long idleResetNanos) {
        if (startWindow <= 0 || maxWindow < startWindow) {
            throw new IllegalArgumentException("invalid prefetch window bounds");
        }
        this.startWindow = startWindow;
        this.maxWindow = maxWindow;
        this.randomThreshold = Math.max(1, randomThreshold);
        this.idleResetNanos = idleResetNanos;
        this.window = startWindow;
    }

    /** Observe a user access and return the current mode + advised window. */
    public Advice observe(long blockIndex, long nowNanos) {
        if (lastAccessNanos != Long.MIN_VALUE
                && nowNanos - lastAccessNanos > idleResetNanos) {
            resetState();
        }

        if (lastBlock == Long.MIN_VALUE) {
            lastBlock = blockIndex;
            lastAccessNanos = nowNanos;
            mode = Mode.COLD;
            return new Advice(mode, 0);
        }

        long delta = blockIndex - lastBlock;
        lastBlock = blockIndex;
        lastAccessNanos = nowNanos;

        if (delta >= 1 && delta <= SEQUENTIAL_STRIDE_LIMIT) {
            sequentialStreak++;
            randomStreak = 0;
            mode = Mode.SEQUENTIAL;
            window = Math.min(maxWindow,
                    window <= 0 ? startWindow : Math.min(maxWindow, window * 2));
            return new Advice(mode, window);
        }

        sequentialStreak = 0;
        randomStreak++;
        if (randomStreak >= randomThreshold) {
            mode = Mode.RANDOM;
            window = 0;
            return new Advice(mode, 0);
        }
        if (mode == Mode.RANDOM) {
            return new Advice(mode, 0);
        } else {
            window = Math.max(startWindow, window / 2);
        }
        mode = Mode.COLD;
        return new Advice(mode, window);
    }

    public Mode getMode() {
        return mode;
    }

    public void reset() {
        resetState();
    }

    private void resetState() {
        mode = Mode.COLD;
        lastBlock = Long.MIN_VALUE;
        lastAccessNanos = Long.MIN_VALUE;
        window = startWindow;
        randomStreak = 0;
        sequentialStreak = 0;
    }
}
