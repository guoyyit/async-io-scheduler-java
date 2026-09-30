package com.github.guoyyit.asyncioschedulerjava.prefetch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequentialDetectorTest {

    private SequentialDetector detector() {
        return new SequentialDetector(2, 16, 3, 1_000_000L);
    }

    @Test
    void growsWindowOnSequentialAccess() {
        SequentialDetector detector = detector();
        long now = 0L;
        detector.observe(0L, now);
        SequentialDetector.Advice second = detector.observe(1L, now + 1);
        assertEquals(SequentialDetector.Mode.SEQUENTIAL, second.getMode());
        int firstWindow = second.getWindowBlocks();
        SequentialDetector.Advice third = detector.observe(2L, now + 2);
        assertTrue(third.getWindowBlocks() >= firstWindow);
    }

    @Test
    void backsOffToZeroOnRepeatedRandomAccess() {
        SequentialDetector detector = detector();
        long now = 0L;
        detector.observe(0L, now);
        detector.observe(1L, now + 1);
        detector.observe(2L, now + 2);

        SequentialDetector.Advice r1 = detector.observe(100L, now + 3);
        SequentialDetector.Advice r2 = detector.observe(5L, now + 4);
        SequentialDetector.Advice r3 = detector.observe(80L, now + 5);
        assertEquals(SequentialDetector.Mode.RANDOM, r3.getMode());
        assertEquals(0, r3.getWindowBlocks());
        assertTrue(r1.getWindowBlocks() >= 0 && r2.getWindowBlocks() >= 0);
    }

    @Test
    void resetsAfterIdleGap() {
        SequentialDetector detector = detector();
        long now = 0L;
        detector.observe(10L, now);
        detector.observe(11L, now + 1);

        SequentialDetector.Advice afterIdle = detector.observe(500L, now + 5_000_000L);
        assertEquals(SequentialDetector.Mode.COLD, afterIdle.getMode());
        assertEquals(0, afterIdle.getWindowBlocks());
    }
}
