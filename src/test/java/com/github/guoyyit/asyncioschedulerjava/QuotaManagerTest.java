package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.OverQuotaAction;
import com.github.guoyyit.asyncioschedulerjava.io.IoType;
import com.github.guoyyit.asyncioschedulerjava.quota.QuotaManager;
import com.github.guoyyit.asyncioschedulerjava.quota.QuotaSnapshot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuotaManagerTest {

    @Test
    void delayPolicyBlocksUntilTokensRefill() throws Exception {
        QuotaManager manager = new QuotaManager(1000, 1000, OverQuotaAction.DELAY);
        manager.setQuota("a", IoType.READ, 1000);
        long start = System.nanoTime();
        manager.reserve("a", IoType.READ, 1000);
        OverQuotaAction second = manager.reserve("a", IoType.READ, 1000);
        long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS
                .toMillis(System.nanoTime() - start);
        assertEquals(OverQuotaAction.DELAY, second, "second reservation had to wait");
        assertTrue(elapsedMs >= 800, "expected ~1s refill delay, was " + elapsedMs + "ms");
        assertTrue(manager.snapshot("a", IoType.READ).delayedCount() >= 1L);
    }

    @Test
    void rejectPolicyDoesNotBlockAndReturnsReject() throws Exception {
        QuotaManager manager = new QuotaManager(1000, 1000, OverQuotaAction.REJECT);
        manager.setQuota("a", IoType.WRITE, 100);
        assertNull(manager.reserve("a", IoType.WRITE, 100));
        OverQuotaAction over = manager.reserve("a", IoType.WRITE, 100);
        assertEquals(OverQuotaAction.REJECT, over);
    }

    @Test
    void degradePolicySignalsDegradeWithoutBlocking() throws Exception {
        QuotaManager manager = new QuotaManager(1000, 1000, OverQuotaAction.DEGRADE);
        manager.setQuota("a", IoType.READ, 100);
        assertNull(manager.reserve("a", IoType.READ, 100));
        assertEquals(OverQuotaAction.DEGRADE, manager.reserve("a", IoType.READ, 100));
    }

    @Test
    void refundReturnsTokensAndSnapshotsAreQueryable() throws Exception {
        QuotaManager manager = new QuotaManager(1000, 1000, OverQuotaAction.REJECT);
        manager.setQuota("a", IoType.READ, 100);
        manager.reserve("a", IoType.READ, 100);
        assertEquals(OverQuotaAction.REJECT, manager.reserve("a", IoType.READ, 1));
        manager.refund("a", IoType.READ, 100);
        assertNull(manager.reserve("a", IoType.READ, 100));

        QuotaSnapshot snapshot = manager.snapshot("a", IoType.READ);
        assertEquals("a", snapshot.submitter());
        assertEquals(100L, snapshot.limitBytesPerSec());
        assertEquals(2, manager.snapshots().size(), "one submitter produces read+write snapshots");
    }

    @Test
    void quotasAreIndependentPerSubmitterAndDirection() throws Exception {
        QuotaManager manager = new QuotaManager(Long.MAX_VALUE, Long.MAX_VALUE,
                OverQuotaAction.REJECT);
        manager.setQuota("a", IoType.READ, 50);
        manager.reserve("a", IoType.READ, 50);
        assertEquals(OverQuotaAction.REJECT, manager.reserve("a", IoType.READ, 1));
        assertNull(manager.reserve("a", IoType.WRITE, 1_000_000));
        assertNull(manager.reserve("b", IoType.READ, 1_000_000));
    }
}
