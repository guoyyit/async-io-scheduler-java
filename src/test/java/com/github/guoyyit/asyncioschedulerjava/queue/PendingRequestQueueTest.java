package com.github.guoyyit.asyncioschedulerjava.queue;

import com.github.guoyyit.asyncioschedulerjava.api.QueueRejectedException;
import com.github.guoyyit.asyncioschedulerjava.model.IoRequest;
import com.github.guoyyit.asyncioschedulerjava.model.OperationType;
import com.github.guoyyit.asyncioschedulerjava.model.OverflowPolicy;
import com.github.guoyyit.asyncioschedulerjava.model.Priority;
import com.github.guoyyit.asyncioschedulerjava.scheduling.StrictPrioritySelector;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PendingRequestQueueTest {

    private static IoRequest request(long id, String submitter, Priority priority) {
        return new IoRequest(id, submitter, OperationType.WRITE, priority,
                id * 64L, 64, ByteBuffer.allocate(64), System.nanoTime());
    }

    @Test
    void rejectPolicyFailsWhenGlobalBoundReached() throws Exception {
        PendingRequestQueue queue = new PendingRequestQueue(2, 8, OverflowPolicy.REJECT);
        queue.enqueue(request(1, "a", Priority.NORMAL));
        queue.enqueue(request(2, "b", Priority.NORMAL));
        assertThrows(QueueRejectedException.class,
                () -> queue.enqueue(request(3, "c", Priority.NORMAL)));
        assertEquals(2, queue.size());
    }

    @Test
    void perSubmitterBoundCapsOneSubmitterButKeepsOthersWorking() throws Exception {
        PendingRequestQueue queue = new PendingRequestQueue(8, 2, OverflowPolicy.REJECT);
        queue.enqueue(request(1, "slow", Priority.NORMAL));
        queue.enqueue(request(2, "slow", Priority.NORMAL));
        assertThrows(QueueRejectedException.class,
                () -> queue.enqueue(request(3, "slow", Priority.NORMAL)));

        queue.enqueue(request(4, "fast", Priority.NORMAL));
        assertEquals(2, queue.sizeOf("slow"));
        assertEquals(1, queue.sizeOf("fast"));
    }

    @Test
    void selectionFreesSpaceForBlockedSubmitter() throws Exception {
        PendingRequestQueue queue = new PendingRequestQueue(1, 8, OverflowPolicy.BLOCK);
        queue.enqueue(request(1, "a", Priority.HIGH));

        Thread blocker = new Thread(() -> {
            try {
                queue.enqueue(request(2, "a", Priority.NORMAL));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        blocker.start();

        Thread.sleep(50);
        IoRequest selected = queue.select(new StrictPrioritySelector(),
                System.nanoTime(), 1_000_000L);
        assertEquals(1L, selected.getId());
        blocker.join(2000);
        assertEquals(1, queue.size());
        queue.close();
    }
}
