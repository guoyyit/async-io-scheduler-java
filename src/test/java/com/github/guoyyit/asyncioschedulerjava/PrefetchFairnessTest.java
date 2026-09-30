package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.model.Priority;
import com.github.guoyyit.asyncioschedulerjava.model.Ticket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

class PrefetchFairnessTest {

    private SchedulerConfig config() {
        SchedulerConfig config = new SchedulerConfig();
        config.setBlockSize(64);
        config.setMaxBatchDelay(Duration.ofMillis(5));
        config.setPrefetchMaxWindowBlocks(8);
        return config;
    }

    @Test
    void pendingWriteIsServedBeforePrefetch(@TempDir Path directory) throws Exception {
        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config())) {
            for (int block = 0; block < 6; block++) {
                scheduler.submitRead("seq", Priority.NORMAL, block * 64L,
                        ByteBuffer.allocate(64)).get(5, TimeUnit.SECONDS);
            }

            Ticket<Void> write = scheduler.submitWrite("writer", Priority.LOW,
                    200_000L, ByteBuffer.wrap(new byte[64]));
            Ticket<Integer> lowRead = scheduler.submitRead("lowreader", Priority.LOW,
                    300_000L, ByteBuffer.allocate(64));

            write.get(5, TimeUnit.SECONDS);
            lowRead.get(5, TimeUnit.SECONDS);
        }
    }
}
