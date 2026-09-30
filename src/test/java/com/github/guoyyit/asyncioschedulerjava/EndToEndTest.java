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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EndToEndTest {

    @Test
    void writesAreReadableWithinAndAcrossRestart(@TempDir Path directory) throws Exception {
        SchedulerConfig config = new SchedulerConfig();
        config.setBlockSize(64);
        config.setMaxBatchDelay(Duration.ofMillis(10));
        config.setMaxWriteBatchBlocks(8);

        byte[] payload = new byte[200];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i + 1);
        }

        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            Ticket<Void> write = scheduler.submitWrite("app", Priority.NORMAL, 30,
                    ByteBuffer.wrap(payload));
            write.get(5, TimeUnit.SECONDS);

            ByteBuffer dst = ByteBuffer.allocate(200);
            Ticket<Integer> read = scheduler.submitRead("app", Priority.NORMAL, 30, dst);
            int readBytes = read.get(5, TimeUnit.SECONDS);
            assertTrue(readBytes >= 200);
            assertArrayEquals(payload, dst.array());
        }

        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            ByteBuffer dst = ByteBuffer.allocate(200);
            Integer readBytes = scheduler.submitRead("app", Priority.NORMAL, 30, dst)
                    .get(5, TimeUnit.SECONDS);
            assertTrue(readBytes >= 200);
            assertArrayEquals(payload, dst.array());
        }
    }
}
