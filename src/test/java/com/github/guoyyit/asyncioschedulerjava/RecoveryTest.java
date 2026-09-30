package com.github.guoyyit.asyncioschedulerjava;

import com.github.guoyyit.asyncioschedulerjava.config.SchedulerConfig;
import com.github.guoyyit.asyncioschedulerjava.model.Priority;
import com.github.guoyyit.asyncioschedulerjava.storage.FileDurableChannel;
import com.github.guoyyit.asyncioschedulerjava.storage.WriteAheadLog;
import com.github.guoyyit.asyncioschedulerjava.storage.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RecoveryTest {

    private SchedulerConfig config() {
        SchedulerConfig config = new SchedulerConfig();
        config.setBlockSize(64);
        config.setMaxBatchDelay(Duration.ofMillis(10));
        config.setMaxWriteBatchBlocks(4);
        return config;
    }

    private byte[] blockBytes(byte marker) {
        byte[] data = new byte[64];
        data[0] = marker;
        return data;
    }

    @Test
    void acknowledgedWriteSurvivesRestart(@TempDir Path directory) throws Exception {
        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config())) {
            scheduler.submitWrite("app", Priority.NORMAL, 0,
                    ByteBuffer.wrap(blockBytes((byte) 7))).get(5,
                    java.util.concurrent.TimeUnit.SECONDS);
        }

        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config())) {
            ByteBuffer dst = ByteBuffer.allocate(64);
            scheduler.submitRead("app", Priority.NORMAL, 0, dst)
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertArrayEquals(blockBytes((byte) 7), dst.array());
        }
    }

    @Test
    void tornTailAfterHardCrashIsDiscardedWithoutLosingCommittedData(
            @TempDir Path directory) throws Exception {
        SchedulerConfig config = config();
        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            scheduler.submitWrite("app", Priority.NORMAL, 0,
                    ByteBuffer.wrap(blockBytes((byte) 11))).get(5,
                    java.util.concurrent.TimeUnit.SECONDS);
        }

        byte[] unfinished = new byte[64];
        unfinished[0] = 99;
        try (FileDurableChannel channel =
                     FileDurableChannel.open(directory.resolve("wal.log"))) {
            WriteAheadLog wal = new WriteAheadLog(channel, 64);
            wal.appendDataAndForce(new WriteBatch(123L,
                    java.util.List.of(new WriteBatch.Entry(5L,
                            ByteBuffer.wrap(unfinished)))));
            // No COMMIT record: the process "crashed" after the data was forced.
        }

        try (DefaultAsyncIoScheduler scheduler =
                     new DefaultAsyncIoScheduler(directory, config)) {
            ByteBuffer dst = ByteBuffer.allocate(64);
            scheduler.submitRead("app", Priority.NORMAL, 0, dst)
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertArrayEquals(blockBytes((byte) 11), dst.array());

            assertEquals(1, scheduler.metricsSnapshot().getDiscardedBatches());
            ByteBuffer uncommitted = ByteBuffer.allocate(64);
            scheduler.submitRead("app", Priority.NORMAL, 5L * 64, uncommitted)
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertArrayEquals(new byte[64], uncommitted.array());
        }
    }
}
