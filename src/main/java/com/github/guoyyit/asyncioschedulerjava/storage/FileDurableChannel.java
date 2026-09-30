package com.github.guoyyit.asyncioschedulerjava.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** {@link DurableChannel} backed by a {@link FileChannel} (production storage). */
public final class FileDurableChannel implements DurableChannel {

    private final FileChannel channel;

    private FileDurableChannel(FileChannel channel) {
        this.channel = channel;
    }

    /** Open/creates a read-write channel on {@code path}. */
    public static FileDurableChannel open(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new FileDurableChannel(channel);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position);
    }

    @Override
    public long append(ByteBuffer src) throws IOException {
        long position = channel.size();
        writeAt(src, position);
        return position;
    }

    @Override
    public void writeAt(ByteBuffer src, long position) throws IOException {
        long cursor = position;
        while (src.hasRemaining()) {
            int written = channel.write(src, cursor);
            if (written < 0) {
                throw new IOException("unexpected EOF writing at " + cursor);
            }
            cursor += written;
        }
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public void force() throws IOException {
        channel.force(true);
    }

    @Override
    public void truncate(long size) throws IOException {
        channel.truncate(size);
        channel.force(true);
    }

    /** Visible for tests: the underlying channel. */
    public FileChannel rawChannel() {
        return channel;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
