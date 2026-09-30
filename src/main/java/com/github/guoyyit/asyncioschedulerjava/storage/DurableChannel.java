package com.github.guoyyit.asyncioschedulerjava.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Random-access byte channel with explicit durability and truncation.
 * {@link #force()} must flush data (and metadata on the target file system) to
 * stable storage; implementations are the single point that defines the
 * durability boundary used by the write-ahead log.
 */
public interface DurableChannel extends Closeable {

    int read(ByteBuffer dst, long position) throws IOException;

    /** Appends at the current end and returns the start position of the write. */
    long append(ByteBuffer src) throws IOException;

    void writeAt(ByteBuffer src, long position) throws IOException;

    long size() throws IOException;

    /** Force all buffered data + metadata to stable storage. */
    void force() throws IOException;

    /** Truncate to exactly {@code size} bytes and force the new length. */
    void truncate(long size) throws IOException;

    @Override
    void close() throws IOException;
}
