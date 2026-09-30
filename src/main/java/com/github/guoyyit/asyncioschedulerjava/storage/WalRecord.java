package com.github.guoyyit.asyncioschedulerjava.storage;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * One framed WAL record.
 *
 * <p>On-disk layout:
 * {@code int magic, int type, long batchId, int payloadLen, byte[] payload, int crc32}
 * where crc32 covers magic..payload. {@link Type#COMMIT} is the phase-2 marker;
 * data records without a following commit record belong to an unfinished batch
 * and are discarded during recovery.
 */
public final class WalRecord {

    public static final int MAGIC = 0x57414C01;

    public enum Type {
        DATA((byte) 1),
        COMMIT((byte) 2);

        private final byte code;

        Type(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static Type fromCode(byte code) {
            for (Type type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("unknown WAL record type: " + code);
        }
    }

    public static final int HEADER_BYTES = 4 + 4 + 8 + 4;
    public static final int CRC_BYTES = 4;

    private final Type type;
    private final long batchId;
    private final ByteBuffer payload;

    public WalRecord(Type type, long batchId, ByteBuffer payload) {
        this.type = type;
        this.batchId = batchId;
        this.payload = payload;
    }

    public Type getType() {
        return type;
    }

    public long getBatchId() {
        return batchId;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    /** Serialise to a new heap {@link ByteBuffer}, positioned at zero. */
    public ByteBuffer encode() {
        int payloadLength = payload == null ? 0 : payload.remaining();
        ByteBuffer out = ByteBuffer.allocate(HEADER_BYTES + payloadLength + CRC_BYTES);
        out.putInt(MAGIC);
        out.putInt(type.code());
        out.putLong(batchId);
        out.putInt(payloadLength);
        if (payloadLength > 0) {
            out.put(payload.duplicate());
        }
        int crc = crc32(out, 0, out.position());
        out.putInt(crc);
        out.flip();
        return out;
    }

    /** CRC32 over {@code magic..payload} of the record being encoded. */
    static int crc32(ByteBuffer buffer, int offset, int length) {
        CRC32 crc = new CRC32();
        int oldPosition = buffer.position();
        int oldLimit = buffer.limit();
        buffer.position(offset);
        buffer.limit(offset + length);
        crc.update(buffer);
        buffer.limit(oldLimit);
        buffer.position(oldPosition);
        return (int) crc.getValue();
    }
}
