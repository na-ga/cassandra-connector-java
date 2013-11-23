package cassandra.cql;

import cassandra.metadata.Partitioner;

import java.nio.ByteBuffer;

public class PagingState {

    public static PagingState copyFrom(ByteBuffer bytes) {
        return new PagingState(bytes.duplicate());
    }

    public static PagingState copyFrom(String hex) {
        return new PagingState(ByteBuffer.wrap(Partitioner.Hex.hexToBytes(hex)).asReadOnlyBuffer());
    }

    private final ByteBuffer bytes;

    PagingState(ByteBuffer bytes) {
        if (bytes == null) {
            throw new NullPointerException("bytes");
        }
        if (bytes.remaining() == 0) {
            throw new IllegalArgumentException("empty bytes");
        }
        if (!bytes.isReadOnly()) {
            this.bytes = bytes.asReadOnlyBuffer();
        } else {
            this.bytes = bytes;
        }
    }

    public String toHexString() {
        return Partitioner.Hex.bytesToHex(bytes);
    }

    public ByteBuffer asByteBuffer() {
        return bytes;
    }
}
