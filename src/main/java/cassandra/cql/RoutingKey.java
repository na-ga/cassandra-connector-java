package cassandra.cql;

import cassandra.metadata.Partitioner;

import java.nio.ByteBuffer;

public class RoutingKey {

    public static RoutingKey copyFrom(ByteBuffer... routingKey) {
        if (routingKey == null) {
            throw new NullPointerException("routingKey");
        }
        if (routingKey.length == 0) {
            throw new IllegalArgumentException("empty routingKey");
        }
        ByteBuffer bytes;
        if (routingKey.length == 1) {
            bytes = routingKey[0];
        } else {
            int size = 0;
            for (ByteBuffer key : routingKey) {
                size += 2 + key.remaining() + 1;
            }
            bytes = ByteBuffer.allocate(size);
            for (ByteBuffer key : routingKey) {
                ByteBuffer copy = key.duplicate();
                int len = copy.remaining();
                bytes.put((byte)((len >> 8) & 0xFF));
                bytes.put((byte)(len & 0xFF));
                bytes.put(copy);
                bytes.put((byte)0);
            }
            bytes.flip();
        }
        return new RoutingKey(bytes.asReadOnlyBuffer());
    }

    public static RoutingKey copyFrom(String hex) {
        return new RoutingKey(ByteBuffer.wrap(Partitioner.Hex.hexToBytes(hex)).asReadOnlyBuffer());
    }

    private final ByteBuffer bytes;

    RoutingKey(ByteBuffer bytes) {
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

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o != null && o instanceof RoutingKey && bytes.equals(((RoutingKey)o).bytes);
    }
}
