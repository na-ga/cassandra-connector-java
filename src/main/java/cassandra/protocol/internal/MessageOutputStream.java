package cassandra.protocol.internal;

import cassandra.cql.type.CQL3Type;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MessageOutputStream {

    private final ByteBuf buffer;

    public MessageOutputStream(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public void writeMessage(Message value) {
        writeInt32(value.getApproximateSize());
        value.writeTo(this);
    }

    public void writeDouble(double value) {
        buffer.writeDouble(value);
    }

    public void writeFloat(float value) {
        buffer.writeFloat(value);
    }

    public void writeInt64(long value) {
        buffer.writeLong(value);
    }

    public void writeInt32(int value) {
        buffer.writeInt(value);
    }

    public void writeInt16(int value) {
        buffer.writeShort(value);
    }

    public void writeInt8(int value) {
        buffer.writeByte(value);
    }

    public void writeBool(boolean value) {
        if (value) {
            writeInt8(1);
        } else {
            writeInt8(0);
        }
    }

    public <T extends Enum<T>> void writeEnumSet8(EnumSet<T> value) {
        int flags = 0;
        for (T v : value) {
            flags |= 1 << v.ordinal();
        }
        writeInt8(flags);
    }

    public <T extends Enum<T>> void writeEnumSet32(EnumSet<T> value) {
        int flags = 0;
        for (T v : value) {
            flags |= 1 << v.ordinal();
        }
        writeInt32(flags);
    }

    public <T extends Enum<T>> void writeEnum(T value) {
        writeString(value.toString());
    }

    public void writeCQLType(CQL3Type value) {
        // TODO
    }

    public void writeUUID(UUID value) {
        writeInt64(value.getMostSignificantBits());
        writeInt64(value.getLeastSignificantBits());
    }

    public void writeInet(InetSocketAddress value) {
        byte[] bytes = value.getAddress().getAddress();
        writeInt8(bytes.length);
        writeRawBytes(bytes);
        writeInt32(value.getPort());
    }

    public void writeStringToStringListMap(Map<String, List<String>> value) {
        writeInt16(value.size());
        for (Map.Entry<String, List<String>> entry : value.entrySet()) {
            writeString(entry.getKey());
            writeStringList(entry.getValue());
        }
    }

    public void writeStringMap(Map<String, String> value) {
        writeInt16(value.size());
        for (Map.Entry<String, String> entry : value.entrySet()) {
            writeString(entry.getKey());
            writeString(entry.getValue());
        }
    }

    public void writeLongString(String value) {
        byte[] bytes = value.getBytes(CharsetUtil.UTF_8);
        writeInt32(bytes.length);
        writeRawBytes(bytes);
    }

    public void writeStringList(List<String> value) {
        writeInt16(value.size());
        for (String v : value) {
            writeString(v);
        }
    }

    public void writeString(String value) {
        byte[] bytes = value.getBytes(CharsetUtil.UTF_8);
        writeInt16(bytes.length);
        writeRawBytes(bytes);
    }

    public void writeValueList(List<ByteBuffer> value) {
        writeInt16(value.size());
        for (ByteBuffer v : value) {
            writeValue(v);
        }
    }

    public void writeValueArray(ByteBuffer[] value) {
        writeInt16(value.length);
        for (ByteBuffer v : value) {
            writeValue(v);
        }
    }

    public void writeValue(ByteBuffer value) {
        if (value == null) {
            writeInt32(-1);
            return;
        }
        writeInt32(value.remaining());
        writeRawBytes(value.duplicate());
    }

    public void writeValue(byte[] value) {
        if (value == null) {
            writeInt32(-1);
            return;
        }
        writeInt32(value.length);
        writeRawBytes(value);
    }

    public void writeBytes(ByteBuffer value) {
        writeInt16(value.remaining());
        writeRawBytes(value);
    }

    public void writeBytes(byte[] value) {
        writeInt16(value.length);
        writeRawBytes(value);
    }

    public void writeRawBytes(ByteBuffer value) {
        buffer.writeBytes(value);
    }

    public void writeRawBytes(byte[] value) {
        buffer.writeBytes(value);
    }

    public ByteBuf buffer() {
        return buffer;
    }

    public boolean close() {
        return buffer != null && buffer.refCnt() > 0 && buffer.release();
    }

    public static <T extends Enum<T>> int computeEnumSize(T value) {
        return computeStringSize(value.toString());
    }

    public static int computeCQLTypeSize(CQL3Type value) {
        int size = 2;
        switch (value.name()) {
            case CUSTOM:
                return size + computeStringSize(value.customClassName());
            case LIST:
            case SET:
                return size + computeCQLTypeSize(value.typeArguments().get(0));
            case MAP:
                size += computeCQLTypeSize(value.typeArguments().get(0));
                size += computeCQLTypeSize(value.typeArguments().get(1));
                return size;
            default:
                return size;
        }
    }

    public static int computeInetSize(InetSocketAddress value) {
        byte[] bytes = value.getAddress().getAddress();
        int size = 1;
        size += bytes.length;
        size += 4;
        return size;
    }

    public static int computeStringToStringListMapSize(Map<String, List<String>> value) {
        int size = 2;
        for (Map.Entry<String, List<String>> entry : value.entrySet()) {
            size += computeStringSize(entry.getKey());
            size += computeStringListSize(entry.getValue());
        }
        return size;
    }

    public static int computeStringMapSize(Map<String, String> value) {
        int size = 2;
        for (Map.Entry<String, String> entry : value.entrySet()) {
            size += computeStringSize(entry.getKey());
            size += computeStringSize(entry.getValue());
        }
        return size;
    }

    public static int computeLongStringSize(String value) {
        byte[] bytes = value.getBytes(CharsetUtil.UTF_8);
        return 4 + bytes.length;
    }

    public static int computeStringListSize(List<String> value) {
        int size = 2;
        for (String v : value) {
            size += computeStringSize(v);
        }
        return size;
    }

    public static int computeStringSize(String value) {
        byte[] bytes = value.getBytes(CharsetUtil.UTF_8);
        return 2 + bytes.length;
    }

    public static int computeValueListSize(List<ByteBuffer> value) {
        int size = 2;
        for (ByteBuffer v : value) {
            size += computeValueSize(v);
        }
        return size;
    }

    public static int computeValueArraySize(ByteBuffer[] value) {
        int size = 2;
        for (ByteBuffer v : value) {
            size += computeValueSize(v);
        }
        return size;
    }

    public static int computeValueSize(ByteBuffer value) {
        return 4 + (value == null ? 0 : value.remaining());
    }

    public static int computeValueSize(byte[] value) {
        return 4 + (value == null ? 0 : value.length);
    }

    public static int computeBytesSize(byte[] value) {
        return 2 + (value == null ? 0 : value.length);
    }
}
