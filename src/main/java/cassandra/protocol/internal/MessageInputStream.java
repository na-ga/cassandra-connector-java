package cassandra.protocol.internal;

import cassandra.cql.type.CQL3Type;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import static cassandra.cql.type.CQL3Type.*;

public class MessageInputStream {

    public static final ByteBuffer[] EMPTY_VALUE_ARRAY = new ByteBuffer[0];

    private final ByteBuf buffer;

    public MessageInputStream(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public <T extends Message> T readMessage(MessageParser<T> parser) {
        return parser.parseFrom(this);
    }

    public double readDouble() {
        return buffer.readDouble();
    }

    public float readFloat() {
        return buffer.readFloat();
    }

    public long readInt64() {
        return buffer.readLong();
    }

    public int readInt32() {
        return buffer.readInt();
    }

    public long readUInt32() {
        return buffer.readUnsignedInt();
    }

    public int readInt16() {
        return buffer.readShort();
    }

    public int readUInt16() {
        return buffer.readUnsignedShort();
    }

    public int readInt8() {
        return buffer.readByte();
    }

    public int readUInt8() {
        return buffer.readUnsignedByte();
    }

    public boolean readBool() {
        return readInt8() != 0;
    }

    public <T extends Enum<T>> EnumSet<T> readEnumSet8(Class<T> valueType) {
        return readEnumSet(valueType, readInt8());
    }

    public <T extends Enum<T>> EnumSet<T> readEnumSet32(Class<T> valueType) {
        return readEnumSet(valueType, readInt32());
    }

    public <T extends Enum<T>> EnumSet<T> readEnumSet(Class<T> valueType, int flags) {
        EnumSet<T> value = EnumSet.noneOf(valueType);
        T[] values = valueType.getEnumConstants();
        for (int i = 0; i < 8; i++) {
            if ((flags & (1 << i)) != 0) {
                value.add(values[i]);
            }
        }
        return value;
    }

    public <T extends Enum<T>> T readEnum(Class<T> valueType) {
        String value = readString();
        return Enum.valueOf(valueType, value.toUpperCase());
    }

    public CQL3Type readCQLType() {
        Name name = Name.valueOf(readUInt16());
        switch (name) {
            case CUSTOM:
                return customType(readString());
            case LIST:
                return listType(readCQLType());
            case SET:
                return setType(readCQLType());
            case MAP:
                return mapType(readCQLType(), readCQLType());
            default:
                return primitiveType(name);
        }
    }

    public UUID readUUID() {
        return new UUID(readInt64(), readInt64());
    }

    public InetSocketAddress readInet() {
        int size = readInt8();
        byte[] address = readRawBytes(size);
        int port = readInt32();
        try {
            return new InetSocketAddress(InetAddress.getByAddress(address), port);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(String.format("Invalid IP address (%d.%d.%d.%d) while deserializing inet address", address[0], address[1], address[2], address[3]));
        }
    }

    public Map<String, List<String>> readStringToStringListMap() {
        int size = readUInt16();
        Map<String, List<String>> value = new HashMap<String, List<String>>(size);
        for (int i = 0; i < size; i++) {
            value.put(readString(), readStringList());
        }
        return value;
    }

    public Map<String, String> readStringMap() {
        int size = readUInt16();
        Map<String, String> value = new HashMap<String, String>(size);
        for (int i = 0; i < size; i++) {
            value.put(readString(), readString());
        }
        return value;
    }

    public String readLongString() {
        int size = readInt32();
        return readString(size);
    }

    public List<String> readStringList() {
        int size = readUInt16();
        List<String> value = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            value.add(readString());
        }
        return value;
    }

    public String readString() {
        int size = readUInt16();
        return readString(size);
    }

    public String readString(int size) {
        String value = buffer.toString(buffer.readerIndex(), size, CharsetUtil.UTF_8);
        buffer.readerIndex(buffer.readerIndex() + size);
        return value;
    }

    public List<ByteBuffer> readValueList() {
        int size = readUInt16();
        if (size == 0) {
            return Collections.emptyList();
        }
        List<ByteBuffer> value = new ArrayList<ByteBuffer>(size);
        for (int i = 0; i < size; i++) {
            value.add(readValue());
        }
        return value;
    }

    public ByteBuffer[] readValueArray() {
        int size = readUInt16();
        if (size == 0) {
            return EMPTY_VALUE_ARRAY;
        }
        ByteBuffer[] value = new ByteBuffer[size];
        for (int i = 0; i < size; i++) {
            value[i] = readValue();
        }
        return value;
    }

    public ByteBuffer readValue() {
        int size = readInt32();
        if (size < 0) {
            return null;
        }
        byte[] bytes = readRawBytes(size);
        return ByteBuffer.wrap(bytes);
    }

    public byte[] readBytes() {
        int size = readInt16();
        return readRawBytes(size);
    }

    public byte[] readRawBytes(int size) {
        byte[] bytes = new byte[size];
        buffer.readBytes(bytes);
        return bytes;
    }

    public ByteBuf buffer() {
        return buffer;
    }

    public boolean close() {
        return buffer != null && buffer.refCnt() > 0 && buffer.release();
    }
}
