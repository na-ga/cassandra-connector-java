package cassandra.cql.type;

import cassandra.metadata.Partitioner;
import io.netty.util.CharsetUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.*;

public class CQL3Type { // TODO refactor this

    private static final EnumMap<Name, CQL3Type> primitiveTypeMap;
    private static final Collection<CQL3Type> primitiveTypeSet;
    private static final Map<String, CQL3Type> class2Type;

    static {
        primitiveTypeMap = new EnumMap<Name, CQL3Type>(Name.class);
        registerType(new CQL3Type(Name.ASCII, StringCodec.ASCII_INSTANCE));
        registerType(new CQL3Type(Name.BIGINT, LongCodec.INSTANCE));
        registerType(new CQL3Type(Name.BLOB, BlobCodec.INSTANCE));
        registerType(new CQL3Type(Name.BOOLEAN, BooleanCodec.INSTANCE));
        registerType(new CQL3Type(Name.COUNTER, LongCodec.INSTANCE));
        registerType(new CQL3Type(Name.DECIMAL, DecimalCodec.INSTANCE));
        registerType(new CQL3Type(Name.DOUBLE, DoubleCodec.INSTANCE));
        registerType(new CQL3Type(Name.FLOAT, FloatCodec.INSTANCE));
        registerType(new CQL3Type(Name.INET, InetCodec.INSTANCE));
        registerType(new CQL3Type(Name.INT, IntCodec.INSTANCE));
        registerType(new CQL3Type(Name.TEXT, StringCodec.UTF8_INSTANCE));
        registerType(new CQL3Type(Name.TIMESTAMP, DateCodec.INSTANCE));
        registerType(new CQL3Type(Name.UUID, UUIDCodec.INSTANCE));
        registerType(new CQL3Type(Name.VARCHAR, StringCodec.UTF8_INSTANCE));
        registerType(new CQL3Type(Name.VARINT, BigIntCodec.INSTANCE));
        registerType(new CQL3Type(Name.TIMEUUID, TimeUUIDCodec.INSTANCE));
        primitiveTypeSet = Collections.unmodifiableCollection(primitiveTypeMap.values());

        class2Type = new HashMap<String, CQL3Type>();
        class2Type.put("org.apache.cassandra.db.marshal.AsciiType", CQL3Type.primitiveType(CQL3Type.Name.ASCII));
        class2Type.put("org.apache.cassandra.db.marshal.LongType", CQL3Type.primitiveType(CQL3Type.Name.BIGINT));
        class2Type.put("org.apache.cassandra.db.marshal.BlobType", CQL3Type.primitiveType(CQL3Type.Name.BLOB));
        class2Type.put("org.apache.cassandra.db.marshal.BooleanType", CQL3Type.primitiveType(CQL3Type.Name.BOOLEAN));
        class2Type.put("org.apache.cassandra.db.marshal.CounterType", CQL3Type.primitiveType(CQL3Type.Name.COUNTER));
        class2Type.put("org.apache.cassandra.db.marshal.DecimalType", CQL3Type.primitiveType(CQL3Type.Name.DECIMAL));
        class2Type.put("org.apache.cassandra.db.marshal.DoubleType", CQL3Type.primitiveType(CQL3Type.Name.DOUBLE));
        class2Type.put("org.apache.cassandra.db.marshal.FloatType", CQL3Type.primitiveType(CQL3Type.Name.FLOAT));
        class2Type.put("org.apache.cassandra.db.marshal.InetAddressType", CQL3Type.primitiveType(CQL3Type.Name.INET));
        class2Type.put("org.apache.cassandra.db.marshal.Int32Type", CQL3Type.primitiveType(CQL3Type.Name.INT));
        class2Type.put("org.apache.cassandra.db.marshal.UTF8Type", CQL3Type.primitiveType(CQL3Type.Name.TEXT));
        class2Type.put("org.apache.cassandra.db.marshal.TimestampType", CQL3Type.primitiveType(CQL3Type.Name.TIMESTAMP));
        class2Type.put("org.apache.cassandra.db.marshal.DateType", CQL3Type.primitiveType(CQL3Type.Name.TIMESTAMP));
        class2Type.put("org.apache.cassandra.db.marshal.UUIDType", CQL3Type.primitiveType(CQL3Type.Name.UUID));
        class2Type.put("org.apache.cassandra.db.marshal.IntegerType", CQL3Type.primitiveType(CQL3Type.Name.VARINT));
        class2Type.put("org.apache.cassandra.db.marshal.TimeUUIDType", CQL3Type.primitiveType(CQL3Type.Name.TIMEUUID));
    }

    public static CQL3Type typeFor(Object value) {
        if (value instanceof ByteBuffer) {
            return primitiveTypeMap.get(Name.BLOB);
        }

        if (value instanceof Number) {
            if (value instanceof Integer) {
                return primitiveTypeMap.get(Name.INT);
            }
            if (value instanceof Long) {
                return primitiveTypeMap.get(Name.BIGINT);
            }
            if (value instanceof Float) {
                return primitiveTypeMap.get(Name.FLOAT);
            }
            if (value instanceof Double) {
                return primitiveTypeMap.get(Name.DOUBLE);
            }
            if (value instanceof BigDecimal) {
                return primitiveTypeMap.get(Name.DECIMAL);
            }
            if (value instanceof BigInteger) {
                return primitiveTypeMap.get(Name.DECIMAL);
            }
            return null;
        }

        if (value instanceof String) {
            return primitiveTypeMap.get(Name.TEXT);
        }

        if (value instanceof Boolean) {
            return primitiveTypeMap.get(Name.BOOLEAN);
        }

        if (value instanceof InetAddress) {
            return primitiveTypeMap.get(Name.INET);
        }

        if (value instanceof Date) {
            return primitiveTypeMap.get(Name.TIMESTAMP);
        }

        if (value instanceof UUID) {
            return primitiveTypeMap.get(Name.UUID);
        }

        if (value instanceof List) {
            List<?> list = (List<?>)value;
            if (list.isEmpty()) {
                return listType(primitiveTypeMap.get(Name.BLOB));
            }
            CQL3Type t = typeFor(list.get(0));
            if (t == null) {
                return null;
            } else {
                return listType(t);
            }
        }

        if (value instanceof Set) {
            Set<?> set = (Set<?>)value;
            if (set.isEmpty()) {
                return setType(primitiveTypeMap.get(Name.BLOB));
            }
            CQL3Type t = typeFor(set.iterator().next());
            if (t == null) {
                return null;
            } else {
                return setType(t);
            }
        }

        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>)value;
            if (map.isEmpty()) {
                return mapType(primitiveTypeMap.get(Name.BLOB), primitiveTypeMap.get(Name.BLOB));
            }
            Map.Entry<?, ?> entry = map.entrySet().iterator().next();
            CQL3Type keyType = typeFor(entry.getKey());
            CQL3Type valType = typeFor(entry.getValue());
            if (keyType == null || valType == null) {
                return null;
            } else {
                return mapType(keyType, valType);
            }
        }

        return null;
    }

    public static Collection<CQL3Type> primitiveTypes() {
        return primitiveTypeSet;
    }

    public static CQL3Type primitiveType(Name name) {
        if (name.isCollection() || name == Name.CUSTOM) {
            throw new IllegalArgumentException(String.format("not a primitive type %s", name));
        }
        return primitiveTypeMap.get(name);
    }

    public static CQL3Type listType(CQL3Type elementType) {
        return new CQL3Type(Name.LIST, Arrays.asList(elementType), null, listCodec(elementType));
    }

    public static CQL3Type setType(CQL3Type elementType) {
        return new CQL3Type(Name.SET, Arrays.asList(elementType), null, setCodec(elementType));
    }

    public static CQL3Type mapType(CQL3Type keyType, CQL3Type valueType) {
        return new CQL3Type(Name.MAP, Arrays.asList(keyType, valueType), null, mapCodec(keyType, valueType));
    }

    public static CQL3Type customType(String customClassName) {
        if (customClassName == null) {
            throw new NullPointerException("customClassName");
        }
        return new CQL3Type(Name.CUSTOM, Collections.<CQL3Type>emptyList(), customClassName, primitiveType(Name.BLOB).codec());
    }

    public static <T> ListCodec<T> listCodec(CQL3Type elementType) {
        return new ListCodec<T>(elementType.<T>codec());
    }

    public static <T> SetCodec<T> setCodec(CQL3Type elementType) {
        return new SetCodec<T>(elementType.<T>codec());
    }

    public static <K, V> MapCodec<K, V> mapCodec(CQL3Type keyType, CQL3Type valueType) {
        return new MapCodec<K, V>(keyType.<K>codec(), valueType.<V>codec());
    }

    private static void registerType(CQL3Type type) {
        primitiveTypeMap.put(type.name(), type);
    }

    public static enum Name {
        CUSTOM(0, ByteBuffer.class),
        ASCII(1, String.class),
        BIGINT(2, Long.class),
        BLOB(3, ByteBuffer.class),
        BOOLEAN(4, Boolean.class),
        COUNTER(5, Long.class),
        DECIMAL(6, BigDecimal.class),
        DOUBLE(7, Double.class),
        FLOAT(8, Float.class),
        INET(16, InetAddress.class),
        INT(9, Integer.class),
        TEXT(10, String.class),
        TIMESTAMP(11, Date.class),
        UUID(12, java.util.UUID.class),
        VARCHAR(13, String.class),
        VARINT(14, BigInteger.class),
        TIMEUUID(15, UUID.class),
        LIST(32, List.class),
        SET(34, Set.class),
        MAP(33, Map.class);

        public final int id;
        public final Class<?> javaType;

        private Name(int id, Class<?> javaType) {
            this.id = id;
            this.javaType = javaType;
        }

        public boolean isCollection() {
            switch (this) {
                case LIST:
                case SET:
                case MAP:
                    return true;
                default:
                    return false;
            }
        }

        public static Name valueOf(int id) {
            for (Name name : Name.values()) {
                if (name.id == id) {
                    return name;
                }
            }
            throw new IllegalStateException(String.format("unknown cql type id %d", id));
        }
    }

    public static interface TypeCodec<T> {

        ByteBuffer encode(T value);

        T decode(ByteBuffer buffer);
    }

    private final Name name;
    private final List<CQL3Type> typeArguments;
    private final String customClassName;
    private final TypeCodec<?> codec;

    public CQL3Type(Name name, TypeCodec<?> codec) {
        this(name, Collections.<CQL3Type>emptyList(), null, codec);
    }

    public CQL3Type(Name name, List<CQL3Type> typeArguments, String customClassName, TypeCodec<?> codec) {
        this.name = name;
        this.typeArguments = Collections.unmodifiableList(typeArguments);
        this.customClassName = customClassName;
        this.codec = codec;
    }

    public Name name() {
        return name;
    }

    public List<CQL3Type> typeArguments() {
        return typeArguments;
    }

    public String customClassName() {
        return customClassName;
    }

    public ByteBuffer serialize(Object value) {
        if (!name.javaType.isAssignableFrom(value.getClass())) {
            throw new CQL3TypeError(String.format("invalid value for CQL type %s, expecting %s but %s provided", name, name.javaType, value.getClass()));
        }
        try {
            return codec().encode(value);
        } catch (Exception e) {
            throw new CQL3TypeError(e);
        }
    }

    public Object deserialize(ByteBuffer buffer) {
        try {
            return codec.decode(buffer);
        } catch (Exception e) {
            throw new CQL3TypeError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> codec() {
        return (TypeCodec<T>)codec;
    }

    public static class StringCodec implements TypeCodec<String> {

        public static StringCodec ASCII_INSTANCE = new StringCodec(CharsetUtil.US_ASCII);
        public static StringCodec UTF8_INSTANCE = new StringCodec(CharsetUtil.UTF_8);

        private Charset charset;

        public StringCodec(Charset charset) {
            this.charset = charset;
        }

        @Override
        public ByteBuffer encode(String value) {
            try {
                return CharsetUtil.getEncoder(charset).encode(CharBuffer.wrap(value));
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public String decode(ByteBuffer buffer) {
            try {
                return CharsetUtil.getDecoder(charset).decode(buffer.duplicate()).toString();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public static class LongCodec implements TypeCodec<Long> {

        public static final LongCodec INSTANCE = new LongCodec();

        @Override
        public ByteBuffer encode(Long value) {
            return ByteBuffer.allocate(8).putLong(0, value);
        }

        @Override
        public Long decode(ByteBuffer buffer) {
            return buffer.getLong(buffer.position());
        }
    }

    public static class BlobCodec implements TypeCodec<ByteBuffer> {

        public static final BlobCodec INSTANCE = new BlobCodec();

        @Override
        public ByteBuffer encode(ByteBuffer value) {
            return value.duplicate();
        }

        @Override
        public ByteBuffer decode(ByteBuffer buffer) {
            return buffer.duplicate();
        }
    }

    public static class BooleanCodec implements TypeCodec<Boolean> {

        public static final BooleanCodec INSTANCE = new BooleanCodec();

        public static final byte[] TRUE = new byte[]{1};
        public static final byte[] FALSE = new byte[]{0};

        @Override
        public ByteBuffer encode(Boolean value) {
            if (value) {
                return ByteBuffer.wrap(TRUE);
            } else {
                return ByteBuffer.wrap(FALSE);
            }
        }

        @Override
        public Boolean decode(ByteBuffer buffer) {
            return buffer.get(buffer.position()) != 0;
        }
    }

    public static class DecimalCodec implements TypeCodec<BigDecimal> {

        public static final DecimalCodec INSTANCE = new DecimalCodec();

        @Override
        public ByteBuffer encode(BigDecimal value) {
            BigInteger bi = value.unscaledValue();
            int scale = value.scale();
            byte[] array = bi.toByteArray();
            ByteBuffer buf = ByteBuffer.allocate(4 + array.length).putInt(scale).put(array);
            buf.rewind();
            return buf;
        }

        @Override
        public BigDecimal decode(ByteBuffer buffer) {
            ByteBuffer dup = buffer.duplicate();
            int scale = dup.getInt();
            byte[] array = new byte[dup.remaining()];
            dup.get(array);
            BigInteger bi = new BigInteger(array);
            return new BigDecimal(bi, scale);
        }
    }

    public static class DoubleCodec implements TypeCodec<Double> {

        public static final DoubleCodec INSTANCE = new DoubleCodec();

        @Override
        public ByteBuffer encode(Double value) {
            return ByteBuffer.allocate(8).putDouble(0, value);
        }

        @Override
        public Double decode(ByteBuffer buffer) {
            return buffer.getDouble(buffer.position());
        }
    }

    public static class FloatCodec implements TypeCodec<Float> {

        public static final FloatCodec INSTANCE = new FloatCodec();

        @Override
        public ByteBuffer encode(Float value) {
            return ByteBuffer.allocate(4).putFloat(0, value);
        }

        @Override
        public Float decode(ByteBuffer buffer) {
            return buffer.getFloat(buffer.position());
        }
    }

    public static class InetCodec implements TypeCodec<InetAddress> {

        public static final InetCodec INSTANCE = new InetCodec();

        @Override
        public ByteBuffer encode(InetAddress value) {
            return ByteBuffer.wrap(value.getAddress());
        }

        @Override
        public InetAddress decode(ByteBuffer buffer) {
            try {
                return InetAddress.getByAddress(getArray(buffer));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public static class IntCodec implements TypeCodec<Integer> {

        public static final IntCodec INSTANCE = new IntCodec();

        @Override
        public ByteBuffer encode(Integer value) {
            return ByteBuffer.allocate(4).putInt(0, value);
        }

        @Override
        public Integer decode(ByteBuffer buffer) {
            return buffer.getInt(buffer.position());
        }
    }

    public static class DateCodec implements TypeCodec<Date> {

        public static final DateCodec INSTANCE = new DateCodec();

        @Override
        public ByteBuffer encode(Date value) {
            return LongCodec.INSTANCE.encode(value.getTime());
        }

        @Override
        public Date decode(ByteBuffer buffer) {
            return new Date(LongCodec.INSTANCE.decode(buffer));
        }
    }

    public static class UUIDCodec implements TypeCodec<UUID> {

        public static final UUIDCodec INSTANCE = new UUIDCodec();

        @Override
        public ByteBuffer encode(UUID value) {
            return ByteBuffer.allocate(16)
                    .putLong(0, value.getMostSignificantBits())
                    .putLong(8, value.getLeastSignificantBits());
        }

        @Override
        public UUID decode(ByteBuffer buffer) {
            return new UUID(buffer.getLong(buffer.position()), buffer.getLong(buffer.position() + 8));
        }
    }

    public static class TimeUUIDCodec extends UUIDCodec {

        public static final TimeUUIDCodec INSTANCE = new TimeUUIDCodec();

        @Override
        public UUID decode(ByteBuffer buffer) {
            UUID uuid = super.decode(buffer);
            if (uuid.version() != 1) {
                throw new IllegalArgumentException(String.format("invalid uuid version %d (expected: version == 1)", uuid.version()));
            }
            return uuid;
        }
    }

    public static class BigIntCodec implements TypeCodec<BigInteger> {

        public static final BigIntCodec INSTANCE = new BigIntCodec();

        @Override
        public ByteBuffer encode(BigInteger value) {
            return ByteBuffer.wrap(value.toByteArray());
        }

        @Override
        public BigInteger decode(ByteBuffer buffer) {
            return new BigInteger(getArray(buffer));
        }
    }

    public static class ListCodec<T> implements TypeCodec<List<T>> {

        public final TypeCodec<T> elementCodec;

        public ListCodec(TypeCodec<T> elementCodec) {
            this.elementCodec = elementCodec;
        }

        @Override
        public ByteBuffer encode(List<T> value) {
            if (value.isEmpty()) {
                ByteBuffer buffer = ByteBuffer.allocate(2).putShort((short)0);
                buffer.flip();
                return buffer;
            }
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(value.size());
            int size = 0;
            for (T e : value) {
                ByteBuffer buffer = elementCodec.encode(e);
                buffers.add(buffer);
                size += 2 + buffer.remaining();
            }
            return pack(buffers, value.size(), size);
        }

        @Override
        public List<T> decode(ByteBuffer buffer) {
            ByteBuffer dup = buffer.duplicate();
            int elements = getUnsignedShort(dup);
            List<T> list = new ArrayList<T>(elements);
            for (int i = 0; i < elements; i++) {
                int datalen = getUnsignedShort(dup);
                byte[] data = new byte[datalen];
                dup.get(data);
                ByteBuffer databuffer = ByteBuffer.wrap(data);
                list.add(elementCodec.decode(databuffer));
            }
            return list;
        }
    }

    public static class SetCodec<T> implements TypeCodec<Set<T>> {

        public final TypeCodec<T> elementCodec;

        public SetCodec(TypeCodec<T> elementCodec) {
            this.elementCodec = elementCodec;
        }

        @Override
        public ByteBuffer encode(Set<T> value) {
            if (value.isEmpty()) {
                ByteBuffer buffer = ByteBuffer.allocate(2).putShort((short)0);
                buffer.flip();
                return buffer;
            }
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(value.size());
            int size = 0;
            for (T e : value) {
                ByteBuffer buffer = elementCodec.encode(e);
                buffers.add(buffer);
                size += 2 + buffer.remaining();
            }
            return pack(buffers, value.size(), size);
        }

        @Override
        public Set<T> decode(ByteBuffer buffer) {
            ByteBuffer dup = buffer.duplicate();
            int elements = getUnsignedShort(dup);
            Set<T> set = new LinkedHashSet<T>(elements);
            for (int i = 0; i < elements; i++) {
                int datalen = getUnsignedShort(dup);
                byte[] data = new byte[datalen];
                dup.get(data);
                ByteBuffer databuffer = ByteBuffer.wrap(data);
                set.add(elementCodec.decode(databuffer));
            }
            return set;
        }
    }

    public static class MapCodec<K, V> implements TypeCodec<Map<K, V>> {

        private final TypeCodec<K> keyCodec;
        private final TypeCodec<V> valueCodec;

        public MapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec) {
            this.keyCodec = keyCodec;
            this.valueCodec = valueCodec;
        }

        @Override
        public ByteBuffer encode(Map<K, V> value) {
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(2 * value.size());
            int size = 0;
            for (Map.Entry<K, V> entry : value.entrySet()) {
                ByteBuffer keybuf = keyCodec.encode(entry.getKey());
                ByteBuffer valbuf = valueCodec.encode(entry.getValue());
                buffers.add(keybuf);
                buffers.add(valbuf);
                size += 4 + keybuf.remaining() + valbuf.remaining();
            }
            return pack(buffers, value.size(), size);
        }

        @Override
        public Map<K, V> decode(ByteBuffer buffer) {
            ByteBuffer dup = buffer.duplicate();
            int size = getUnsignedShort(dup);
            Map<K, V> map = new LinkedHashMap<K, V>(size);
            for (int i = 0; i < size; i++) {
                int keylen = getUnsignedShort(dup);
                byte[] key = new byte[keylen];
                dup.get(key);
                ByteBuffer keybuf = ByteBuffer.wrap(key);

                int vallen = getUnsignedShort(dup);
                byte[] val = new byte[vallen];
                dup.get(val);
                ByteBuffer valbuf = ByteBuffer.wrap(val);

                map.put(keyCodec.decode(keybuf), valueCodec.decode(valbuf));
            }
            return map;
        }
    }

    private static byte[] getArray(ByteBuffer buffer) {
        int length = buffer.remaining();
        if (buffer.hasArray()) {
            int offset = buffer.arrayOffset() + buffer.position();
            if (offset == 0 && length == buffer.array().length) {
                return buffer.array();
            } else {
                return Arrays.copyOfRange(buffer.array(), offset, offset + length);
            }
        }
        byte[] array = new byte[length];
        buffer.duplicate().get(array);
        return array;
    }

    private static ByteBuffer pack(List<ByteBuffer> buffers, int elements, int size) {
        ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short)elements);
        for (ByteBuffer bb : buffers) {
            result.putShort((short)bb.remaining());
            result.put(bb.duplicate());
        }
        return (ByteBuffer)result.flip();
    }

    private static int getUnsignedShort(ByteBuffer buffer) {
        int length = (buffer.get() & 0xFF) << 8;
        return length | (buffer.get() & 0xFF);
    }

    public static class TypeParser {

        public static TypeParser parse(String className) {
            return new TypeParser(className, 0).parse();
        }

        private final String str;
        private int idx;
        private List<CQL3Type> types;
        private Map<String, CQL3Type> collections;

        private TypeParser(String str, int idx) {
            this.str = str;
            this.idx = idx;
        }

        public CQL3Type type() {
            if (types == null) {
                return null;
            }
            return types.get(0);
        }

        public List<CQL3Type> types() {
            if (types == null) {
                return Collections.emptyList();
            }
            return types;
        }

        public Map<String, CQL3Type> collections() {
            if (collections == null) {
                return Collections.emptyMap();
            }
            return collections;
        }

        public boolean isComposite() {
            return collections != null && !collections.isEmpty();
        }

        private TypeParser parse() {
            String name = parseName();
            if (name.startsWith("org.apache.cassandra.db.marshal.CompositeType")) {
                types = parseTypeParameters();
            } else {
                types = Collections.singletonList(parseType(name));
            }
            return this;
        }

        private CQL3Type parseType(String name) {
            if (name.startsWith("org.apache.cassandra.db.marshal.ReversedType")) {
                return parseTypeParameters().get(0);
            } else if (name.startsWith("org.apache.cassandra.db.marshal.ListType")) {
                List<CQL3Type> params = parseTypeParameters();
                return CQL3Type.listType(params.get(0));
            } else if (name.startsWith("org.apache.cassandra.db.marshal.SetType")) {
                List<CQL3Type> params = parseTypeParameters();
                return CQL3Type.setType(params.get(0));
            } else if (name.startsWith("org.apache.cassandra.db.marshal.MapType")) {
                List<CQL3Type> params = parseTypeParameters();
                return CQL3Type.mapType(params.get(0), params.get(1));
            }
            CQL3Type type = class2Type.get(name);
            if (type != null) {
                return type;
            }
            return CQL3Type.customType(name);
        }

        private String parseName() {
            skipBlank();
            return readNextIdentifier();
        }

        private List<CQL3Type> parseTypeParameters() {
            List<CQL3Type> list = new ArrayList<CQL3Type>();
            if (isEOS(str, idx)) {
                return list;
            }
            if (str.charAt(idx) != '(') {
                throw new IllegalStateException();
            }
            ++idx;
            while (skipBlankAndComma()) {
                if (str.charAt(idx) == ')') {
                    ++idx;
                    return list;
                }
                try {
                    String name = parseName();
                    if (name.startsWith("org.apache.cassandra.db.marshal.ColumnToCollectionType")) {
                        if (collections == null) {
                            collections = new HashMap<String, CQL3Type>();
                        }
                        collections.putAll(parseCollectionsParameters());
                        return list;
                    }
                    list.add(parseType(name));
                } catch (Exception e) {
                    throw new CQL3TypeError(String.format("Exception while parsing '%s' around char %d", str, idx));
                }
            }
            throw new CQL3TypeError(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
        }

        private Map<String, CQL3Type> parseCollectionsParameters() {
            Map<String, CQL3Type> map = new HashMap<String, CQL3Type>();
            if (isEOS(str, idx)) {
                return map;
            }
            if (str.charAt(idx) != '(') {
                throw new IllegalStateException();
            }
            ++idx;
            while (skipBlankAndComma()) {
                if (str.charAt(idx) == ')') {
                    ++idx;
                    return map;
                }
                String hex = readNextIdentifier();
                String name;
                try {
                    name = StringCodec.UTF8_INSTANCE.decode(ByteBuffer.wrap(Partitioner.Hex.hexToBytes(hex)));
                } catch (NumberFormatException e) {
                    throw new CQL3TypeError(e.getMessage());
                }
                skipBlank();
                if (str.charAt(idx) != ':') {
                    throw new CQL3TypeError("expecting ':' token");
                }
                ++idx;
                skipBlank();
                try {
                    map.put(name, parseType(parseName()));
                } catch (Exception e) {
                    throw new CQL3TypeError(String.format("Exception while parsing '%s' around char %d", str, idx));
                }
            }
            throw new CQL3TypeError(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
        }

        private String readNextIdentifier() {
            int i = idx;
            while (!isEOS(str, idx) && isIdentifierChar(str.charAt(idx))) {
                ++idx;
            }

            return str.substring(i, idx);
        }

        private char readNextChar() {
            skipBlank();
            return str.charAt(idx++);
        }

        private void skipBlank() {
            while (!isEOS(str, idx) && isBlank(str.charAt(idx))) {
                ++idx;
            }
        }

        private boolean skipBlankAndComma() {
            boolean commaFound = false;
            while (!isEOS(str, idx)) {
                int c = str.charAt(idx);
                if (c == ',') {
                    if (commaFound) {
                        return true;
                    } else {
                        commaFound = true;
                    }
                } else if (!isBlank(c)) {
                    return true;
                }
                ++idx;
            }
            return false;
        }

        private static boolean isIdentifierChar(int c) {
            return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '-' || c == '+' || c == '.' || c == '_' || c == '&';
        }

        private static boolean isBlank(int c) {
            return c == ' ' || c == '\t' || c == '\n';
        }

        private static boolean isEOS(String str, int i) {
            return i >= str.length();
        }
    }
}
