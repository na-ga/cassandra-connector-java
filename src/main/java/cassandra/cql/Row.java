package cassandra.cql;

import cassandra.cql.type.CQL3Type;
import cassandra.cql.type.CQL3TypeError;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class Row {

    private final RowMetadata metadata;
    private final List<ByteBuffer> row;

    public Row(RowMetadata metadata, List<ByteBuffer> row) {
        this.metadata = metadata;
        this.row = row;
    }

    public RowMetadata getMetadata() {
        return metadata;
    }

    public boolean isApplied() {
        return isNull("[applied]") || getBool("[applied]");
    }

    public boolean isNull(String column) {
        return isNull(metadata.getColumnIndex(column));
    }

    public boolean isNull(int column) {
        return metadata.getColumn(column) == null || row.get(column) == null;
    }

    public boolean getBool(String column) {
        return getBool(metadata.getColumnIndex(column));
    }

    public boolean getBool(int column) {
        return getBool(column, false);
    }

    public boolean getBool(String column, boolean defaultValue) {
        return getBool(metadata.getColumnIndex(column), defaultValue);
    }

    public boolean getBool(int column, boolean defaultValue) {
        return getValue(column, CQL3Type.Name.BOOLEAN, defaultValue);
    }

    public ByteBuffer getBlob(String column) {
        return getBlob(metadata.getColumnIndex(column));
    }

    public ByteBuffer getBlob(int column) {
        return getBlob(column, null);
    }

    public ByteBuffer getBlob(String column, ByteBuffer defaultValue) {
        return getBlob(metadata.getColumnIndex(column), defaultValue);
    }

    public ByteBuffer getBlob(int column, ByteBuffer defaultValue) {
        return getValue(column, CQL3Type.Name.BLOB, defaultValue);
    }

    public Date getDate(String column) {
        return getDate(metadata.getColumnIndex(column));
    }

    public Date getDate(int column) {
        return getDate(column, null);
    }

    public Date getDate(String column, Date defaultValue) {
        return getDate(metadata.getColumnIndex(column), defaultValue);
    }

    public Date getDate(int column, Date defaultValue) {
        return getValue(column, CQL3Type.Name.TIMESTAMP, defaultValue);
    }

    public BigDecimal getDecimal(String column) {
        return getDecimal(metadata.getColumnIndex(column));
    }

    public BigDecimal getDecimal(int column) {
        return getDecimal(column, null);
    }

    public BigDecimal getDecimal(String column, BigDecimal defaultValue) {
        return getDecimal(metadata.getColumnIndex(column), defaultValue);
    }

    public BigDecimal getDecimal(int column, BigDecimal defaultValue) {
        return getValue(column, CQL3Type.Name.DECIMAL, defaultValue);
    }

    public double getDouble(String column) {
        return getDouble(metadata.getColumnIndex(column));
    }

    public double getDouble(int column) {
        return getDouble(column, 0.0D);
    }

    public double getDouble(String column, double defaultValue) {
        return getDouble(metadata.getColumnIndex(column), defaultValue);
    }

    public double getDouble(int column, double defaultValue) {
        return getValue(column, CQL3Type.Name.DOUBLE, defaultValue);
    }

    public float getFloat(String column) {
        return getFloat(metadata.getColumnIndex(column));
    }

    public float getFloat(int column) {
        return getFloat(column, 0.0F);
    }

    public float getFloat(String column, float defaultValue) {
        return getFloat(metadata.getColumnIndex(column), defaultValue);
    }

    public float getFloat(int column, float defaultValue) {
        return getValue(column, CQL3Type.Name.FLOAT, defaultValue);
    }

    public int getInt(String column) {
        return getInt(metadata.getColumnIndex(column));
    }

    public int getInt(int column) {
        return getInt(column, 0);
    }

    public int getInt(String column, int defaultValue) {
        return getInt(metadata.getColumnIndex(column), defaultValue);
    }

    public int getInt(int column, int defaultValue) {
        return getValue(column, CQL3Type.Name.INT, defaultValue);
    }

    public long getLong(String column) {
        return getLong(metadata.getColumnIndex(column));
    }

    public long getLong(int column) {
        return getLong(column, 0L);
    }

    public long getLong(String column, long defaultValue) {
        return getLong(metadata.getColumnIndex(column), defaultValue);
    }

    public long getLong(int column, long defaultValue) {
        return getValue(column, CQL3Type.Name.BIGINT, CQL3Type.Name.COUNTER, defaultValue);
    }

    public String getString(String column) {
        return getString(metadata.getColumnIndex(column));
    }

    public String getString(int column) {
        return getString(column, null);
    }

    public String getString(String column, String defaultValue) {
        return getString(metadata.getColumnIndex(column), defaultValue);
    }

    public String getString(int column, String defaultValue) {
        return getValue(column, CQL3Type.Name.VARCHAR, CQL3Type.Name.TEXT, CQL3Type.Name.ASCII, defaultValue);
    }

    public InetAddress getInet(String column) {
        return getInet(metadata.getColumnIndex(column));
    }

    public InetAddress getInet(int column) {
        return getInet(column, null);
    }

    public InetAddress getInet(String column, InetAddress defaultValue) {
        return getInet(metadata.getColumnIndex(column), defaultValue);
    }

    public InetAddress getInet(int column, InetAddress defaultValue) {
        return getValue(column, CQL3Type.Name.INET, defaultValue);
    }

    public BigInteger getVarint(String column) {
        return getVarint(metadata.getColumnIndex(column));
    }

    public BigInteger getVarint(int column) {
        return getVarint(column, null);
    }

    public BigInteger getVarint(String column, BigInteger defaultValue) {
        return getVarint(metadata.getColumnIndex(column), defaultValue);
    }

    public BigInteger getVarint(int column, BigInteger defaultValue) {
        return getValue(column, CQL3Type.Name.VARINT, defaultValue);
    }

    public UUID getUUID(String column) {
        return getUUID(metadata.getColumnIndex(column));
    }

    public UUID getUUID(int column) {
        return getUUID(column, null);
    }

    public UUID getUUID(String column, UUID defaultValue) {
        return getUUID(metadata.getColumnIndex(column), defaultValue);
    }

    public UUID getUUID(int column, UUID defaultValue) {
        return getValue(column, CQL3Type.Name.UUID, CQL3Type.Name.TIMEUUID, defaultValue);
    }

    public <T> List<T> getList(String column, Class<T> valueClass) {
        return getList(metadata.getColumnIndex(column), valueClass);
    }

    public <T> List<T> getList(int column, Class<T> valueClass) {
        return getList(column, valueClass, Collections.<T>emptyList());
    }

    public <T> List<T> getList(String column, Class<T> valueClass, List<T> defaultValue) {
        return getList(metadata.getColumnIndex(column), valueClass, defaultValue);
    }

    public <T> List<T> getList(int column, Class<T> valueClass, List<T> defaultValue) {
        CQL3Type columnType = metadata.validateColumnType(column, CQL3Type.Name.LIST);
        Class<?> expectedClass = columnType.typeArguments().get(0).name().javaType;
        if (expectedClass.isAssignableFrom(valueClass)) {
            throw new CQL3TypeError(String.format("column type does not match: %s, List<%s> (expected: List<%s>)", metadata.getColumnName(column), valueClass.getName(), expectedClass.getName()));
        }
        return getValue(columnType, row.get(column), defaultValue);
    }

    public <T> Set<T> getSet(String column, Class<T> valueClass) {
        return getSet(metadata.getColumnIndex(column), valueClass);
    }

    public <T> Set<T> getSet(int column, Class<T> valueClass) {
        return getSet(column, valueClass, Collections.<T>emptySet());
    }

    public <T> Set<T> getSet(String column, Class<T> valueClass, Set<T> defaultValue) {
        return getSet(metadata.getColumnIndex(column), valueClass, defaultValue);
    }

    public <T> Set<T> getSet(int column, Class<T> valueClass, Set<T> defaultValue) {
        CQL3Type columnType = metadata.validateColumnType(column, CQL3Type.Name.SET);
        Class<?> expectedClass = columnType.typeArguments().get(0).name().javaType;
        if (!expectedClass.isAssignableFrom(valueClass)) {
            throw new CQL3TypeError(String.format("column type does not match: %s, Set<%s> (expected: Set<%s>)", metadata.getColumnName(column), valueClass.getName(), expectedClass.getName()));
        }
        return getValue(columnType, row.get(column), defaultValue);
    }

    public <K, V> Map<K, V> getMap(String column, Class<K> keyClass, Class<V> valueClass) {
        return getMap(metadata.getColumnIndex(column), keyClass, valueClass);
    }

    public <K, V> Map<K, V> getMap(int column, Class<K> keyClass, Class<V> valueClass) {
        return getMap(column, keyClass, valueClass, Collections.<K, V>emptyMap());
    }

    public <K, V> Map<K, V> getMap(String column, Class<K> keyClass, Class<V> valueClass, Map<K, V> defaultValue) {
        return getMap(metadata.getColumnIndex(column), keyClass, valueClass, defaultValue);
    }

    public <K, V> Map<K, V> getMap(int column, Class<K> keyClass, Class<V> valueClass, Map<K, V> defaultValue) {
        CQL3Type columnType = metadata.validateColumnType(column, CQL3Type.Name.MAP);
        Class<?> expectedKeyClass = columnType.typeArguments().get(0).name().javaType;
        Class<?> expectedValueClass = columnType.typeArguments().get(1).name().javaType;
        if (!expectedKeyClass.isAssignableFrom(keyClass) || !expectedValueClass.isAssignableFrom(valueClass)) {
            throw new CQL3TypeError(String.format("column type does not match: %s, Map<%s, %s> (expected: Map<%s, %s>)", metadata.getColumnName(column), keyClass.getName(), valueClass.getName(), expectedKeyClass.getName(), expectedValueClass.getName()));
        }
        return getValue(columnType, row.get(column), defaultValue);
    }

    private <T> T getValue(int index, CQL3Type.Name name, T defaultValue) {
        return getValue(metadata.validateColumnType(index, name), row.get(index), defaultValue);
    }

    private <T> T getValue(int index, CQL3Type.Name name1, CQL3Type.Name name2, T defaultValue) {
        return getValue(metadata.validateColumnType(index, name1, name2), row.get(index), defaultValue);
    }

    private <T> T getValue(int index, CQL3Type.Name name1, CQL3Type.Name name2, CQL3Type.Name name3, T defaultValue) {
        return getValue(metadata.validateColumnType(index, name1, name2, name3), row.get(index), defaultValue);
    }

    @SuppressWarnings("unchecked")
    private <T> T getValue(CQL3Type columnType, ByteBuffer buf, T defaultValue) {
        if (buf == null || buf.remaining() == 0) {
            return defaultValue;
        }

        return (T)columnType.deserialize(buf);
    }
}
