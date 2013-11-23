package cassandra.cql;

import cassandra.CassandraSession;
import cassandra.cql.type.CQL3Type;
import cassandra.metadata.ColumnMetadata;
import cassandra.metadata.Partitioner;
import cassandra.metadata.TableMetadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class PreparedStatement extends AbstractStatement<PreparedStatement> {

    public static class StatementId {

        private final byte[] array;

        public StatementId(byte[] array) {
            this.array = array;
        }

        public byte[] array() {
            return array;
        }

        public int size() {
            return array.length;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o != null && o instanceof StatementId && Arrays.equals(array(), ((StatementId)o).array());
        }

        @Override
        public String toString() {
            return Partitioner.Hex.bytesToHex(array);
        }
    }

    private final StatementId id;
    private final RowMetadata metadata, parameterMetadata;
    private final int[] partitionKeyIndexes;

    public PreparedStatement(CassandraSession session, StatementId id, String query, RowMetadata metadata, RowMetadata parameterMetadata) {
        this.id = id;
        this.metadata = metadata;
        this.parameterMetadata = parameterMetadata;
        setSession(session);
        setQuery(query);
        int[] idxs = null;
        if (parameterMetadata != null && parameterMetadata.getColumnCount() > 0) {
            String keyspaceName = parameterMetadata.getKeyspaceName(0);
            String tableName = parameterMetadata.getTableName(0);
            setKeyspace(keyspaceName);
            setParameters(new ByteBuffer[parameterMetadata.getColumnCount()]);
            TableMetadata table = session.metadata().getTable(keyspaceName, tableName);
            if (table != null) {
                List<ColumnMetadata> pkList = table.getPartitionKey();
                idxs = new int[pkList.size()];
                for (int i = 0; i < idxs.length; i++) {
                    idxs[i] = -1;
                }
                for (int i = 0; i < parameterMetadata.getColumnCount(); i++) {
                    String name = parameterMetadata.getColumnName(i);
                    for (int j = 0; j < pkList.size(); j++) {
                        if (name.equals(pkList.get(j).getName())) {
                            idxs[j] = i;
                            break;
                        }
                    }
                }
                boolean hasnull = false;
                for (int indexe : idxs) {
                    if (indexe < 0) {
                        hasnull = true;
                        break;
                    }
                }
                if (hasnull) {
                    idxs = null;
                }
            }
        }
        partitionKeyIndexes = idxs;
    }

    private PreparedStatement(PreparedStatement pstmt) {
        super(pstmt);
        this.id = pstmt.getId();
        this.metadata = pstmt.getMetadata();
        this.parameterMetadata = pstmt.getParameterMetadata();
        partitionKeyIndexes = pstmt.partitionKeyIndexes;
    }

    public StatementId getId() {
        return id;
    }

    public RowMetadata getMetadata() {
        return metadata;
    }

    public RowMetadata getParameterMetadata() {
        return parameterMetadata;
    }

    public PreparedStatement setBool(int column, boolean value) {
        return setValue(column, CQL3Type.Name.BOOLEAN, value);
    }

    public PreparedStatement setBool(String column, boolean value) {
        return setValue(column, CQL3Type.Name.BOOLEAN, value);
    }

    public PreparedStatement setBlob(int column, ByteBuffer value) {
        return setValue(column, CQL3Type.Name.BLOB, value);
    }

    public PreparedStatement setBlob(String column, ByteBuffer value) {
        return setValue(column, CQL3Type.Name.BLOB, value);
    }

    public PreparedStatement setDate(int column, Date value) {
        return setValue(column, CQL3Type.Name.TIMESTAMP, value);
    }

    public PreparedStatement setDate(String column, Date value) {
        return setValue(column, CQL3Type.Name.TIMESTAMP, value);
    }

    public PreparedStatement setDecimal(int column, BigDecimal value) {
        return setValue(column, CQL3Type.Name.DECIMAL, value);
    }

    public PreparedStatement setDecimal(String column, BigDecimal value) {
        return setValue(column, CQL3Type.Name.DECIMAL, value);
    }

    public PreparedStatement setDouble(int column, double value) {
        return setValue(column, CQL3Type.Name.DOUBLE, value);
    }

    public PreparedStatement setDouble(String column, double value) {
        return setValue(column, CQL3Type.Name.DOUBLE, value);
    }

    public PreparedStatement setFloat(int column, float value) {
        return setValue(column, CQL3Type.Name.FLOAT, value);
    }

    public PreparedStatement setFloat(String column, float value) {
        return setValue(column, CQL3Type.Name.FLOAT, value);
    }

    public PreparedStatement setInt(int column, int value) {
        return setValue(column, CQL3Type.Name.INT, value);
    }

    public PreparedStatement setInt(String column, int value) {
        return setValue(column, CQL3Type.Name.INT, value);
    }

    public PreparedStatement setLong(int column, long value) {
        return setValue(column, CQL3Type.Name.BIGINT, value);
    }

    public PreparedStatement setLong(String column, long value) {
        return setValue(column, CQL3Type.Name.BIGINT, value);
    }

    public PreparedStatement setString(int column, String value) {
        return setValue(column, CQL3Type.Name.VARCHAR, CQL3Type.Name.TEXT, CQL3Type.Name.ASCII, value);
    }

    public PreparedStatement setString(String column, String value) {
        return setValue(column, CQL3Type.Name.VARCHAR, CQL3Type.Name.TEXT, CQL3Type.Name.ASCII, value);
    }

    public PreparedStatement setInet(int column, InetAddress value) {
        return setValue(column, CQL3Type.Name.INET, value);
    }

    public PreparedStatement setInet(String column, InetAddress value) {
        return setValue(column, CQL3Type.Name.INET, value);
    }

    public PreparedStatement setVarint(int column, BigInteger value) {
        return setValue(column, CQL3Type.Name.VARINT, value);
    }

    public PreparedStatement setVarint(String column, BigInteger value) {
        return setValue(column, CQL3Type.Name.VARINT, value);
    }

    public PreparedStatement setUUID(int column, UUID value) {
        return setValue(column, CQL3Type.Name.UUID, value);
    }

    public PreparedStatement setUUID(String column, UUID value) {
        return setValue(column, CQL3Type.Name.UUID, value);
    }

    public PreparedStatement setList(int column, List<?> value) {
        return setValue(column, CQL3Type.Name.LIST, value);
    }

    public PreparedStatement setList(String column, List<?> value) {
        return setValue(column, CQL3Type.Name.LIST, value);
    }

    public PreparedStatement setSet(int column, Set<?> value) {
        return setValue(column, CQL3Type.Name.SET, value);
    }

    public PreparedStatement setSet(String column, Set<?> value) {
        return setValue(column, CQL3Type.Name.SET, value);
    }

    public PreparedStatement setMap(int column, Map<?, ?> value) {
        return setValue(column, CQL3Type.Name.MAP, value);
    }

    public PreparedStatement setMap(String column, Map<?, ?> value) {
        return setValue(column, CQL3Type.Name.MAP, value);
    }

    public PreparedStatement setObject(int column, Object value) {
        return setValue(column, getParameterMetadata().getColumnType(column), value);
    }

    public PreparedStatement setObject(String column, Object value) {
        int[] idxs = getParameterMetadata().getColumnIndexArray(column);
        ByteBuffer buf = null;
        if (value != null) {
            CQL3Type columnType = getParameterMetadata().getColumnType(idxs[0]);
            for (int i = 1; i < idxs.length; i++) {
                getParameterMetadata().validateColumnType(idxs[i], columnType, value);
            }
            buf = columnType.serialize(value);
        }
        for (int index : idxs) {
            getParameters()[index] = buf;
        }
        return this;
    }

    public PreparedStatement bind(Object... values) {
        if (values == null) {
            throw new NullPointerException("values");
        }
        if (values.length == 0) {
            throw new IllegalArgumentException("empty values");
        }
        if (values.length > getParameterMetadata().getColumnCount()) {
            throw new IllegalArgumentException(String.format("number of bound variables does not match number of parameters: %d (expected: %d)", values.length, getParameterMetadata().getColumnCount()));
        }
        for (int i = 0; i < values.length; i++) {
            setObject(i, values[i]);
        }
        return this;
    }

    @Override
    public RoutingKey getRoutingKey() {
        RoutingKey routingKey = super.getRoutingKey();
        if (routingKey != null) {
            return routingKey;
        }
        if (partitionKeyIndexes != null) {
            ByteBuffer[] keys = new ByteBuffer[partitionKeyIndexes.length];
            for (int i = 0; i < keys.length; i++) {
                ByteBuffer value = getParameters()[partitionKeyIndexes[i]];
                if (value == null) {
                    return null;
                }
                keys[i] = value;
            }
            routingKey = RoutingKey.copyFrom(keys);
        }
        return routingKey;
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public PreparedStatement clone() {
        return new PreparedStatement(this);
    }

    private PreparedStatement setValue(int index, CQL3Type.Name name, Object value) {
        return setValue(index, getParameterMetadata().validateColumnType(index, name), value);
    }

    private PreparedStatement setValue(int index, CQL3Type.Name name1, CQL3Type.Name name2, Object value) {
        return setValue(index, getParameterMetadata().validateColumnType(index, name1, name2), value);
    }

    private PreparedStatement setValue(int index, CQL3Type.Name name1, CQL3Type.Name name2, CQL3Type.Name name3, Object value) {
        return setValue(index, getParameterMetadata().validateColumnType(index, name1, name2, name3), value);
    }

    private PreparedStatement setValue(String column, CQL3Type.Name name, Object value) {
        int[] idxs = getParameterMetadata().getColumnIndexArray(column);
        CQL3Type columnType = null;
        for (int index : idxs) {
            columnType = getParameterMetadata().validateColumnType(index, name);
        }
        return setValue(idxs, columnType, value);
    }

    private PreparedStatement setValue(String column, CQL3Type.Name name1, CQL3Type.Name name2, Object value) {
        int[] idxs = getParameterMetadata().getColumnIndexArray(column);
        CQL3Type columnType = null;
        for (int index : idxs) {
            columnType = getParameterMetadata().validateColumnType(index, name1, name2);
        }
        return setValue(idxs, columnType, value);
    }

    private PreparedStatement setValue(String column, CQL3Type.Name name1, CQL3Type.Name name2, CQL3Type.Name name3, Object value) {
        int[] idxs = getParameterMetadata().getColumnIndexArray(column);
        CQL3Type columnType = null;
        for (int index : idxs) {
            columnType = getParameterMetadata().validateColumnType(index, name1, name2, name3);
        }
        return setValue(idxs, columnType, value);
    }

    private PreparedStatement setValue(int index, CQL3Type columnType, Object value) {
        if (value == null) {
            getParameters()[index] = null;
        } else {
            getParameters()[index] = getParameterMetadata().validateColumnType(index, columnType, value).serialize(value);
        }
        return this;
    }

    private PreparedStatement setValue(int[] idxs, CQL3Type columnType, Object value) {
        ByteBuffer buf = null;
        if (value != null) {
            buf = getParameterMetadata().validateColumnType(idxs[0], columnType, value).serialize(value);
        }
        for (int index : idxs) {
            getParameters()[index] = buf;
        }
        return this;
    }
}
