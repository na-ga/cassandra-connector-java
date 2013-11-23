package cassandra.cql;

import cassandra.cql.RowMetadata.Column;
import cassandra.cql.type.CQL3Type;
import cassandra.cql.type.CQL3TypeError;

import java.util.*;

public class RowMetadata implements Iterable<Column> {

    private final Column[] columns;
    private final Map<String, int[]> namemap;

    public RowMetadata(Column[] columns) {
        this.columns = columns;
        namemap = new HashMap<String, int[]>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            String name = columns[i].name().toLowerCase();
            int[] idxs;
            int[] prev = namemap.get(name);
            if (prev == null) {
                idxs = new int[]{i};
            } else {
                idxs = new int[prev.length + 1];
                System.arraycopy(prev, 0, idxs, 0, prev.length);
                idxs[idxs.length - 1] = i;
            }
            namemap.put(name, idxs);
        }
    }

    public String getKeyspaceName(int column) {
        return columns[column].keyspace();
    }

    public String getKeyspaceName(String column) {
        return getKeyspaceName(getColumnIndex(column));
    }

    public String getTableName(int column) {
        return columns[column].table();
    }

    public String getTableName(String column) {
        return getTableName(getColumnIndex(column));
    }

    public String getColumnName(int column) {
        return columns[column].name();
    }

    public CQL3Type getColumnType(int column) {
        return columns[column].type();
    }

    public CQL3Type getColumnType(String column) {
        return getColumnType(getColumnIndex(column));
    }

    public int getColumnIndex(String column) {
        return getColumnIndexArray(column)[0];
    }

    public int[] getColumnIndexArray(String column) {
        String name = column;
        boolean caseSensitive = false;
        if (name.length() >= 2 && name.charAt(0) == '"' && name.charAt(name.length() - 1) == '"') {
            name = name.substring(1, name.length() - 1);
            caseSensitive = true;
        }
        int[] idxs = namemap.get(name.toLowerCase());
        if (idxs == null) {
            throw new IllegalArgumentException(String.format("no matching column found, %s", column));
        }
        if (!caseSensitive) {
            return idxs;
        }
        int found = 0;
        for (int idx : idxs) {
            if (name.equals(columns[idx].name)) {
                found++;
            }
        }
        if (found == idxs.length) {
            return idxs;
        }
        int[] result = new int[found];
        int j = 0;
        for (int idx : idxs) {
            if (name.equals(columns[idx].name)) {
                result[j++] = idx;
            }
        }
        return result;
    }

    public CQL3Type validateColumnType(int column, CQL3Type.Name name) {
        CQL3Type columnType = getColumnType(column);
        if (name != columnType.name()) {
            throw new CQL3TypeError(String.format("column type does not match: %s (expected: %s)", getColumnName(column), columnType.name()));
        }
        return columnType;
    }

    public CQL3Type validateColumnType(int column, CQL3Type.Name name1, CQL3Type.Name name2) {
        CQL3Type columnType = getColumnType(column);
        if (name1 != columnType.name() && name2 != columnType.name()) {
            throw new CQL3TypeError(String.format("column type does not match: %s (expected: %s)", getColumnName(column), columnType.name()));
        }
        return columnType;
    }

    public CQL3Type validateColumnType(int column, CQL3Type.Name name1, CQL3Type.Name name2, CQL3Type.Name name3) {
        CQL3Type columnType = getColumnType(column);
        if (name1 != columnType.name() && name2 != columnType.name() && name3 != columnType.name()) {
            throw new CQL3TypeError(String.format("column type does not match: %s (expected: %s)", getColumnName(column), columnType.name()));
        }
        return columnType;
    }

    public CQL3Type validateColumnType(int column, CQL3Type columnType, Object value) {
        String columnName = getColumnName(column);
        CQL3Type.Name actualName = getColumnType(column).name();
        if (actualName != columnType.name()) {
            throw new CQL3TypeError(String.format("column type does not match: %s, %s (expected: %s)", columnName, columnType.name(), actualName));
        }

        switch (columnType.name()) {
            case LIST:
                List<?> list = (List<?>)value;
                if (!list.isEmpty()) {
                    Class<?> providedClass = list.get(0).getClass();
                    Class<?> expectedClass = columnType.typeArguments().get(0).name().javaType;
                    if (!expectedClass.isAssignableFrom(providedClass)) {
                        throw new CQL3TypeError(String.format("column type does not match: %s, List<%s> (expected: List<%s>)", columnName, providedClass.getName(), expectedClass.getName()));
                    }
                }
                break;
            case SET:
                Set<?> set = (Set<?>)value;
                if (!set.isEmpty()) {
                    Class<?> providedClass = set.iterator().next().getClass();
                    Class<?> expectedClass = columnType.typeArguments().get(0).name().javaType;
                    if (!expectedClass.isAssignableFrom(providedClass)) {
                        throw new CQL3TypeError(String.format("column type does not match: %s, Set<%s> (expected: Set<%s>)", columnName, providedClass.getName(), expectedClass.getName()));
                    }
                }
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>)value;
                if (!map.isEmpty()) {
                    Map.Entry<?, ?> entry = map.entrySet().iterator().next();
                    Class<?> providedKeysClass = entry.getKey().getClass();
                    Class<?> providedValuesClass = entry.getValue().getClass();
                    Class<?> expectedKeyClass = columnType.typeArguments().get(0).name().javaType;
                    Class<?> expectedValueClass = columnType.typeArguments().get(1).name().javaType;
                    if (!expectedKeyClass.isAssignableFrom(providedKeysClass) || !expectedValueClass.isAssignableFrom(providedValuesClass)) {
                        throw new CQL3TypeError(String.format("column type does not match: %s, Map<%s, %s> (expected: Map<%s, %s>)", columnName, providedKeysClass.getName(), providedValuesClass.getName(), expectedKeyClass.getName(), expectedValueClass.getName()));
                    }
                }
                break;
            default:
                Class<?> providedClass = value.getClass();
                Class<?> expectedClass = columnType.name().javaType;
                if (!expectedClass.isAssignableFrom(providedClass)) {
                    throw new CQL3TypeError(String.format("column type does not match: %s, %s (expected: %s)", columnName, providedClass.getName(), expectedClass.getName()));
                }
                break;
        }
        return columnType;
    }

    public Column getColumn(int index) {
        return columns[index];
    }

    public int getColumnCount() {
        return columns.length;
    }

    @Override
    public Iterator<Column> iterator() {
        return Arrays.asList(columns).iterator();
    }

    public static class Column {

        private String keyspace;
        private String table;
        private String name;
        private CQL3Type type;

        public Column(String keyspace, String table, String name, CQL3Type type) {
            this.keyspace = keyspace;
            this.table = table;
            this.name = name;
            this.type = type;
        }

        public String keyspace() {
            return keyspace;
        }

        public String table() {
            return table;
        }

        public String name() {
            return name;
        }

        public CQL3Type type() {
            return type;
        }

        @Override
        public String toString() {
            return String.format("%s.%s.%s(%s)", keyspace, table, name, type.name());
        }
    }
}
