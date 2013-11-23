package cassandra.metadata;

import cassandra.cql.Row;
import cassandra.cql.type.CQL3Type;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;

public class ColumnMetadata extends MetadataEntity implements Comparable<ColumnMetadata> {

    private final Metadata metadata;
    private final String keyspace;
    private final String table;
    private final String name;
    private final int componentIndex;
    private final String indexName;
    private final Map<String, String> indexOptions;
    private final String indexType;
    private final String type;
    private final String validatorClass;
    private final ComparatorOrValidator validator;
    private final CQL3Type cqlType;

    public static class Builder {

        protected Metadata metadata;
        protected String keyspace;
        protected String table;
        protected String name;
        protected int componentIndex;
        protected String indexName;
        protected Map<String, String> indexOptions;
        protected String indexType;
        protected String type;
        protected String validatorClass;

        public Builder setMetadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setKeyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setComponentIndex(int componentIndex) {
            this.componentIndex = componentIndex;
            return this;
        }

        public Builder setIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder setIndexOptions(Map<String, String> indexOptions) {
            this.indexOptions = indexOptions;
            return this;
        }

        public Builder setIndexType(String indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setValidatorClass(String validatorClass) {
            this.validatorClass = validatorClass;
            return this;
        }

        public Builder mergeFrom(Metadata metadata, Row row) {
            setMetadata(metadata);
            setKeyspace(row.getString("keyspace_name"));
            setTable(row.getString("columnfamily_name"));
            setName(row.getString("column_name", ""));
            setComponentIndex(row.getInt("component_index"));
            setIndexName(row.getString("index_name"));
            String indexOptions = row.getString("index_options");
            if (indexOptions != null) {
                setIndexOptions(MetadataService.convertAsMap(indexOptions, String.class, String.class));
            }
            setIndexType(row.getString("index_type"));
            setType(row.getString("type"));
            setValidatorClass(row.getString("validator"));
            return this;
        }

        public Builder mergeFrom(ColumnMetadata column) {
            setMetadata(column.metadata);
            setKeyspace(column.keyspace);
            setTable(column.table);
            setName(column.name);
            setComponentIndex(column.componentIndex);
            setIndexName(column.indexName);
            setIndexOptions(column.indexOptions);
            setIndexType(column.indexType);
            setType(column.type);
            setValidatorClass(column.validatorClass);
            return this;
        }

        public ColumnMetadata build() {
            return new ColumnMetadata(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public ColumnMetadata(Builder builder) {
        metadata = builder.metadata;
        keyspace = builder.keyspace;
        table = builder.table;
        name = builder.name;
        componentIndex = builder.componentIndex;
        indexName = builder.indexName;
        if (builder.indexOptions != null) {
            indexOptions = Collections.unmodifiableMap(builder.indexOptions);
        } else {
            indexOptions = null;
        }
        indexType = builder.indexType;
        type = builder.type;
        validatorClass = builder.validatorClass;
        validator = new ComparatorOrValidator(validatorClass);
        cqlType = CQL3Type.TypeParser.parse(validatorClass).type();
    }

    @JsonIgnore
    public Metadata getMetadata() {
        return metadata;
    }

    @JsonProperty("keyspace_name")
    public String getKeyspace() {
        return keyspace;
    }

    @JsonProperty("columnfamily_name")
    public String getTable() {
        return table;
    }

    @JsonProperty("column_name")
    public String getName() {
        return name;
    }

    public int getComponentIndex() {
        return componentIndex;
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<String, String> getIndexOptions() {
        return indexOptions;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getType() {
        return type;
    }

    @JsonProperty("validator")
    public String getValidatorClass() {
        return validatorClass;
    }

    @JsonIgnore
    public ComparatorOrValidator getValidator() {
        return validator;
    }

    @JsonIgnore
    public CQL3Type getCqlType() {
        return cqlType;
    }

    @JsonIgnore
    public boolean isIndex() {
        return indexName != null && !indexName.isEmpty();
    }

    @JsonIgnore
    public boolean isPartitionKey() {
        return "partition_key".equals(type);
    }

    @JsonIgnore
    public boolean isClusteringKey() {
        return "clustering_key".equals(type);
    }

    @JsonIgnore
    public boolean isRegular() {
        return "regular".equals(type);
    }

    @JsonIgnore
    public boolean isCompactValue() {
        return "compact_value".equals(type);
    }

    @Override
    public int compareTo(ColumnMetadata other) {
        return compareTo(this, other);
    }

    public static int compareTo(ColumnMetadata c1, ColumnMetadata c2) {
        if (c1.isPartitionKey()) {
            if (c2.isPartitionKey()) {
                return c1.getComponentIndex() - c2.getComponentIndex();
            } else {
                return -1;
            }
        } else if (c1.isClusteringKey()) {
            if (c2.isPartitionKey()) {
                return 1;
            } else if (c2.isClusteringKey()) {
                return c1.getComponentIndex() - c2.getComponentIndex();
            } else {
                return -1;
            }
        } else {
            if (c2.isPartitionKey() || c2.isClusteringKey()) {
                return 1;
            } else {
                return c1.getName().compareTo(c2.getName());
            }
        }
    }
}
