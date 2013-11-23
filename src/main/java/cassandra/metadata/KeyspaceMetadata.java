package cassandra.metadata;

import cassandra.cql.Row;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class KeyspaceMetadata extends MetadataEntity {

    private final Metadata metadata;
    private final String name;
    private final boolean durableWrites;
    private final String strategyClass;
    private final Map<String, String> strategyOptions;
    private final ReplicationStrategy replicationStrategy;

    public static class Builder {

        protected Metadata metadata;
        protected String name;
        protected boolean durableWrites;
        protected String strategyClass;
        protected Map<String, String> strategyOptions;

        public Builder setMetadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setDurableWrites(boolean durableWrites) {
            this.durableWrites = durableWrites;
            return this;
        }

        public Builder setStrategyClass(String strategyClass) {
            this.strategyClass = strategyClass;
            return this;
        }

        public Builder setStrategyOptions(Map<String, String> strategyOptions) {
            this.strategyOptions = strategyOptions;
            return this;
        }

        public Builder mergeFrom(Metadata metadata, Row row) {
            setMetadata(metadata);
            setName(row.getString("keyspace_name"));
            setDurableWrites(row.getBool("durable_writes"));
            setStrategyClass(row.getString("strategy_class"));
            setStrategyOptions(MetadataService.convertAsMap(row.getString("strategy_options"), String.class, String.class));
            return this;
        }

        public Builder mergeFrom(KeyspaceMetadata keyspace) {
            setMetadata(keyspace.metadata);
            setName(keyspace.name);
            setDurableWrites(keyspace.durableWrites);
            setStrategyClass(keyspace.getStrategyClass());
            setStrategyOptions(keyspace.getStrategyOptions());
            return this;
        }

        public KeyspaceMetadata build() {
            return new KeyspaceMetadata(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public KeyspaceMetadata(Builder builder) {
        metadata = builder.metadata;
        name = builder.name;
        durableWrites = builder.durableWrites;
        strategyClass = builder.strategyClass;
        strategyOptions = builder.strategyOptions;
        replicationStrategy = ReplicationStrategy.getReplicationStrategy(this);
    }

    @JsonIgnore
    public Metadata getMetadata() {
        return metadata;
    }

    @JsonIgnore
    public String getCluster() {
        return metadata.getClusterName();
    }

    @JsonProperty("keyspace_name")
    public String getName() {
        return name;
    }

    public boolean isDurableWrites() {
        return durableWrites;
    }

    public String getStrategyClass() {
        return strategyClass;
    }

    public Map<String, String> getStrategyOptions() {
        return strategyOptions;
    }

    @JsonIgnore
    public ReplicationStrategy getReplicationStrategy() {
        return replicationStrategy;
    }

    @JsonIgnore
    public List<TableMetadata> getTables() {
        return metadata.getTables(name);
    }

    @JsonIgnore
    public boolean hasTable(String table) {
        return metadata.hasTable(name, table);
    }

    @JsonIgnore
    public TableMetadata getTable(String table) {
        return metadata.getTable(name, table);
    }
}
