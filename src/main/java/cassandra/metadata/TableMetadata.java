package cassandra.metadata;

import cassandra.cql.Row;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableMetadata extends MetadataEntity {

    private Metadata metadata;
    private String keyspace;
    private String name;
    private double bloomFilterFpChance;
    private String caching;
    private List<String> columnAliases;
    private String comment;
    private String compactionStrategyClass;
    private Map<String, String> compactionStrategyOptions;
    private String comparatorClass;
    private Map<String, String> compressionParameters;
    private int defaultTimeToLive;
    private String defaultValidator;
    private Map<String, Long> droppedColumns;
    private int gcGraceSeconds;
    private int indexInterval;
    private List<String> keyAliases;
    private String keyValidatorClass;
    private double localReadRepairChance;
    private int maxCompactionThreshold;
    private int memtableFlushPeriodInMs;
    private int minCompactionThreshold;
    private boolean populateIOCacheOnFlush;
    private double readRepairChance;
    private boolean replicateOnWrite;
    private String speculativeRetry;
    private String subcomparator;
    private String type;
    private String valueAlias;
    private List<ColumnMetadata> partitionKeyList, clusteringKeyList;
    private ComparatorOrValidator comparator, keyValidator;

    public static class Builder {

        protected Metadata metadata;
        protected String keyspace;
        protected String name;
        protected double bloomFilterFpChance;
        protected String caching;
        protected List<String> columnAliases;
        protected String comment;
        protected String compactionStrategyClass;
        protected Map<String, String> compactionStrategyOptions;
        protected String comparatorClass;
        protected Map<String, String> compressionParameters;
        protected int defaultTimeToLive;
        protected String defaultValidator;
        protected Map<String, Long> droppedColumns;
        protected int gcGraceSeconds;
        protected int indexInterval;
        protected List<String> keyAliases;
        protected String keyValidatorClass;
        protected double localReadRepairChance;
        protected int maxCompactionThreshold;
        protected int memtableFlushPeriodInMs;
        protected int minCompactionThreshold;
        protected boolean populateIOCacheOnFlush;
        protected double readRepairChance;
        protected boolean replicateOnWrite;
        protected String speculativeRetry;
        protected String subcomparator;
        protected String type;
        protected String valueAlias;

        public Builder setMetadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setKeyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setBloomFilterFpChance(double bloomFilterFpChance) {
            this.bloomFilterFpChance = bloomFilterFpChance;
            return this;
        }

        public Builder setCaching(String caching) {
            this.caching = caching;
            return this;
        }

        public Builder setColumnAliases(List<String> columnAliases) {
            this.columnAliases = columnAliases;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder setCompactionStrategyClass(String compactionStrategyClass) {
            this.compactionStrategyClass = compactionStrategyClass;
            return this;
        }

        public Builder setCompactionStrategyOptions(Map<String, String> compactionStrategyOptions) {
            this.compactionStrategyOptions = compactionStrategyOptions;
            return this;
        }

        public Builder setComparatorClass(String comparatorClass) {
            this.comparatorClass = comparatorClass;
            return this;
        }

        public Builder setCompressionParameters(Map<String, String> compressionParameters) {
            this.compressionParameters = compressionParameters;
            return this;
        }

        public Builder setDefaultTimeToLive(int defaultTimeToLive) {
            this.defaultTimeToLive = defaultTimeToLive;
            return this;
        }

        public Builder setDefaultValidator(String defaultValidator) {
            this.defaultValidator = defaultValidator;
            return this;
        }

        public Builder setDroppedColumns(Map<String, Long> droppedColumns) {
            this.droppedColumns = droppedColumns;
            return this;
        }

        public Builder setGcGraceSeconds(int gcGraceSeconds) {
            this.gcGraceSeconds = gcGraceSeconds;
            return this;
        }

        public Builder setIndexInterval(int indexInterval) {
            this.indexInterval = indexInterval;
            return this;
        }

        public Builder setKeyAliases(List<String> keyAliases) {
            this.keyAliases = keyAliases;
            return this;
        }

        public Builder setKeyValidatorClass(String keyValidatorClass) {
            this.keyValidatorClass = keyValidatorClass;
            return this;
        }

        public Builder setLocalReadRepairChance(double localReadRepairChance) {
            this.localReadRepairChance = localReadRepairChance;
            return this;
        }

        public Builder setMaxCompactionThreshold(int maxCompactionThreshold) {
            this.maxCompactionThreshold = maxCompactionThreshold;
            return this;
        }

        public Builder setMemtableFlushPeriodInMs(int memtableFlushPeriodInMs) {
            this.memtableFlushPeriodInMs = memtableFlushPeriodInMs;
            return this;
        }

        public Builder setMinCompactionThreshold(int minCompactionThreshold) {
            this.minCompactionThreshold = minCompactionThreshold;
            return this;
        }

        public Builder setPopulateIOCacheOnFlush(boolean populateIOCacheOnFlush) {
            this.populateIOCacheOnFlush = populateIOCacheOnFlush;
            return this;
        }

        public Builder setReadRepairChance(double readRepairChance) {
            this.readRepairChance = readRepairChance;
            return this;
        }

        public Builder setReplicateOnWrite(boolean replicateOnWrite) {
            this.replicateOnWrite = replicateOnWrite;
            return this;
        }

        public Builder setSpeculativeRetry(String speculativeRetry) {
            this.speculativeRetry = speculativeRetry;
            return this;
        }

        public Builder setSubcomparator(String subcomparator) {
            this.subcomparator = subcomparator;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setValueAlias(String valueAlias) {
            this.valueAlias = valueAlias;
            return this;
        }

        public Builder mergeFrom(Metadata metadata, Row row) {
            setMetadata(metadata);
            setKeyspace(row.getString("keyspace_name"));
            setName(row.getString("columnfamily_name"));
            setBloomFilterFpChance(row.getDouble("bloom_filter_fp_chance", 0.01D));
            setCaching(row.getString("caching", "KEYS_ONLY"));
            setColumnAliases(MetadataService.convertAsList(row.getString("column_aliases"), String.class));
            setComment(row.getString("comment", ""));
            setCompactionStrategyClass(row.getString("compaction_strategy_class"));
            setCompactionStrategyOptions(MetadataService.convertAsMap(row.getString("compaction_strategy_options"), String.class, String.class));
            setComparatorClass(row.getString("comparator"));
            setCompressionParameters(MetadataService.convertAsMap(row.getString("compression_parameters"), String.class, String.class));
            setDefaultTimeToLive(row.getInt("default_time_to_live"));
            setDefaultValidator(row.getString("default_validator"));
            setDroppedColumns(row.getMap("dropped_columns", String.class, Long.class, null));
            setGcGraceSeconds(row.getInt("gc_grace_seconds"));
            setIndexInterval(row.getInt("index_interval", 128));
            setKeyAliases(MetadataService.convertAsList(row.getString("key_aliases"), String.class));
            setKeyValidatorClass(row.getString("key_validator"));
            setLocalReadRepairChance(row.getDouble("local_read_repair_chance"));
            setMaxCompactionThreshold(row.getInt("max_compaction_threshold"));
            setMinCompactionThreshold(row.getInt("memtable_flush_period_in_ms"));
            setMemtableFlushPeriodInMs(row.getInt("min_compaction_threshold"));
            setPopulateIOCacheOnFlush(row.getBool("populate_io_cache_on_flush"));
            setReadRepairChance(row.getDouble("read_repair_chance"));
            setReplicateOnWrite(row.getBool("replicate_on_write"));
            setSpeculativeRetry(row.getString("speculative_retry", "NONE"));
            setSubcomparator(row.getString("subcomparator"));
            setType(row.getString("type"));
            setValueAlias(row.getString("value_alias"));
            return this;
        }

        public Builder mergeFrom(TableMetadata table) {
            setMetadata(table.metadata);
            setKeyspace(table.keyspace);
            setName(table.name);
            setBloomFilterFpChance(table.bloomFilterFpChance);
            setCaching(table.caching);
            setColumnAliases(table.columnAliases);
            setComment(table.comment);
            setCompactionStrategyClass(table.compactionStrategyClass);
            setCompactionStrategyOptions(table.compactionStrategyOptions);
            setComparatorClass(table.comparatorClass);
            setCompressionParameters(table.compressionParameters);
            setDefaultTimeToLive(table.defaultTimeToLive);
            setDefaultValidator(table.defaultValidator);
            setDroppedColumns(table.droppedColumns);
            setGcGraceSeconds(table.gcGraceSeconds);
            setIndexInterval(table.indexInterval);
            setKeyAliases(table.keyAliases);
            setKeyValidatorClass(table.keyValidatorClass);
            setLocalReadRepairChance(table.localReadRepairChance);
            setMaxCompactionThreshold(table.maxCompactionThreshold);
            setMinCompactionThreshold(table.minCompactionThreshold);
            setMemtableFlushPeriodInMs(table.memtableFlushPeriodInMs);
            setPopulateIOCacheOnFlush(table.populateIOCacheOnFlush);
            setReadRepairChance(table.readRepairChance);
            setReplicateOnWrite(table.replicateOnWrite);
            setSpeculativeRetry(table.speculativeRetry);
            setSubcomparator(table.subcomparator);
            setType(table.type);
            setValueAlias(table.valueAlias);
            return this;
        }

        public TableMetadata build() {
            return new TableMetadata(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public TableMetadata(Builder builder) {
        metadata = builder.metadata;
        keyspace = builder.keyspace;
        name = builder.name;
        bloomFilterFpChance = builder.bloomFilterFpChance;
        caching = builder.caching;
        columnAliases = Collections.unmodifiableList(builder.columnAliases);
        comment = builder.comment;
        compactionStrategyClass = builder.compactionStrategyClass;
        compactionStrategyOptions = Collections.unmodifiableMap(builder.compactionStrategyOptions);
        comparatorClass = builder.comparatorClass;
        compressionParameters = Collections.unmodifiableMap(builder.compressionParameters);
        defaultTimeToLive = builder.defaultTimeToLive;
        defaultValidator = builder.defaultValidator;
        if (builder.droppedColumns != null) {
            droppedColumns = Collections.unmodifiableMap(builder.droppedColumns);
        }
        gcGraceSeconds = builder.gcGraceSeconds;
        indexInterval = builder.indexInterval;
        keyAliases = Collections.unmodifiableList(builder.keyAliases);
        keyValidatorClass = builder.keyValidatorClass;
        localReadRepairChance = builder.localReadRepairChance;
        maxCompactionThreshold = builder.maxCompactionThreshold;
        minCompactionThreshold = builder.minCompactionThreshold;
        memtableFlushPeriodInMs = builder.memtableFlushPeriodInMs;
        populateIOCacheOnFlush = builder.populateIOCacheOnFlush;
        readRepairChance = builder.readRepairChance;
        replicateOnWrite = builder.replicateOnWrite;
        speculativeRetry = builder.speculativeRetry;
        subcomparator = builder.subcomparator;
        type = builder.type;
        valueAlias = builder.valueAlias;
        comparator = new ComparatorOrValidator(comparatorClass);
        keyValidator = new ComparatorOrValidator(keyValidatorClass);
    }

    @JsonIgnore
    public Metadata getMetadata() {
        return metadata;
    }

    @JsonProperty("keyspace_name")
    public String getKeyspaceName() {
        return keyspace;
    }

    @JsonIgnore
    public KeyspaceMetadata getKeyspace() {
        return metadata.getKeyspace(keyspace);
    }

    @JsonProperty("columnfamily_name")
    public String getName() {
        return name;
    }

    public double getBloomFilterFpChance() {
        return bloomFilterFpChance;
    }

    public String getCaching() {
        return caching;
    }

    public List<String> getColumnAliases() {
        return columnAliases;
    }

    public String getComment() {
        return comment;
    }

    public String getCompactionStrategyClass() {
        return compactionStrategyClass;
    }

    public Map<String, String> getCompactionStrategyOptions() {
        return compactionStrategyOptions;
    }

    @JsonProperty("comparator")
    public String getComparatorClass() {
        return comparatorClass;
    }

    @JsonIgnore
    public ComparatorOrValidator getComparator() {
        return comparator;
    }

    public Map<String, String> getCompressionParameters() {
        return compressionParameters;
    }

    public int getDefaultTimeToLive() {
        return defaultTimeToLive;
    }

    public String getDefaultValidator() {
        return defaultValidator;
    }

    public Map<String, Long> getDroppedColumns() {
        return droppedColumns;
    }

    public int getGcGraceSeconds() {
        return gcGraceSeconds;
    }

    public int getIndexInterval() {
        return indexInterval;
    }

    public List<String> getKeyAliases() {
        return keyAliases;
    }

    @JsonProperty("key_validator")
    public String getKeyValidatorClass() {
        return keyValidatorClass;
    }

    @JsonIgnore
    public ComparatorOrValidator getKeyValidator() {
        return keyValidator;
    }

    public double getLocalReadRepairChance() {
        return localReadRepairChance;
    }

    public int getMaxCompactionThreshold() {
        return maxCompactionThreshold;
    }

    public int getMemtableFlushPeriodInMs() {
        return memtableFlushPeriodInMs;
    }

    public int getMinCompactionThreshold() {
        return minCompactionThreshold;
    }

    public boolean isPopulateIOCacheOnFlush() {
        return populateIOCacheOnFlush;
    }

    public double getReadRepairChance() {
        return readRepairChance;
    }

    public boolean isReplicateOnWrite() {
        return replicateOnWrite;
    }

    public String getSpeculativeRetry() {
        return speculativeRetry;
    }

    public String getSubcomparator() {
        return subcomparator;
    }

    public String getType() {
        return type;
    }

    public String getValueAlias() {
        return valueAlias;
    }

    @JsonIgnore
    public List<ColumnMetadata> getColumns() {
        return metadata.getColumns(keyspace, name);
    }

    @JsonIgnore
    public boolean hasColumn(String column) {
        return metadata.hasColumn(keyspace, name, column);
    }

    @JsonIgnore
    public ColumnMetadata getColumn(String column) {
        return metadata.getColumn(keyspace, name, column);
    }

    @JsonIgnore
    public List<ColumnMetadata> getPrimaryKey() {
        List<ColumnMetadata> partitionKey = getPartitionKey();
        List<ColumnMetadata> clusteringKey = getClusteringKey();
        if (partitionKey.isEmpty() && clusteringKey.isEmpty()) {
            return Collections.emptyList();
        }
        List<ColumnMetadata> primaryKey = new ArrayList<ColumnMetadata>(partitionKey.size() + clusteringKey.size());
        primaryKey.addAll(partitionKey);
        primaryKey.addAll(clusteringKey);
        return primaryKey;
    }

    @JsonIgnore
    public List<ColumnMetadata> getPartitionKey() {
        List<ColumnMetadata> columns = getColumns();
        if (partitionKeyList == null && !columns.isEmpty()) {
            partitionKeyList = new ArrayList<ColumnMetadata>();
            for (ColumnMetadata column : columns) {
                if (column.isPartitionKey()) {
                    partitionKeyList.add(column);
                }
            }
        }
        if (partitionKeyList == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(partitionKeyList);
    }

    @JsonIgnore
    public List<ColumnMetadata> getClusteringKey() {
        List<ColumnMetadata> columns = getColumns();
        if (clusteringKeyList == null && !columns.isEmpty()) {
            clusteringKeyList = new ArrayList<ColumnMetadata>();
            for (ColumnMetadata column : columns) {
                if (column.isClusteringKey()) {
                    clusteringKeyList.add(column);
                }
            }
        }
        if (clusteringKeyList == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(clusteringKeyList);
    }
}
