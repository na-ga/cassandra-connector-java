package cassandra.metadata;

import cassandra.CassandraCluster;
import cassandra.cql.*;
import cassandra.routing.RoutingPolicy;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class MetadataService extends Metadata implements CassandraCluster.EventListener {

    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);
    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    }

    private final CassandraCluster cluster;
    private RoutingPolicy routingPolicy;

    public MetadataService(CassandraCluster cluster) {
        this.cluster = cluster;
        routingPolicy = new LocalRoutingPolicy();
    }

    @JsonIgnore
    public boolean isInitialized() {
        return super.getClusterName() != null;
    }

    public boolean initialize() {
        if (!updateLocal()) {
            return false;
        }
        updateKeyspaces();
        updateTables();
        updateColumns();
        return true;
    }

    public List<InetAddress> selectPeers() {
        List<InetAddress> peers = null;
        for (Row row : executeLocal("SELECT peer FROM system.peers")) {
            if (peers == null) {
                peers = new ArrayList<InetAddress>();
            }
            peers.add(row.getInet("peer"));
        }
        if (peers == null) {
            return Collections.emptyList();
        }
        return peers;
    }

    @Override
    public void setLocal(InetAddress local) {
        super.setLocal(local);
        if (!isInitialized()) {
            initialize();
        }
    }

    @Override
    public String getClusterName() {
        String clusterName = super.getClusterName();
        if (clusterName == null) {
            clusterName = "unknown";
        }
        return clusterName;
    }

    @Override
    public void onJoinCluster(CassandraCluster cluster, InetAddress endpoint) {
        validate(cluster).updatePeer(endpoint);
        if (!isInitialized() && cluster.seeds().contains(endpoint)) {
            initialize();
        }
    }

    @Override
    public void onLeaveCluster(CassandraCluster cluster, InetAddress endpoint) {
        validate(cluster).removePeer(endpoint);
    }

    @Override
    public void onMove(CassandraCluster cluster, InetAddress endpoint) {
        validate(cluster).updatePeers();
    }

    @Override
    public void onUp(CassandraCluster cluster, InetAddress endpoint) {
        if (validate(cluster).hasPeer(endpoint)) {
            setPeerAsUp(endpoint);
        } else {
            updatePeer(endpoint);
        }
    }

    @Override
    public void onDown(CassandraCluster cluster, InetAddress endpoint) {
        validate(cluster).setPeerAsDown(endpoint);
    }

    @Override
    public void onCreateKeyspace(CassandraCluster cluster, String keyspace) {
        validate(cluster).updateKeyspace(keyspace);
    }

    @Override
    public void onUpdateKeyspace(CassandraCluster cluster, String keyspace) {
        validate(cluster).updateKeyspace(keyspace);
    }

    @Override
    public void onDropKeyspace(CassandraCluster cluster, String keyspace) {
        validate(cluster).removeKeyspace(keyspace);
    }

    @Override
    public void onCreateTable(CassandraCluster cluster, String keyspace, String table) {
        validate(cluster).updateTable(keyspace, table);
        updateColumns(keyspace, table);
    }

    @Override
    public void onUpdateTable(CassandraCluster cluster, String keyspace, String table) {
        validate(cluster).updateTable(keyspace, table);
        updateColumns(keyspace, table);
    }

    @Override
    public void onDropTable(CassandraCluster cluster, String keyspace, String table) {
        validate(cluster).removeTable(keyspace, table);
    }

    public boolean updateLocal() {
        ResultSet rs = executeLocal("SELECT cluster_name,data_center,host_id,partitioner,rack,release_version,schema_version,tokens FROM system.local WHERE key='local'");
        if (!rs.hasNext()) {
            return false;
        }
        Row row = rs.next();
        setClusterName(row.getString("cluster_name"));
        setPartitioner(row.getString("partitioner"));

        PeerMetadata newPeer = PeerMetadata.newBuilder()
                .setMetadata(this)
                .setAddress(getLocal())
                .setDatacenter(row.getString("data_center"))
                .setRack(row.getString("rack"))
                .setHostId(row.getUUID("host_id"))
                .setSchemaVersion(row.getUUID("schema_version"))
                .setReleaseVersion(row.getString("release_version"))
                .setTokens(row.getSet("tokens", String.class))
                .build();
        addPeer(newPeer);

        return true;
    }

    public void updatePeers() {
        ResultSet rs = executeLocal("SELECT * FROM system.peers");
        for (Row row : rs) {
            boolean down = false;
            PeerMetadata oldPeer = getPeer(row.getInet("peer"));
            if (oldPeer != null) {
                down = oldPeer.isDown();
            }
            addPeer(PeerMetadata.newBuilder().mergeFrom(this, row).setDown(down).build());
        }
    }

    public boolean updatePeer(InetAddress endpoint) {
        ResultSet rs = executeLocal("SELECT * FROM system.peers WHERE peer=?", endpoint);
        if (!rs.hasNext()) {
            return false;
        }
        addPeer(PeerMetadata.newBuilder().mergeFrom(this, rs.next()).build());
        return true;
    }

    public void updateKeyspaces() {
        for (Row row : executeLocal("SELECT * FROM system.schema_keyspaces")) {
            addKeyspace(KeyspaceMetadata.newBuilder().mergeFrom(this, row).build());
        }
    }

    public boolean updateKeyspace(String keyspace) {
        ResultSet rs = executeLocal("SELECT * FROM system.schema_keyspaces WHERE keyspace_name=?", keyspace);
        if (!rs.hasNext()) {
            return false;
        }
        addKeyspace(KeyspaceMetadata.newBuilder().mergeFrom(this, rs.next()).build());
        return true;
    }

    public void updateTables() {
        for (Row row : executeLocal("SELECT * FROM system.schema_columnfamilies")) {
            addTable(TableMetadata.newBuilder().mergeFrom(this, row).build());
        }
    }

    public boolean updateTable(String keyspace, String table) {
        ResultSet rs = executeLocal("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name=? AND columnfamily_name=?", keyspace, table);
        if (!rs.hasNext()) {
            return false;
        }
        addTable(TableMetadata.newBuilder().mergeFrom(this, rs.next()).build());
        return true;
    }

    public void updateColumns() {
        for (Row row : executeLocal("SELECT * FROM system.schema_columns")) {
            addColumn(ColumnMetadata.newBuilder().mergeFrom(this, row).build());
        }
    }

    public void updateColumns(String keyspace, String table) {
        for (Row row : executeLocal("SELECT * FROM system.schema_columns WHERE keyspace_name=? AND columnfamily_name=?", keyspace, table)) {
            addColumn(ColumnMetadata.newBuilder().mergeFrom(this, row).build());
        }
    }

    public void clear() {
        super.clear();
    }

    private ResultSet executeLocal(String query) {
        return cluster.session().statement(query).setConsistency(Consistency.ONE).setRoutingPolicy(routingPolicy).execute();
    }

    private ResultSet executeLocal(String query, Object... values) {
        return cluster.session().statement(query, values).setConsistency(Consistency.ONE).setRoutingPolicy(routingPolicy).execute();
    }

    private MetadataService validate(CassandraCluster cluster) {
        if (!this.cluster.equals(cluster)) {
            throw new IllegalStateException();
        }
        return this;
    }

    static JsonNode valueToJsonNode(Object value) {
        return mapper.valueToTree(value);
    }

    static String valueToJsonString(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    static <E> List<E> convertAsList(String json, Class<E> elementClass) {
        try {
            return mapper.readValue(json, mapper.getTypeFactory().constructCollectionType(List.class, elementClass));
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    static <K, V> Map<K, V> convertAsMap(String json, Class<K> keyClass, Class<V> valueClass) {
        try {
            return mapper.readValue(json, mapper.getTypeFactory().constructMapType(Map.class, keyClass, valueClass));
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    private class LocalRoutingPolicy implements RoutingPolicy {

        @Override
        public boolean isLocal(InetAddress endpoint) {
            return true;
        }

        @Override
        public Iterator<InetAddress> activeEndpoints(AbstractStatement<?> statement) {
            InetAddress local = getLocal();
            if (local == null) {
                return cluster.options().getRoutingPolicy().activeEndpoints(statement);
            }
            return Collections.singletonList(local).iterator();
        }

        @Override
        public void addEndpoint(InetAddress endpoint) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeEndpoint(InetAddress endpoint) {
            throw new UnsupportedOperationException();
        }
    }
}
