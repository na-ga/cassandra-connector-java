package cassandra.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.netty.util.internal.PlatformDependent.newConcurrentHashMap;

public abstract class Metadata extends MetadataEntity {

    private static final ColumnComparator COLUMN_COMPARATOR = new ColumnComparator();
    private static final Iterator<Map.Entry<Partitioner.Token, InetAddress>> EMPTY_RING_ITERATOR = Collections.<Map.Entry<Partitioner.Token, InetAddress>>emptyList().iterator();

    private InetAddress local;
    private String clusterName;
    private String partitioner;
    private final ConcurrentMap<InetAddress, PeerMetadata> peers;
    private final ConcurrentMap<String, KeyspaceMetadata> keyspaces;
    private final ConcurrentMap<String, ConcurrentMap<String, TableMetadata>> tables;
    private final ConcurrentMap<String, ConcurrentMap<String, ColumnMetadata>> columns;
    private final ConcurrentMap<String, CopyOnWriteArrayList<InetAddress>> dc2Endpoints;
    private final ConcurrentMap<String, ConcurrentMap<String, CopyOnWriteArrayList<InetAddress>>> rack2Endpoints;
    private final ConcurrentSkipListMap<Partitioner.Token, InetAddress> tokenring;

    protected Metadata() {
        peers = newConcurrentHashMap();
        keyspaces = newConcurrentHashMap();
        tables = newConcurrentHashMap();
        columns = newConcurrentHashMap();
        dc2Endpoints = newConcurrentHashMap();
        rack2Endpoints = newConcurrentHashMap();
        tokenring = new ConcurrentSkipListMap<Partitioner.Token, InetAddress>();
    }

    @JsonIgnore
    protected InetAddress getLocal() {
        return local;
    }

    protected void setLocal(InetAddress local) {
        this.local = local;
    }

    public String getClusterName() {
        return clusterName;
    }

    protected void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @JsonProperty("partitioner")
    public String getPartitionerClass() {
        return partitioner;
    }

    @JsonIgnore
    public Partitioner getPartitioner() {
        return Partitioner.getPartitioner(partitioner);
    }

    protected void setPartitioner(String partitioner) {
        this.partitioner = partitioner;
    }

    @JsonProperty("data_center")
    public String getDatacenter() {
        if (!hasPeer(local)) {
            return null;
        }
        return getPeer(local).getDatacenter();
    }

    public String getRack() {
        if (!hasPeer(local)) {
            return null;
        }
        return getPeer(local).getRack();
    }

    public UUID getHostId() {
        if (!hasPeer(local)) {
            return null;
        }
        return getPeer(local).getHostId();
    }

    public UUID getSchemaVersion() {
        if (!hasPeer(local)) {
            return null;
        }
        return getPeer(local).getSchemaVersion();
    }

    @JsonIgnore
    public Set<String> getTokens() {
        if (!hasPeer(local)) {
            return null;
        }
        return getPeer(local).getTokens();
    }

    public String getReleaseVersion() {
        if (!hasPeer(local)) {
            return null;
        }
        return getPeer(local).getReleaseVersion();
    }

    @JsonIgnore
    public Map<String, CopyOnWriteArrayList<InetAddress>> getDatacenterEndpoints() {
        return dc2Endpoints;
    }

    @JsonIgnore
    public Map<String, ConcurrentMap<String, CopyOnWriteArrayList<InetAddress>>> getDatacenterRacks() {
        return rack2Endpoints;
    }

    @JsonIgnore
    public Iterator<Map.Entry<Partitioner.Token, InetAddress>> getRingIterator(Partitioner.Token start) {
        if (tokenring.isEmpty()) {
            return EMPTY_RING_ITERATOR;
        }
        Partitioner.Token startToken = tokenring.ceilingKey(start);
        if (startToken == null) {
            startToken = tokenring.firstKey();
        }
        return tokenring.tailMap(startToken).entrySet().iterator();
    }

    @JsonIgnore
    public int getRingSize() {
        return tokenring.size();
    }

    @JsonIgnore
    public List<PeerMetadata> getPeers() {
        return new ArrayList<PeerMetadata>(peers.values());
    }

    @JsonIgnore
    public boolean hasPeer(InetAddress endpoint) {
        return getPeer(endpoint) != null;
    }

    @JsonIgnore
    public PeerMetadata getPeer(InetAddress endpoint) {
        return peers.get(endpoint);
    }

    protected void addPeer(PeerMetadata peer) {
        peers.put(peer.getAddress(), peer);
        if (peer.hasDatacenter()) {
            CopyOnWriteArrayList<InetAddress> dc2EndpointList = dc2Endpoints.get(peer.getDatacenter());
            if (dc2EndpointList == null) {
                CopyOnWriteArrayList<InetAddress> newDc2EndpointList = new CopyOnWriteArrayList<InetAddress>();
                dc2EndpointList = dc2Endpoints.putIfAbsent(peer.getDatacenter(), newDc2EndpointList);
                if (dc2EndpointList == null) {
                    dc2EndpointList = newDc2EndpointList;
                }
            }
            dc2EndpointList.addIfAbsent(peer.getAddress());
            if (peer.hasRack()) {
                ConcurrentMap<String, CopyOnWriteArrayList<InetAddress>> rack2EndpointMap = rack2Endpoints.get(peer.getDatacenter());
                if (rack2EndpointMap == null) {
                    ConcurrentMap<String, CopyOnWriteArrayList<InetAddress>> newRack2EndpointMap = newConcurrentHashMap();
                    rack2EndpointMap = rack2Endpoints.putIfAbsent(peer.getDatacenter(), newRack2EndpointMap);
                    if (rack2EndpointMap == null) {
                        rack2EndpointMap = newRack2EndpointMap;
                    }
                }
                CopyOnWriteArrayList<InetAddress> rack2EndpointList = rack2EndpointMap.get(peer.getRack());
                if (rack2EndpointList == null) {
                    CopyOnWriteArrayList<InetAddress> newRack2EndpointList = new CopyOnWriteArrayList<InetAddress>();
                    rack2EndpointList = rack2EndpointMap.putIfAbsent(peer.getRack(), newRack2EndpointList);
                    if (rack2EndpointList == null) {
                        rack2EndpointList = newRack2EndpointList;
                    }
                }
                rack2EndpointList.add(peer.getAddress());
            }
        }
        for (String tokenString : peer.getTokens()) {
            Partitioner partitioner = getPartitioner();
            if (partitioner != null) {
                Partitioner.Token token = partitioner.getToken(tokenString);
                tokenring.put(token, peer.getAddress());
            }
        }
    }

    protected void setPeerAsUp(InetAddress endpoint) {
        PeerMetadata peer = peers.get(endpoint);
        if (peer != null && peer.isDown()) {
            PeerMetadata up = PeerMetadata.newBuilder().mergeFrom(peer).setDown(false).build();
            peers.put(endpoint, up);
        }
    }

    protected void setPeerAsDown(InetAddress endpoint) {
        PeerMetadata peer = peers.get(endpoint);
        if (peer != null && peer.isUp()) {
            PeerMetadata down = PeerMetadata.newBuilder().mergeFrom(peer).setDown(true).build();
            peers.put(endpoint, down);
        }
    }

    protected void removePeer(InetAddress endpoint) {
        peers.remove(endpoint);
        for (List<InetAddress> endpoints : dc2Endpoints.values()) {
            endpoints.remove(endpoint);
        }
        for (Map<String, CopyOnWriteArrayList<InetAddress>> map : rack2Endpoints.values()) {
            for (List<InetAddress> endpoints : map.values()) {
                endpoints.remove(endpoint);
            }
        }
        Iterator<InetAddress> iterator = tokenring.values().iterator();
        while (iterator.hasNext()) {
            InetAddress ep = iterator.next();
            if (endpoint.equals(ep)) {
                iterator.remove();
            }
        }
    }

    @JsonIgnore
    public List<KeyspaceMetadata> getKeyspaces() {
        return new ArrayList<KeyspaceMetadata>(keyspaces.values());
    }

    @JsonIgnore
    public boolean hasKeyspace(String keyspace) {
        return getKeyspace(keyspace) != null;
    }

    @JsonIgnore
    public KeyspaceMetadata getKeyspace(String keyspace) {
        return keyspaces.get(keyspace);
    }

    protected void addKeyspace(KeyspaceMetadata keyspace) {
        keyspaces.put(keyspace.getName(), keyspace);
    }

    protected void removeKeyspace(String keyspace) {
        keyspaces.remove(keyspace);
        tables.remove(keyspace);
    }

    @JsonIgnore
    public List<TableMetadata> getTables() {
        List<TableMetadata> list = new ArrayList<TableMetadata>();
        for (Map<String, TableMetadata> map : tables.values()) {
            list.addAll(map.values());
        }
        return list;
    }

    @JsonIgnore
    public List<TableMetadata> getTables(String keyspace) {
        Map<String, TableMetadata> map = tables.get(keyspace);
        if (map == null) {
            return Collections.emptyList();
        }
        return new ArrayList<TableMetadata>(map.values());
    }

    @JsonIgnore
    public boolean hasTable(String keyspace, String table) {
        return getTable(keyspace, table) != null;
    }

    @JsonIgnore
    public TableMetadata getTable(String keyspace, String table) {
        Map<String, TableMetadata> map = tables.get(keyspace);
        if (map == null) {
            return null;
        }
        return map.get(table);
    }

    protected void addTable(TableMetadata table) {
        String keyspace = table.getKeyspaceName();
        if (hasKeyspace(keyspace)) {
            Map<String, TableMetadata> map = tables.get(keyspace);
            if (map == null) {
                ConcurrentMap<String, TableMetadata> newmap = newConcurrentHashMap();
                map = tables.putIfAbsent(keyspace, newmap);
                if (map == null) {
                    map = newmap;
                }
            }
            map.put(table.getName(), table);
        }
    }

    protected void removeTable(String keyspace, String table) {
        Map<String, TableMetadata> map = tables.get(keyspace);
        if (map != null) {
            map.remove(table);
        }
        removeColumns(keyspace, table);
    }

    @JsonIgnore
    public List<ColumnMetadata> getColumns(String keyspace, String table) {
        Map<String, ColumnMetadata> map = columns.get(globalTable(keyspace, table));
        if (map == null) {
            return Collections.emptyList();
        }
        List<ColumnMetadata> list = new ArrayList<ColumnMetadata>(map.values());
        Collections.sort(list, COLUMN_COMPARATOR);
        return list;
    }

    @JsonIgnore
    public boolean hasColumn(String keyspace, String table, String column) {
        return getColumn(keyspace, table, column) != null;
    }

    @JsonIgnore
    public ColumnMetadata getColumn(String keyspace, String table, String column) {
        Map<String, ColumnMetadata> map = columns.get(globalTable(keyspace, table));
        if (map == null) {
            return null;
        }
        return map.get(column);
    }

    protected void addColumn(ColumnMetadata column) {
        String keyspace = column.getKeyspace();
        String table = column.getTable();
        if (hasTable(keyspace, table)) {
            String globalTalble = globalTable(keyspace, table);
            Map<String, ColumnMetadata> map = columns.get(globalTalble);
            if (map == null) {
                ConcurrentMap<String, ColumnMetadata> newmap = newConcurrentHashMap();
                map = columns.putIfAbsent(globalTalble, newmap);
                if (map == null) {
                    map = newmap;
                }
            }
            map.put(column.getName(), column);
        }
    }

    protected void addColumns(ColumnMetadata... columns) {
        for (ColumnMetadata column : columns) {
            addColumn(column);
        }
    }

    protected void removeColumn(String keyspace, String table, String column) {
        String globalTable = globalTable(keyspace, table);
        Map<String, ColumnMetadata> map = columns.get(globalTable);
        if (map != null) {
            map.remove(column);
        }
    }

    protected void removeColumns(String keyspace, String table) {
        columns.remove(globalTable(keyspace, table));
    }

    protected void clear() {
        local = null;
        clusterName = null;
        partitioner = null;
        peers.clear();
        keyspaces.clear();
        tables.clear();
        columns.clear();
        dc2Endpoints.clear();
        rack2Endpoints.clear();
        tokenring.clear();
    }

    protected static String globalTable(String keyspace, String table) {
        return String.format("%s.%s", keyspace, table);
    }

    private static class ColumnComparator implements Comparator<ColumnMetadata> {

        @Override
        public int compare(ColumnMetadata c1, ColumnMetadata c2) {
            return ColumnMetadata.compareTo(c1, c2);
        }
    }
}
