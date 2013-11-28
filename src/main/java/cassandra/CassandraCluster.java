package cassandra;

import cassandra.cql.PreparedStatement;
import cassandra.metadata.Metadata;
import cassandra.metadata.MetadataService;
import cassandra.metadata.PeerMetadata;
import cassandra.protocol.CassandraMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.util.internal.PlatformDependent.newConcurrentHashMap;

public class CassandraCluster {

    public static interface EventListener {

        void onJoinCluster(CassandraCluster cluster, InetAddress endpoint);

        void onLeaveCluster(CassandraCluster cluster, InetAddress endpoint);

        void onMove(CassandraCluster cluster, InetAddress endpoint);

        void onUp(CassandraCluster cluster, InetAddress endpoint);

        void onDown(CassandraCluster cluster, InetAddress endpoint);

        void onCreateKeyspace(CassandraCluster cluster, String keyspace);

        void onCreateTable(CassandraCluster cluster, String keyspace, String table);

        void onUpdateKeyspace(CassandraCluster cluster, String keyspace);

        void onUpdateTable(CassandraCluster cluster, String keyspace, String table);

        void onDropKeyspace(CassandraCluster cluster, String keyspace);

        void onDropTable(CassandraCluster cluster, String keyspace, String table);
    }

    private static final Logger logger = LoggerFactory.getLogger(CassandraCluster.class);
    private static final RuntimeException unavailable = new RuntimeException("no available peers");

    private Client client;

    public static class Builder {

        protected CassandraOptions.Builder options;
        protected CassandraDriver driver;
        protected List<InetAddress> seeds = new ArrayList<InetAddress>();
        protected List<EventListener> listeners;

        public CassandraOptions.Builder getOptions() {
            if (options == null) {
                options = CassandraOptions.newBuilder();
            }
            return options;
        }

        public Builder setOptions(CassandraOptions.Builder options) {
            getOptions().mergeFrom(options);
            return this;
        }

        public Builder setOptions(CassandraOptions options) {
            getOptions().mergeFrom(options);
            return this;
        }

        public Builder setDriver(CassandraDriver driver) {
            this.driver = driver;
            return this;
        }

        public Builder addSeeds(String... seeds) {
            if (seeds == null) {
                throw new NullPointerException("seeds");
            }
            for (String seed : seeds) {
                addSeed(seed);
            }
            return this;
        }

        public Builder addSeed(String seed) {
            try {
                return addSeed(InetAddress.getByName(seed));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(String.format("invalid host %s", seed));
            }
        }

        public Builder addSeeds(InetAddress... seeds) {
            if (seeds == null) {
                throw new NullPointerException("seeds");
            }
            for (InetAddress seed : seeds) {
                addSeed(seed);
            }
            return this;
        }

        public Builder addSeed(InetAddress seed) {
            seeds.add(seed);
            return this;
        }

        public Builder addEventListener(EventListener listener) {
            if (listener == null) {
                throw new NullPointerException("listener");
            }
            if (listeners == null) {
                listeners = new ArrayList<EventListener>();
            }
            listeners.add(listener);
            return this;
        }

        public Builder addEventListeners(EventListener... listeners) {
            if (listeners == null) {
                throw new NullPointerException("listeners");
            }
            for (EventListener listener : listeners) {
                addEventListener(listener);
            }
            return this;
        }

        public CassandraCluster build() {
            if (seeds.isEmpty()) {
                throw new IllegalStateException("empty seeds");
            }

            if (driver == null) {
                logger.debug("driver is not set. creating new driver.");
                setDriver(new CassandraDriver());
            }
            return new CassandraCluster(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private CassandraCluster(Builder builder) {
        client = new Client(builder);
        if (!client.startDiscovery()) {
            throw unavailable;
        }
        client.driver().addShutdownHook(this);
    }

    public boolean isActive() {
        return client.isActive();
    }

    public CassandraDriver driver() {
        return client.driver();
    }

    public CassandraOptions options() {
        return client.options();
    }

    public CassandraSession session() {
        return client.session();
    }

    public CassandraSession session(String keyspace) {
        return client.session(keyspace);
    }

    public Collection<InetAddress> seeds() {
        return client.seeds();
    }

    public Metadata metadata() {
        return client.metadata();
    }

    public CassandraCluster addEventListener(EventListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener");
        }
        client.listeners.add(listener);
        return this;
    }

    public CassandraCluster addEventListeners(EventListener... listeners) {
        if (listeners == null) {
            throw new NullPointerException("listeners");
        }
        for (EventListener listener : listeners) {
            addEventListener(listener);
        }
        return this;
    }




    public void close() {
        client.close();
    }

    class Client implements CassandraConnection.StateListener {

        private final CassandraOptions options;
        private final CassandraDriver driver;
        private final Set<InetAddress> seeds;
        private final List<EventListener> listeners;
        private final CassandraSession session;
        private final ConcurrentMap<String, CassandraSession> sessions;
        private final MetadataService metadata;
        private final AtomicReference<CassandraConnection> connection;
        private final ConcurrentMap<Integer, CassandraConnection> connections;
        private final ConcurrentMap<PreparedStatement.StatementId, String> pstmts;
        private final AtomicBoolean active;

        private Client(Builder builder) {
            options = builder.getOptions().build();
            driver = builder.driver;
            seeds = Collections.unmodifiableSet(new LinkedHashSet<InetAddress>(builder.seeds));
            session = new CassandraSession(this);
            sessions = newConcurrentHashMap();
            metadata = new MetadataService(CassandraCluster.this);
            listeners = new CopyOnWriteArrayList<EventListener>();
            listeners.add(metadata);
            if (builder.listeners != null) {
                listeners.addAll(builder.listeners);
            }
            connection = new AtomicReference<CassandraConnection>(null);
            connections = newConcurrentHashMap();
            pstmts = newConcurrentHashMap();
            active = new AtomicBoolean();
        }

        public boolean isActive() {
            return !driver.isShutdown() && active.get();
        }

        public CassandraDriver driver() {
            return driver;
        }

        public CassandraOptions options() {
            return options;
        }

        public CassandraSession session() {
            return session;
        }

        public CassandraSession session(String keyspace) {
            if (keyspace == null) {
                throw new NullPointerException("keyspace");
            }
            if (keyspace.isEmpty()) {
                throw new IllegalArgumentException("empty keyspace");
            }
            CassandraSession session = sessions.get(keyspace);
            if (session == null) {
                CassandraSession newSession = new CassandraSession(this, keyspace);
                session = sessions.putIfAbsent(keyspace, newSession);
                if (session == null) {
                    session = newSession;
                }
            }
            return session;
        }

        public Collection<InetAddress> seeds() {
            return seeds;
        }

        public Metadata metadata() {
            return metadata;
        }

        public boolean startDiscovery() {
            if (active.compareAndSet(false, true)) {
                logger.info("starting discovery cluster - seeds{} (port:{})", seeds, options.getPort());
                metadata.clear();
                boolean registered = registerConnection(seeds.iterator());
                if (registered) {
                    CassandraConnection connection = this.connection.get();
                    InetSocketAddress seed = connection.remoteAddress();
                    fireTopologyChanged(CassandraMessage.Event.TopologyChange.newNode(seed));
                    for (InetAddress peer : metadata.selectPeers()) {
                        InetSocketAddress address = new InetSocketAddress(peer, options.getPort());
                        fireTopologyChanged(CassandraMessage.Event.TopologyChange.newNode(address));
                    }
                    for (PeerMetadata peer : metadata.getPeers()) {
                        CassandraConnection tmp = driver.newConnection(peer.getAddress(), options).open(DEFAULT);
                        tmp.openFuture().await();
                        if (!tmp.isActive()) {
                            fireStatusChanged(CassandraMessage.Event.StatusChange.down(tmp.remoteAddress()));
                        }
                        tmp.close();
                    }
                }
                active.set(registered && metadata.isInitialized());
            }
            return active.get();
        }

        public boolean registerConnection(Iterator<InetAddress> addresses) {
            if (!addresses.hasNext()) {
                return false;
            }
            if (!isActive()) {
                return false;
            }
            CassandraConnection connection = this.connection.get();
            if (connection != null && connection.isActive()) {
                if (connection.isRegistered()) {
                    return true;
                } else {
                    connection.close();
                }
            }
            this.connection.set(null);
            InetAddress address = addresses.next();
            logger.info("trying to register for cluster events - {}", address);
            try {
                boolean sync = true;
                connection = driver.newConnection(address, options).open(this);
                if (connection.isActive() && this.connection.compareAndSet(null, connection)) {
                    connection.register(sync);
                }
                if (connection.isRegistered()) {
                    metadata.setLocal(address);
                    return true;
                }
                return registerConnection(addresses);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        public String findPreparedQuery(PreparedStatement.StatementId id) {
            return pstmts.get(id);
        }

        public PreparedStatement registerPreparedQuery(CassandraConnection connection, PreparedStatement pstmt) {
            if (pstmts.putIfAbsent(pstmt.getId(), pstmt.getQuery()) == null) {
                for (CassandraConnection c : connections.values()) {
                    if (c.equals(connection)) {
                        continue;
                    }
                    c.send(new CassandraMessage.Prepare(pstmt.getQuery()));
                }
            }
            return pstmt;
        }

        public void close() {
            if (active.compareAndSet(true, false)) {
                logger.info("closing cluster - {}", metadata.getClusterName());
                session.close();
                for (CassandraSession session : sessions.values()) {
                    session.close();
                }
                sessions.clear();
            }
        }

        @Override
        public void onOpen(CassandraConnection connection) {
            logger.debug("OPEN(cluster={}, address={})", metadata.getClusterName(), connection.remoteAddress());
            connections.putIfAbsent(connection.hashCode(), connection);
            for (String query : pstmts.values()) {
                connection.send(new CassandraMessage.Prepare(query));
            }
        }

        @Override
        public void onOpenFail(CassandraConnection connection, Throwable cause) {
            if (isActive()) {
                logger.debug("OPEN FAIL(cluster={}, address={}, cause={})", metadata.getClusterName(), connection.remoteAddress(), cause.getMessage(), cause);
            }
        }

        @Override
        public void onClose(CassandraConnection connection) {
            logger.debug("CLOSE(cluster={}, address={})", metadata.getClusterName(), connection.remoteAddress());
            connections.remove(connection.hashCode());
        }

        @Override
        public void onRegister(CassandraConnection connection, List<CassandraMessage.Event.Type> events) {
            logger.debug("REGISTER(cluster={}, address={})", metadata.getClusterName(), connection.remoteAddress());
        }

        @Override
        public void onRegisterFail(CassandraConnection connection, List<CassandraMessage.Event.Type> events, Throwable cause) {
            logger.debug("REGISTER FAIL(cluster={}, address={}, cause={})", metadata.getClusterName(), connection.remoteAddress(), cause.getMessage(), cause);
        }

        @Override
        public void onUnregister(CassandraConnection connection) {
            logger.debug("UNREGISTER(cluster={}, address={})", metadata.getClusterName(), connection.remoteAddress());
            if (isActive() && this.connection.get().equals(connection)) {
                Set<InetAddress> addressSet = new LinkedHashSet<InetAddress>();
                addressSet.addAll(seeds);
                for (PeerMetadata peer : metadata.getPeers()) {
                    if (peer.isUp()) {
                        addressSet.add(peer.getAddress());
                    }
                }
                if (!registerConnection(addressSet.iterator())) {
                    if (isActive()) {
                        logger.error(unavailable.getMessage(), unavailable);
                        throw unavailable;
                    }
                }
            }
        }

        @Override
        public void onEvent(CassandraConnection connection, CassandraMessage.Event event) {
            logger.debug("EVENT(cluster={}, address={}, event={})", metadata.getClusterName(), connection.remoteAddress(), event);
            switch (event.type) {
                case TOPOLOGY_CHANGE:
                    fireTopologyChanged((CassandraMessage.Event.TopologyChange)event);
                    break;
                case STATUS_CHANGE:
                    fireStatusChanged((CassandraMessage.Event.StatusChange)event);
                    break;
                case SCHEMA_CHANGE:
                    fireSchemaChanged((CassandraMessage.Event.SchemaChange)event);
                    break;
                default:
                    break;
            }
        }

        private void fireTopologyChanged(CassandraMessage.Event.TopologyChange event) {
            switch (event.change) {
                case NEW_NODE:
                    options.getRoutingPolicy().addEndpoint(event.node.getAddress());
                    for (EventListener listener : listeners) {
                        listener.onJoinCluster(CassandraCluster.this, event.node.getAddress());
                    }
                    break;
                case REMOVED_NODE:
                    options.getRoutingPolicy().removeEndpoint(event.node.getAddress());
                    for (EventListener listener : listeners) {
                        listener.onLeaveCluster(CassandraCluster.this, event.node.getAddress());
                    }
                    break;
                case MOVED_NODE:
                    options.getRoutingPolicy().removeEndpoint(event.node.getAddress());
                    for (EventListener listener : listeners) {
                        listener.onMove(CassandraCluster.this, event.node.getAddress());
                    }
                    options.getRoutingPolicy().addEndpoint(event.node.getAddress());
                    break;
                default:
                    break;
            }
        }

        private void fireStatusChanged(CassandraMessage.Event.StatusChange event) {
            switch (event.status) {
                case UP:
                    options.getRoutingPolicy().addEndpoint(event.node.getAddress());
                    for (EventListener listener : listeners) {
                        listener.onUp(CassandraCluster.this, event.node.getAddress());
                    }
                    break;
                case DOWN:
                    options.getRoutingPolicy().removeEndpoint(event.node.getAddress());
                    for (EventListener listener : listeners) {
                        listener.onDown(CassandraCluster.this, event.node.getAddress());
                    }
                    break;
                default:
                    break;
            }
        }

        private void fireSchemaChanged(CassandraMessage.Event.SchemaChange event) {
            boolean emptyTable = event.table.isEmpty();
            switch (event.change) {
                case CREATED:
                    for (EventListener listener : listeners) {
                        if (emptyTable) {
                            listener.onCreateKeyspace(CassandraCluster.this, event.keyspace);
                        } else {
                            listener.onCreateTable(CassandraCluster.this, event.keyspace, event.table);
                        }
                    }
                    break;
                case UPDATED:
                    for (EventListener listener : listeners) {
                        if (emptyTable) {
                            listener.onUpdateKeyspace(CassandraCluster.this, event.keyspace);
                        } else {
                            listener.onUpdateTable(CassandraCluster.this, event.keyspace, event.table);
                        }
                    }
                    break;
                case DROPPED:
                    for (EventListener listener : listeners) {
                        if (emptyTable) {
                            listener.onDropKeyspace(CassandraCluster.this, event.keyspace);
                        } else {
                            listener.onDropTable(CassandraCluster.this, event.keyspace, event.table);
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
