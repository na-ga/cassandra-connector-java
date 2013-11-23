package cassandra.routing;

import cassandra.cql.AbstractStatement;
import cassandra.metadata.Metadata;
import cassandra.metadata.PeerMetadata;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import static io.netty.util.internal.PlatformDependent.newConcurrentHashMap;

public class DCAwareRoundRobinPolicy implements RoutingPolicy {

    private final String datacenter;
    private final ConcurrentMap<String, RoundRobinPolicy> dc2Policy;
    private Metadata metadata;

    public DCAwareRoundRobinPolicy(String datacenter) {
        if (datacenter == null) {
            throw new NullPointerException("datacenter");
        }
        if (datacenter.isEmpty()) {
            throw new IllegalArgumentException("empty datacenter");
        }
        this.datacenter = datacenter;
        dc2Policy = newConcurrentHashMap();
        dc2Policy.put(datacenter, new RoundRobinPolicy());
    }

    public String datacenter() {
        return datacenter;
    }

    public void init(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean isLocal(InetAddress endpoint) {
        if (metadata == null) {
            return false;
        }
        PeerMetadata peer = metadata.getPeer(endpoint);
        return peer != null && peer.hasDatacenter() && peer.getDatacenter().equals(datacenter);
    }

    @Override
    public Iterator<InetAddress> activeEndpoints(final AbstractStatement<?> statement) {
        if (metadata == null) {
            metadata = statement.getSession().metadata();
            for (PeerMetadata peer : metadata.getPeers()) {
                add(peer);
            }
        }
        return new Iterator<InetAddress>() {

            private final Iterator<InetAddress> local = dc2Policy.get(datacenter).activeEndpoints(statement);
            private final Iterator<String> remotedc = dc2Policy.keySet().iterator();
            private Iterator<InetAddress> current = local;

            @Override
            public boolean hasNext() {
                for (;;) {
                    if (current.hasNext()) {
                        return true;
                    }
                    if (!remotedc.hasNext()) {
                        return false;
                    }
                    while (remotedc.hasNext()) {
                        String nextdc = remotedc.next();
                        if (!nextdc.equals(datacenter)) {
                            current = dc2Policy.get(nextdc).activeEndpoints(statement);
                            break;
                        }
                    }
                }
            }

            @Override
            public InetAddress next() {
                return current.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void addEndpoint(InetAddress endpoint) {
        if (metadata != null) {
            add(metadata.getPeer(endpoint));
        }
    }

    @Override
    public void removeEndpoint(InetAddress endpoint) {
        for (RoutingPolicy policy : dc2Policy.values()) {
            policy.removeEndpoint(endpoint);
        }
    }

    private void add(PeerMetadata peer) {
        if (metadata != null) {
            if (peer != null && peer.hasDatacenter()) {
                String dc = peer.getDatacenter();
                RoundRobinPolicy policy = dc2Policy.get(dc);
                if (policy == null) {
                    RoundRobinPolicy newPolicy = new RoundRobinPolicy();
                    policy = dc2Policy.putIfAbsent(dc, newPolicy);
                    if (policy == null) {
                        policy = newPolicy;
                    }
                }
                policy.addEndpoint(peer.getAddress());
            }
        }
    }
}
