package cassandra.routing;

import cassandra.cql.AbstractStatement;
import cassandra.cql.RoutingKey;
import cassandra.metadata.Metadata;
import cassandra.metadata.Partitioner;
import cassandra.metadata.ReplicationStrategy;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;

public class TokenAwareRoutingPolicy extends RoutingPolicy.Wrapper {

    public TokenAwareRoutingPolicy(RoutingPolicy routingPolicy) {
        super(routingPolicy);
    }

    @Override
    public boolean isLocal(InetAddress endpoint) {
        return routingPolicy.isLocal(endpoint);
    }

    @Override
    public Iterator<InetAddress> activeEndpoints(final AbstractStatement<?> statement) {
        String keyspace = statement.getKeyspace();
        RoutingKey routingKey = statement.getRoutingKey();
        if (keyspace != null && routingKey != null) {
            Metadata metadata = statement.getSession().metadata();
            Partitioner partitioner = metadata.getPartitioner();
            Partitioner.Token token = partitioner.getToken(routingKey.asByteBuffer());
            ReplicationStrategy replicationStrategy = metadata.getKeyspace(keyspace).getReplicationStrategy();
            final List<InetAddress> replicas = replicationStrategy.calculateNaturalEndpoints(token);
            if (!replicas.isEmpty()) {
                return new Iterator<InetAddress>() {

                    private final Iterator<InetAddress> parent = replicas.iterator();
                    private Iterator<InetAddress> child;
                    private InetAddress next;

                    @Override
                    public boolean hasNext() {
                        if (parent.hasNext()) {
                            InetAddress endpoint = parent.next();
                            if (isLocal(endpoint)) {
                                next = endpoint;
                                return true;
                            }
                        }
                        if (child == null) {
                            child = routingPolicy.activeEndpoints(statement);
                        }
                        while (child.hasNext()) {
                            InetAddress endpoint = child.next();
                            if (!replicas.contains(endpoint) && !isLocal(endpoint)) {
                                next = endpoint;
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public InetAddress next() {
                        InetAddress endpoint = null;
                        if (next != null) {
                            endpoint = next;
                            next = null;
                        }
                        return endpoint;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }
        return routingPolicy.activeEndpoints(statement);
    }

    @Override
    public void addEndpoint(InetAddress endpoint) {
        routingPolicy.addEndpoint(endpoint);
    }

    @Override
    public void removeEndpoint(InetAddress endpoint) {
        routingPolicy.removeEndpoint(endpoint);
    }
}
