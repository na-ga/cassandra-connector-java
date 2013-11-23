package cassandra.routing;

import cassandra.cql.AbstractStatement;

import java.net.InetAddress;
import java.util.Iterator;

public interface RoutingPolicy {

    boolean isLocal(InetAddress endpoint);

    Iterator<InetAddress> activeEndpoints(AbstractStatement<?> statement);

    void addEndpoint(InetAddress endpoint);

    void removeEndpoint(InetAddress endpoint);

    public static abstract class Wrapper implements RoutingPolicy {

        protected final RoutingPolicy routingPolicy;

        protected Wrapper(RoutingPolicy routingPolicy) {
            if (routingPolicy == null) {
                throw new NullPointerException("routingPolicy");
            }
            this.routingPolicy = routingPolicy;
        }
    }
}
