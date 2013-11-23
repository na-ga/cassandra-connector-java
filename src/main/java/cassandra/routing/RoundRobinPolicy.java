package cassandra.routing;

import cassandra.cql.AbstractStatement;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

public class RoundRobinPolicy implements RoutingPolicy {

    private final List<InetAddress> activeEndpoints;
    private volatile int counter;

    public RoundRobinPolicy() {
        this(new CopyOnWriteArrayList<InetAddress>());
    }

    public RoundRobinPolicy(List<InetAddress> activeEndpoints) {
        if (activeEndpoints == null) {
            throw new NullPointerException("activeEndpoints");
        }
        this.activeEndpoints = activeEndpoints;
        counter = new Random().nextInt(997);
    }

    @Override
    public boolean isLocal(InetAddress endpoint) {
        return true;
    }

    @Override
    public Iterator<InetAddress> activeEndpoints(AbstractStatement<?> statement) {
        return new Iterator<InetAddress>() {

            private int start = counter++;
            private int remaining = activeEndpoints.size();

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public InetAddress next() {
                if (!hasNext()) {
                    return null;
                }
                remaining--;
                int index = start++ % activeEndpoints.size();
                if (index < 0) {
                    index += activeEndpoints.size();
                }
                return activeEndpoints.get(index);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void addEndpoint(InetAddress endpoint) {
        if (!activeEndpoints.contains(endpoint)) {
            activeEndpoints.add(endpoint);
        }
    }

    @Override
    public void removeEndpoint(InetAddress endpoint) {
        activeEndpoints.remove(endpoint);
    }
}
