package cassandra.metadata;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class ReplicationStrategy {

    public static ReplicationStrategy getReplicationStrategy(KeyspaceMetadata keyspace) {
        String strategyClass = keyspace.getStrategyClass();
        if (strategyClass.endsWith(".SimpleStrategy")) {
            return new SimpleStrategy(keyspace);
        } else if (strategyClass.endsWith(".NetworkTopologyStrategy")) {
            return new NetworkTopologyStrategy(keyspace);
        } else if (strategyClass.endsWith(".OldNetworkTopologyStrategy")) {
            return new OldNetworkTopologyStrategy(keyspace);
        } else if (strategyClass.endsWith(".LocalStrategy")) {
            return new LocalStrategy(keyspace);
        }
        return null;
    }

    protected final KeyspaceMetadata keyspace;

    protected ReplicationStrategy(KeyspaceMetadata keyspace) {
        if (keyspace == null) {
            throw new NullPointerException("keyspace");
        }
        this.keyspace = keyspace;
    }

    public KeyspaceMetadata getKeyspace() {
        return keyspace;
    }

    public String getStrategyClass() {
        return keyspace.getStrategyClass();
    }

    public Map<String, String> getStrategyOptions() {
        return keyspace.getStrategyOptions();
    }

    public abstract int getReplicationFactor();

    public abstract List<InetAddress> calculateNaturalEndpoints(Partitioner.Token token);

    public static class SimpleStrategy extends ReplicationStrategy {

        public SimpleStrategy(KeyspaceMetadata keyspace) {
            super(keyspace);
        }

        @Override
        public int getReplicationFactor() {
            return Integer.parseInt(keyspace.getStrategyOptions().get("replication_factor"));
        }

        @Override
        public List<InetAddress> calculateNaturalEndpoints(Partitioner.Token token) {
            Metadata metadata = keyspace.getMetadata();
            if (metadata.getRingSize() == 0) {
                return Collections.emptyList();
            }
            int replicas = getReplicationFactor();
            List<InetAddress> endpoints = new ArrayList<InetAddress>(replicas);
            Iterator<Map.Entry<Partitioner.Token, InetAddress>> iterator = metadata.getRingIterator(token);
            while (endpoints.size() < replicas && iterator.hasNext()) {
                Map.Entry<Partitioner.Token, InetAddress> entry = iterator.next();
                InetAddress endpoint = entry.getValue();
                if (!endpoints.contains(endpoint)) {
                    endpoints.add(endpoint);
                }
            }
            return endpoints;
        }
    }

    public static class NetworkTopologyStrategy extends ReplicationStrategy {

        private final Map<String, Integer> replicationFactors;

        public NetworkTopologyStrategy(KeyspaceMetadata keyspace) {
            super(keyspace);
            replicationFactors = new HashMap<String, Integer>();
            for (Map.Entry<String, String> entry : getStrategyOptions().entrySet()) {
                String datacenter = entry.getKey();
                Integer replicationFactor = Integer.valueOf(entry.getValue());
                replicationFactors.put(datacenter, replicationFactor);
            }
        }

        @Override
        public int getReplicationFactor() {
            int total = 0;
            for (int repFactor : replicationFactors.values()) {
                total += repFactor;
            }
            return total;
        }

        public int getReplicationFactor(String dc) {
            Integer replicas = replicationFactors.get(dc);
            return replicas == null ? 0 : replicas;
        }

        @Override
        public List<InetAddress> calculateNaturalEndpoints(Partitioner.Token token) { // TODO refactor this
            Metadata metadata = keyspace.getMetadata();
            Set<InetAddress> replicas = new LinkedHashSet<InetAddress>();
            Map<String, Set<InetAddress>> dcReplicas = new HashMap<String, Set<InetAddress>>(replicationFactors.size());
            Map<String, Set<String>> seenRacks = new HashMap<String, Set<String>>(replicationFactors.size());
            Map<String, Set<InetAddress>> skippedDcEndpoints = new HashMap<String, Set<InetAddress>>(replicationFactors.size());
            for (Map.Entry<String, Integer> dc : replicationFactors.entrySet()) {
                dcReplicas.put(dc.getKey(), new HashSet<InetAddress>(dc.getValue()));
                seenRacks.put(dc.getKey(), new HashSet<String>());
                skippedDcEndpoints.put(dc.getKey(), new LinkedHashSet<InetAddress>());
            }
            Map<String, CopyOnWriteArrayList<InetAddress>> allEndpoints = metadata.getDatacenterEndpoints();
            Map<String, ConcurrentMap<String, CopyOnWriteArrayList<InetAddress>>> racks = metadata.getDatacenterRacks();
            Iterator<Map.Entry<Partitioner.Token, InetAddress>> iterator = metadata.getRingIterator(token);
            while (iterator.hasNext() && !hasSufficientReplicas(dcReplicas, allEndpoints)) {
                Map.Entry<Partitioner.Token, InetAddress> entry = iterator.next();
                PeerMetadata peer = metadata.getPeer(entry.getValue());
                if (peer == null || !peer.hasDatacenter()) {
                    continue;
                }
                InetAddress ep = peer.getAddress();
                String dc = peer.getDatacenter();
                if (!replicationFactors.containsKey(dc) || hasSufficientReplicas(dc, dcReplicas, allEndpoints)) {
                    continue;
                }
                if (seenRacks.get(dc).size() == racks.get(dc).keySet().size()) {
                    dcReplicas.get(dc).add(ep);
                    replicas.add(ep);
                } else {
                    String rack = peer.getRack();
                    if (seenRacks.get(dc).contains(rack)) {
                        skippedDcEndpoints.get(dc).add(ep);
                    } else {
                        dcReplicas.get(dc).add(ep);
                        replicas.add(ep);
                        seenRacks.get(dc).add(rack);
                        if (seenRacks.get(dc).size() == racks.get(dc).keySet().size()) {
                            Iterator<InetAddress> skippedIt = skippedDcEndpoints.get(dc).iterator();
                            while (skippedIt.hasNext() && !hasSufficientReplicas(dc, dcReplicas, allEndpoints)) {
                                InetAddress nextSkipped = skippedIt.next();
                                dcReplicas.get(dc).add(nextSkipped);
                                replicas.add(nextSkipped);
                            }
                        }
                    }
                }
            }
            return new ArrayList<InetAddress>(replicas);
        }

        private boolean hasSufficientReplicas(String dc, Map<String, Set<InetAddress>> dcReplicas, Map<String, CopyOnWriteArrayList<InetAddress>> allEndpoints) {
            return dcReplicas.get(dc).size() >= Math.min(allEndpoints.get(dc).size(), getReplicationFactor(dc));
        }

        private boolean hasSufficientReplicas(Map<String, Set<InetAddress>> dcReplicas, Map<String, CopyOnWriteArrayList<InetAddress>> allEndpoints) {
            for (String dc : replicationFactors.keySet()) {
                if (!hasSufficientReplicas(dc, dcReplicas, allEndpoints)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class OldNetworkTopologyStrategy extends ReplicationStrategy { // TODO refactor this

        public OldNetworkTopologyStrategy(KeyspaceMetadata keyspace) {
            super(keyspace);
        }

        @Override
        public int getReplicationFactor() {
            return Integer.parseInt(keyspace.getStrategyOptions().get("replication_factor"));
        }

        @Override
        public List<InetAddress> calculateNaturalEndpoints(Partitioner.Token token) {
            Metadata metadata = keyspace.getMetadata();
            if (metadata.getRingSize() == 0) {
                return Collections.emptyList();
            }
            int replicas = getReplicationFactor();
            List<InetAddress> endpoints = new ArrayList<InetAddress>(replicas);
            Iterator<Map.Entry<Partitioner.Token, InetAddress>> iterator = metadata.getRingIterator(token);
            Map.Entry<Partitioner.Token, InetAddress> primaryToken = iterator.next();
            endpoints.add(primaryToken.getValue());
            boolean bDataCenter = false;
            boolean bOtherRack = false;
            while (endpoints.size() < replicas && iterator.hasNext()) {
                Map.Entry<Partitioner.Token, InetAddress> entry = iterator.next();
                if (!metadata.getPeer(primaryToken.getValue()).getDatacenter().equals(metadata.getPeer(entry.getValue()).getDatacenter())) {
                    if (!bDataCenter) {
                        endpoints.add(entry.getValue());
                        bDataCenter = true;
                    }
                    continue;
                }
                if (!metadata.getPeer(primaryToken.getValue()).getRack().equals(metadata.getPeer(entry.getValue()).getRack()) &&
                        metadata.getPeer(primaryToken.getValue()).getDatacenter().equals(metadata.getPeer(entry.getValue()).getDatacenter())) {
                    if (!bOtherRack) {
                        endpoints.add(entry.getValue());
                        bOtherRack = true;
                    }
                }
            }
            if (endpoints.size() < replicas) {
                iterator = metadata.getRingIterator(token);
                while (endpoints.size() < replicas && iterator.hasNext()) {
                    Map.Entry<Partitioner.Token, InetAddress> entry = iterator.next();
                    if (!endpoints.contains(entry.getValue()))
                        endpoints.add(entry.getValue());
                }
            }
            return endpoints;
        }
    }

    public static class LocalStrategy extends SimpleStrategy {

        public LocalStrategy(KeyspaceMetadata keyspace) {
            super(keyspace);
        }

        @Override
        public int getReplicationFactor() {
            return 1;
        }
    }
}
