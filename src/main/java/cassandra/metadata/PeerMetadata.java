package cassandra.metadata;

import cassandra.cql.Row;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public class PeerMetadata extends MetadataEntity {

    private final Metadata metadata;
    private final InetAddress address;
    private final String datacenter;
    private final String rack;
    private final UUID hostId;
    private final String releaseVersion;
    private final UUID schemaVersion;
    private final Set<String> tokens;
    private final boolean down;

    public static class Builder {

        protected Metadata metadata;
        protected InetAddress address;
        protected String datacenter;
        protected String rack;
        protected UUID hostId;
        protected String releaseVersion;
        protected UUID schemaVersion;
        protected Set<String> tokens;
        protected boolean down;

        public Builder setMetadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setAddress(InetAddress address) {
            this.address = address;
            return this;
        }

        public Builder setDatacenter(String datacenter) {
            this.datacenter = datacenter;
            return this;
        }

        public Builder setRack(String rack) {
            this.rack = rack;
            return this;
        }

        public Builder setHostId(UUID hostId) {
            this.hostId = hostId;
            return this;
        }

        public Builder setReleaseVersion(String releaseVersion) {
            this.releaseVersion = releaseVersion;
            return this;
        }

        public Builder setSchemaVersion(UUID schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        public Builder setTokens(Set<String> tokens) {
            this.tokens = tokens;
            return this;
        }

        public Builder setDown(boolean down) {
            this.down = down;
            return this;
        }

        public Builder mergeFrom(Metadata metadata, Row row) {
            setMetadata(metadata);
            setAddress(row.getInet("peer"));
            setDatacenter(row.getString("data_center"));
            setRack("rack");
            setHostId(row.getUUID("host_id"));
            setReleaseVersion(row.getString("release_version"));
            setSchemaVersion(row.getUUID("schema_version"));
            setTokens(row.getSet("tokens", String.class));
            return this;
        }

        public Builder mergeFrom(PeerMetadata peer) {
            setMetadata(peer.metadata);
            setAddress(peer.address);
            setDatacenter(peer.datacenter);
            setRack(peer.rack);
            setHostId(peer.hostId);
            setReleaseVersion(peer.releaseVersion);
            setSchemaVersion(peer.schemaVersion);
            setTokens(peer.tokens);
            setDown(peer.down);
            return this;
        }

        public PeerMetadata build() {
            return new PeerMetadata(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public PeerMetadata(Builder builder) {
        metadata = builder.metadata;
        address = builder.address;
        datacenter = builder.datacenter;
        rack = builder.rack;
        hostId = builder.hostId;
        releaseVersion = builder.releaseVersion;
        schemaVersion = builder.schemaVersion;
        tokens = Collections.unmodifiableSet(builder.tokens);
        down = builder.down;
    }

    @JsonIgnore
    public Metadata getMetadata() {
        return metadata;
    }

    @JsonProperty("peer")
    public InetAddress getAddress() {
        return address;
    }

    @JsonIgnore
    public boolean hasDatacenter() {
        return datacenter != null && !datacenter.isEmpty();
    }

    @JsonProperty("data_center")
    public String getDatacenter() {
        return datacenter;
    }

    @JsonIgnore
    public boolean hasRack() {
        return rack != null && !rack.isEmpty();
    }

    public String getRack() {
        return rack;
    }

    public UUID getHostId() {
        return hostId;
    }

    public String getReleaseVersion() {
        return releaseVersion;
    }

    public UUID getSchemaVersion() {
        return schemaVersion;
    }

    public Set<String> getTokens() {
        return tokens;
    }

    @JsonIgnore
    public boolean isUp() {
        return !down;
    }

    @JsonIgnore
    public boolean isDown() {
        return down;
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PeerMetadata && getAddress().equals(((PeerMetadata)o).getAddress());
    }

    @Override
    public String toString() {
        String status = "UP";
        if (down) {
            status = "DOWN";
        }
        if (datacenter != null && rack != null) {
            return String.format("[cluster: %s, addr: %s, dc: %s, rack: %s, status: %s]", metadata.getClusterName(), address, datacenter, rack, status);
        } else {
            return String.format("[cluster: %s, addr: %s, status: %s]", metadata.getClusterName(), address, status);
        }
    }
}
