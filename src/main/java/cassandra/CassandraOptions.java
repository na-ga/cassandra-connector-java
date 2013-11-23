package cassandra;

import cassandra.auth.AuthProvider;
import cassandra.cql.Consistency;
import cassandra.protocol.internal.Compressor;
import cassandra.retry.RetryPolicy;
import cassandra.routing.RoundRobinPolicy;
import cassandra.routing.RoutingPolicy;

import javax.net.ssl.SSLContext;

public class CassandraOptions {

    public enum Compression {
        NONE, SNAPPY, LZ4
    }

    public static final CassandraOptions DEFAULT = newBuilder().build();
    public static final int DEFALUT_PORT = 9042;
    public static final int DEFALUT_CONNECT_TIMEOUT_MILLIS = 3000;
    public static final int DEFAULT_PAGE_SIZE_LIMIT = 1000;
    public static final Compression DEFAULT_COMPRESSION = Compression.NONE;
    public static final Consistency DEFAULT_CONSISTENCY = Consistency.QUORUM;
    public static final Consistency DEFAULT_SERIAL_CONSISTENCY = Consistency.SERIAL;

    private final int port;
    private final int connectTimeoutMillis;
    private final AuthProvider authProvider;
    private final SSLContext sslContext;
    private final String[] cipherSuites;
    private final Compression compression;
    private final int pageSizeLimit;
    private final RoutingPolicy routingPolicy;
    private final RetryPolicy retryPolicy;
    private final Consistency consistency, serialConsistency;

    public static class Builder {

        protected Integer port;
        protected Integer connectTimeoutMillis;
        protected Integer pageSizeLimit;
        protected AuthProvider authProvider;
        protected SSLContext sslContext;
        protected String[] cipherSuites;
        protected Compression compression;
        protected RoutingPolicy routingPolicy;
        protected RetryPolicy retryPolicy;
        protected Consistency consistency;
        protected Consistency serialConsistency;

        public boolean hasPort() {
            return port != null;
        }

        public Builder setPort(int port) {
            if (port < 0 || 65535 < port) {
                throw new IllegalArgumentException(String.format("port: %d (expected: between 0 and 65535)", port));
            }
            this.port = port;
            return this;
        }

        public boolean hasConnectTimeoutMillis() {
            return connectTimeoutMillis != null;
        }

        public Builder setConnectTimeoutMillis(int connectTimeoutMillis) {
            if (connectTimeoutMillis <= 0) {
                throw new IllegalArgumentException(String.format("connectTimeoutMillis: %d (expected: > 0)", connectTimeoutMillis));
            }
            this.connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

        public boolean hasPageSizeLimit() {
            return pageSizeLimit != null;
        }

        public Builder setPageSizeLimit(int pageSizeLimit) {
            if (pageSizeLimit <= 0) {
                throw new IllegalArgumentException(String.format("pageSizeLimit: %d (expected: > 0)", pageSizeLimit));
            }
            this.pageSizeLimit = pageSizeLimit;
            return this;
        }

        public boolean hasAuthProvider() {
            return authProvider != null;
        }

        public Builder setAuthProvider(AuthProvider authProvider) {
            this.authProvider = authProvider;
            return this;
        }

        public boolean hasSSL() {
            return sslContext != null;
        }

        public Builder setSSL(SSLContext sslContext, String[] cipherSuites) {
            this.sslContext = sslContext;
            this.cipherSuites = cipherSuites;
            return this;
        }

        public boolean hasCompression() {
            return compression != null;
        }

        public Builder setCompression(Compression compression) {
            if (compression == null) {
                throw new NullPointerException("compression");
            }
            if (compression != Compression.NONE && !Compressor.Factory.canUse(compression)) {
                throw new IllegalArgumentException(String.format("cannot find %s class", compression));
            }
            this.compression = compression;
            return this;
        }

        public boolean hasRoutingPolicy() {
            return routingPolicy != null;
        }

        public Builder setRoutingPolicy(RoutingPolicy routingPolicy) {
            this.routingPolicy = routingPolicy;
            return this;
        }

        public boolean hasRetryPolicy() {
            return retryPolicy != null;
        }

        public Builder setRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public boolean hasConsistency() {
            return consistency != null;
        }

        public Builder setConsistency(Consistency consistency) {
            this.consistency = consistency;
            return this;
        }

        public boolean hasSerialConsistency() {
            return serialConsistency != null;
        }

        public Builder setSerialConsistency(Consistency serialConsistency) {
            if (serialConsistency != null) {
                if (serialConsistency != Consistency.SERIAL && serialConsistency != Consistency.LOCAL_SERIAL) {
                    throw new IllegalArgumentException();
                }
            }
            this.serialConsistency = serialConsistency;
            return this;
        }

        public Builder mergeFrom(Builder builder) {
            if (!hasPort()) {
                port = builder.port;
            }
            if (!hasConnectTimeoutMillis()) {
                connectTimeoutMillis = builder.connectTimeoutMillis;
            }
            if (!hasPageSizeLimit()) {
                pageSizeLimit = builder.pageSizeLimit;
            }
            if (!hasAuthProvider()) {
                authProvider = builder.authProvider;
            }
            if (!hasSSL()) {
                sslContext = builder.sslContext;
                cipherSuites = builder.cipherSuites;
            }
            if (!hasCompression()) {
                compression = builder.compression;
            }
            if (!hasRoutingPolicy()) {
                routingPolicy = builder.routingPolicy;
            }
            if (!hasRetryPolicy()) {
                retryPolicy = builder.retryPolicy;
            }
            if (!hasConsistency()) {
                consistency = builder.consistency;
            }
            if (!hasSerialConsistency()) {
                serialConsistency = builder.serialConsistency;
            }
            return this;
        }

        public Builder mergeFrom(CassandraOptions options) {
            if (!hasPort()) {
                port = options.port;
            }
            if (!hasConnectTimeoutMillis()) {
                connectTimeoutMillis = options.connectTimeoutMillis;
            }
            if (!hasPageSizeLimit()) {
                pageSizeLimit = options.pageSizeLimit;
            }
            if (!hasAuthProvider()) {
                authProvider = options.authProvider;
            }
            if (!hasSSL()) {
                sslContext = options.sslContext;
                cipherSuites = options.cipherSuites;
            }
            if (!hasCompression()) {
                compression = options.compression;
            }
            if (!hasRoutingPolicy()) {
                routingPolicy = options.routingPolicy;
            }
            if (!hasRetryPolicy()) {
                retryPolicy = options.retryPolicy;
            }
            if (!hasConsistency()) {
                consistency = options.consistency;
            }
            if (!hasSerialConsistency()) {
                serialConsistency = options.serialConsistency;
            }
            return this;
        }

        public CassandraOptions build() {
            if (!hasPort()) {
                port = DEFALUT_PORT;
            }
            if (!hasConnectTimeoutMillis()) {
                connectTimeoutMillis = DEFALUT_CONNECT_TIMEOUT_MILLIS;
            }
            if (!hasPageSizeLimit()) {
                pageSizeLimit = DEFAULT_PAGE_SIZE_LIMIT;
            }
            if (!hasAuthProvider()) {
                authProvider = AuthProvider.NULL_PROVIDER;
            }
            if (!hasCompression()) {
                compression = DEFAULT_COMPRESSION;
            }
            if (!hasRoutingPolicy()) {
                routingPolicy = new RoundRobinPolicy();
            }
            if (!hasRetryPolicy()) {
                retryPolicy = RetryPolicy.DEFAULT;
            }
            if (!hasConsistency()) {
                consistency = DEFAULT_CONSISTENCY;
            }
            if (!hasSerialConsistency()) {
                serialConsistency = DEFAULT_SERIAL_CONSISTENCY;
            }
            return new CassandraOptions(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private CassandraOptions(Builder builder) {
        port = builder.port;
        connectTimeoutMillis = builder.connectTimeoutMillis;
        pageSizeLimit = builder.pageSizeLimit;
        authProvider = builder.authProvider;
        sslContext = builder.sslContext;
        cipherSuites = builder.cipherSuites;
        compression = builder.compression;
        retryPolicy = builder.retryPolicy;
        routingPolicy = builder.routingPolicy;
        consistency = builder.consistency;
        serialConsistency = builder.serialConsistency;
    }

    public int getPort() {
        return port;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public AuthProvider getAuthProvider() {
        return authProvider;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public String[] getCipherSuites() {
        return cipherSuites;
    }

    public Compression getCompression() {
        return compression;
    }

    public int getPageSizeLimit() {
        return pageSizeLimit;
    }

    public RoutingPolicy getRoutingPolicy() {
        return routingPolicy;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public Consistency getConsistency() {
        return consistency;
    }

    public Consistency getSerialConsistency() {
        return serialConsistency;
    }
}
