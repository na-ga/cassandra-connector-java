package cassandra.cql;

import cassandra.CassandraSession;
import cassandra.metadata.KeyspaceMetadata;
import cassandra.retry.RetryPolicy;
import cassandra.routing.DCAwareRoundRobinPolicy;
import cassandra.routing.RoutingPolicy;

import java.nio.ByteBuffer;

public abstract class AbstractStatement<S extends AbstractStatement<S>> implements Cloneable {

    private CassandraSession session;
    private String keyspace;
    private String query;
    private ByteBuffer[] parameters;
    private RoutingKey routingKey;
    private int pageSizeLimit;
    private PagingState pagingState;
    private Consistency consistency, serialConsistency;
    private RoutingPolicy routingPolicy;
    private RetryPolicy retryPolicy;
    private boolean traceQuery;

    AbstractStatement() {
    }

    AbstractStatement(AbstractStatement<S> statement) {
        setSession(statement.session);
        setKeyspace(statement.keyspace);
        setQuery(statement.query);
        setRoutingKey(statement.routingKey);
        setPageSizeLimit(statement.pageSizeLimit);
        setConsistency(statement.consistency);
        setSerialConsistency(statement.serialConsistency);
        setRoutingPolicy(statement.routingPolicy);
        setRetryPolicy(statement.retryPolicy);
        setTraceQuery(statement.traceQuery);
        if (statement.hasParameters()) {
            parameters = new ByteBuffer[statement.getParameters().length];
            for (int i = 0; i < statement.getParameters().length; i++) {
                parameters[i] = statement.getParameters()[i].duplicate();
            }
        }
    }

    public CassandraSession getSession() {
        return session;
    }

    @SuppressWarnings("unchecked")
    protected S setSession(CassandraSession session) {
        if (session == null) {
            throw new NullPointerException("session");
        }
        this.session = session;
        pageSizeLimit = session.options().getPageSizeLimit();
        consistency = session.options().getConsistency();
        serialConsistency = session.options().getSerialConsistency();
        routingPolicy = session.options().getRoutingPolicy();
        retryPolicy = session.options().getRetryPolicy();
        return (S)this;
    }

    public String getKeyspace() {
        if (keyspace != null && !keyspace.isEmpty()) {
            return keyspace;
        }
        String sessionKeyspace = getSession().keyspace();
        if (sessionKeyspace != null && !sessionKeyspace.isEmpty()) {
            return sessionKeyspace;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public S setKeyspace(String keyspace) {
        if (keyspace != null && !keyspace.isEmpty()) {
            this.keyspace = keyspace;
        }
        return (S)this;
    }

    public String getQuery() {
        return query;
    }

    @SuppressWarnings("unchecked")
    protected S setQuery(String query) {
        if (query == null) {
            throw new NullPointerException("query");
        }
        if (query.isEmpty()) {
            throw new IllegalArgumentException("empty query");
        }
        this.query = query;
        return (S)this;
    }

    public boolean hasParameters() {
        return parameters != null && parameters.length > 0;
    }

    public ByteBuffer[] getParameters() {
        return parameters;
    }

    @SuppressWarnings("unchecked")
    protected S setParameters(ByteBuffer[] parameters) {
        this.parameters = parameters;
        return (S)this;
    }

    @SuppressWarnings("unchecked")
    public S clearParameters() {
        if (hasParameters()) {
            for (ByteBuffer parameter : parameters) {
                parameter.clear();
            }
            parameters = null;
        }
        return (S)this;
    }

    public boolean hasRoutingKey() {
        return routingKey != null;
    }

    public RoutingKey getRoutingKey() {
        return routingKey;
    }

    @SuppressWarnings("unchecked")
    public S setRoutingKey(RoutingKey routingKey) {
        this.routingKey = routingKey;
        return (S)this;
    }

    public S setRoutingKey(ByteBuffer... routingKey) {
        return setRoutingKey(RoutingKey.copyFrom(routingKey));
    }

    public Consistency getConsistency() {
        if (consistency == Consistency.LOCAL_ONE) {
            String keyspaceName = getKeyspace();
            if (keyspaceName != null) {
                KeyspaceMetadata keyspaceMetadata = session.metadata().getKeyspace(keyspaceName);
                if (keyspaceMetadata != null && !keyspaceMetadata.getStrategyClass().endsWith(".NetworkTopologyStrategy")) {
                    return Consistency.ONE;
                }
            }
        }
        return consistency;
    }

    @SuppressWarnings("unchecked")
    public S setConsistency(Consistency consistency) {
        if (consistency == null) {
            throw new NullPointerException("consistency");
        }
        this.consistency = consistency;
        return (S)this;
    }

    public Consistency getSerialConsistency() {
        return serialConsistency;
    }

    @SuppressWarnings("unchecked")
    public S setSerialConsistency(Consistency serialConsistency) {
        if (serialConsistency == null) {
            throw new NullPointerException("serialConsistency");
        }
        if (serialConsistency != Consistency.SERIAL && serialConsistency != Consistency.LOCAL_SERIAL) {
            throw new IllegalArgumentException();
        }
        this.serialConsistency = serialConsistency;
        return (S)this;
    }

    public boolean isTraceQuery() {
        return traceQuery;
    }

    @SuppressWarnings("unchecked")
    public S setTraceQuery(boolean traceQuery) {
        this.traceQuery = traceQuery;
        return (S)this;
    }

    public int getPageSizeLimit() {
        return pageSizeLimit;
    }

    @SuppressWarnings("unchecked")
    public S setPageSizeLimit(int pageSizeLimit) {
        this.pageSizeLimit = pageSizeLimit;
        return (S)this;
    }

    public PagingState getPagingState() {
        return pagingState;
    }

    @SuppressWarnings("unchecked")
    public S setPagingState(PagingState pagingState) {
        this.pagingState = pagingState;
        return (S)this;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    @SuppressWarnings("unchecked")
    public S setRetryPolicy(RetryPolicy retryPolicy) {
        if (retryPolicy == null) {
            throw new NullPointerException("retryPolicy");
        }
        this.retryPolicy = retryPolicy;
        return (S)this;
    }

    public RoutingPolicy getRoutingPolicy() {
        return routingPolicy;
    }

    @SuppressWarnings("unchecked")
    public S setRoutingPolicy(RoutingPolicy routingPolicy) {
        if (routingPolicy == null) {
            throw new NullPointerException("routingPolicy");
        }
        this.routingPolicy = routingPolicy;
        if (this.routingPolicy instanceof DCAwareRoundRobinPolicy) {
            ((DCAwareRoundRobinPolicy)this.routingPolicy).init(session.metadata());
        }
        return (S)this;
    }

    public ResultSet execute() {
        if (session == null) {
            throw new IllegalStateException("session not set");
        }
        return session.execute(this);
    }

    public ResultSetFuture executeAsync() {
        if (session == null) {
            throw new IllegalStateException("session not set");
        }
        return session.executeAsync(this);
    }

    @Override
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public abstract S clone();
}
