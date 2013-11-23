package cassandra;

import cassandra.cql.*;
import cassandra.cql.query.Query;
import cassandra.cql.query.QueryBuilder;
import cassandra.metadata.Metadata;
import cassandra.protocol.CassandraMessage;
import cassandra.retry.RetryContext;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static io.netty.util.internal.PlatformDependent.newConcurrentHashMap;

public class CassandraSession {

    private final CassandraCluster.Client cluster;
    private final String keyspace;
    private final ConcurrentMap<InetAddress, CassandraConnection> connections;

    CassandraSession(CassandraCluster.Client cluster) {
        this(cluster, "");
    }

    CassandraSession(CassandraCluster.Client cluster, String keyspace) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        connections = newConcurrentHashMap();
    }

    public boolean isGlobal() {
        return keyspace.isEmpty();
    }

    public Metadata metadata() {
        return cluster.metadata();
    }

    public CassandraOptions options() {
        return cluster.options();
    }

    public String keyspace() {
        return keyspace;
    }

    public BatchStatement batch() {
        return new BatchStatement(this);
    }

    public Statement statement(String query) {
        return new Statement(this, query);
    }

    public Statement statement(String query, Object... values) {
        return new Statement(this, query, values);
    }

    public PreparedStatement prepareStatement(String query) {
        RetryContext context = new RetryContext(options().getRetryPolicy(), options().getRoutingPolicy().activeEndpoints(statement(query)));
        CassandraMessage.Request prepare = new CassandraMessage.Prepare(query);
        CassandraMessage.Result result = execute(prepare, new ResultFuture(this, context)).get();
        CassandraMessage.Result.Prepared prepared = CassandraMessage.Result.Prepared.class.cast(result);
        RowMetadata metadata = null;
        RowMetadata parameterMetadata = null;
        if (prepared.resultMetadata.columns != null) {
            metadata = new RowMetadata(prepared.resultMetadata.columns);
        }
        if (prepared.metadata.columns != null) {
            parameterMetadata = new RowMetadata(prepared.metadata.columns);
        }
        PreparedStatement pstmt = new PreparedStatement(this, prepared.statementId, query, metadata, parameterMetadata);
        return cluster.registerPreparedQuery(connection(context.getCurrentEndpoint()), pstmt);
    }

    public ResultSet execute(Query query) {
        return executeAsync(query).get();
    }

    public ResultSet execute(String query) {
        return execute(statement(query));
    }

    public ResultSet execute(String query, Object... values) {
        return execute(statement(query, values));
    }

    public ResultSet prepareAndExecute(String query, Object... values) {
        return execute(prepareStatement(query).bind(values));
    }

    public ResultSet execute(AbstractStatement<?> statement) {
        return executeAsync(statement).get();
    }

    public ResultSetFuture executeAsync(Query query) {
        String keyspace = query.keyspace();
        if (keyspace == null || keyspace.isEmpty()) {
            if (isGlobal()) {
                throw new IllegalArgumentException("empty keyspace");
            }
            keyspace = this.keyspace;
        }
        if (!query.hasTable()) {
            throw new IllegalArgumentException("empty table");
        }
        if (!metadata().hasTable(keyspace, query.table())) {
            throw new IllegalStateException(String.format("no matching table found: keyspace %s, table %s", keyspace, query.table()));
        }
        QueryBuilder builder = new QueryBuilder(metadata().getTable(keyspace, query.table()));
        query.accept(builder);
        AbstractStatement<?> stmt;
        if (query.isPrepared() && builder.hasParameters()) {
            stmt = prepareStatement(builder.build()).bind(builder.parameters().toArray());
        } else {
            if (builder.hasParameters()) {
                stmt = statement(builder.build(), builder.parameters().toArray());
            } else {
                stmt = statement(builder.build());
            }
        }
        if (query.pageSizeLimit() > 0) {
            stmt.setPageSizeLimit(query.pageSizeLimit());
        }
        stmt.setKeyspace(query.keyspace());
        stmt.setRoutingKey(builder.routingKey());
        if (query.routingKey() != null) {
            stmt.setRoutingKey(query.routingKey());
        }
        if (query.routingPolicy() != null) {
            stmt.setRoutingPolicy(query.routingPolicy());
        }
        if (query.retryPolicy() != null) {
            stmt.setRetryPolicy(query.retryPolicy());
        }
        if (query.consistency() != null) {
            stmt.setConsistency(query.consistency());
        }
        if (query.serialConsistency() != null) {
            stmt.setSerialConsistency(query.serialConsistency());
        }
        stmt.setPagingState(query.pagingState());
        stmt.setTraceQuery(query.isTracing());
        return executeAsync(stmt);
    }

    public ResultSetFuture executeAsync(String query) {
        return executeAsync(statement(query));
    }

    public ResultSetFuture executeAsync(String query, Object... values) {
        return executeAsync(statement(query, values));
    }

    public ResultSetFuture prepareAndExecuteAsync(String query, Object... values) {
        return executeAsync(prepareStatement(query).bind(values));
    }

    public ResultSetFuture executeAsync(AbstractStatement<?> statement) {
        if (statement == null) {
            throw new NullPointerException("statement");
        }
        CassandraMessage.QueryParameters queryParameters = null;
        if (!(statement instanceof BatchStatement)) {
            RowMetadata metadata = null;
            if (statement instanceof PreparedStatement) {
                metadata = ((PreparedStatement)statement).getMetadata();
            }
            PagingState pagingState = statement.getPagingState();
            queryParameters = new CassandraMessage.QueryParameters(statement.getConsistency(),
                    statement.getParameters(),
                    metadata != null,
                    statement.getPageSizeLimit(),
                    pagingState != null ? pagingState.asByteBuffer() : null,
                    statement.getSerialConsistency());
        }
        ResultFuture future = executeAsync(statement, queryParameters);
        return new ResultSetFuture(future, statement);
    }

    public ResultFuture executeAsync(AbstractStatement<?> statement, CassandraMessage.QueryParameters queryParameters) {
        if (statement == null) {
            throw new NullPointerException("statement");
        }
        CassandraMessage.Request request;
        if (statement instanceof BatchStatement) {
            BatchStatement batch = (BatchStatement)statement;
            List<CassandraMessage.Batch.QueryValue> queryValues = new ArrayList<CassandraMessage.Batch.QueryValue>();
            for (AbstractStatement<?> stmt : batch) {
                Object stringOrId;
                if (stmt instanceof PreparedStatement) {
                    stringOrId = ((PreparedStatement)stmt).getId();
                } else {
                    stringOrId = stmt.getQuery();
                }
                List<ByteBuffer> values;
                if (stmt.hasParameters()) {
                    values = Arrays.asList(stmt.getParameters());
                } else {
                    values = Collections.emptyList();
                }
                queryValues.add(new CassandraMessage.Batch.QueryValue(stringOrId, values));
            }
            request = new CassandraMessage.Batch(batch.getType(), queryValues, batch.getConsistency());
        } else if (statement instanceof PreparedStatement) {
            PreparedStatement pstmt = (PreparedStatement)statement;
            request = new CassandraMessage.Execute(pstmt.getId(), queryParameters);
        } else {
            request = new CassandraMessage.Query(statement.getQuery(), queryParameters);
        }
        request.setTracing(statement.isTraceQuery());
        RetryContext context = new RetryContext(statement.getRetryPolicy(), statement.getRoutingPolicy().activeEndpoints(statement));
        return execute(request, new ResultFuture(this, context));
    }

    public void close() {
        if (connections != null) {
            for (CassandraConnection connection : connections.values()) {
                connection.close();
            }
            connections.clear();
        }
    }

    private ResultFuture execute(CassandraMessage.Request request, ResultFuture future) {
        InetAddress endpoint = future.context().getCurrentEndpoint();
        connection(endpoint).send(request).addListener(future);
        return future;
    }

    private CassandraConnection connection(InetAddress endpoint) {
        if (endpoint == null) {
            throw new NullPointerException("endpoint");
        }
        if (!cluster.isActive()) {
            throw new IllegalStateException("cluster not active");
        }
        CassandraConnection connection = connections.get(endpoint.getAddress());
        if (connection == null) {
            InetSocketAddress remoteAddress = new InetSocketAddress(endpoint, options().getPort());
            CassandraConnection newConnection = cluster.driver().newConnection(remoteAddress, options());
            connection = connections.putIfAbsent(endpoint, newConnection);
            if (connection == null) {
                connection = newConnection;
            }
            connection.open(cluster);
            if (!isGlobal()) {
                connection.keyspace(keyspace);
            }
        }
        if (!connection.isActive()) {
            connections.remove(endpoint.getAddress());
        }
        return connection;
    }

    public class ResultFuture implements CassandraFuture.Listener {

        private final CassandraSession session;
        private final RetryContext context;
        private final Promise<CassandraMessage.Result> promise;

        ResultFuture(CassandraSession session, RetryContext context) {
            this(session, context, new DefaultPromise<CassandraMessage.Result>(GlobalEventExecutor.INSTANCE));
        }

        ResultFuture(CassandraSession session, RetryContext context, Promise<CassandraMessage.Result> promise) {
            this.session = session;
            this.context = context;
            this.promise = promise;
        }

        public RetryContext context() {
            return context;
        }

        public Promise<CassandraMessage.Result> promise() {
            return promise;
        }

        public CassandraMessage.Result get() {
            return CassandraFuture.get(promise, CassandraFuture.DEADLINE);
        }

        public CassandraMessage.Result get(long timeout) {
            return CassandraFuture.get(promise, timeout);
        }

        @Override
        public void completed(final CassandraFuture future) throws Exception {
            if (future.isSuccess()) {
                promise.trySuccess((CassandraMessage.Result)future.get());
            } else {
                Throwable cause = future.cause();
                context.setFailure(cause);
                if (cause instanceof CassandraException.Unprepared) {
                    CassandraException.Unprepared unprepared = (CassandraException.Unprepared)cause;
                    String query = cluster.findPreparedQuery(new PreparedStatement.StatementId(unprepared.id));
                    if (query != null) {
                        future.connection().send(new CassandraMessage.Prepare(query)).addListener(new CassandraFuture.Listener() {
                            @Override
                            public void completed(CassandraFuture f) throws Exception {
                                if (f.isSuccess()) {
                                    if (context.canRetry()) {
                                        session.execute(future.request(), ResultFuture.this);
                                    } else {
                                        promise.tryFailure(context.getLastThrowable());
                                    }
                                } else {
                                    context.setFailure(f.cause());
                                    promise.tryFailure(f.cause());
                                }
                            }
                        });
                    } else {
                        promise.tryFailure(cause);
                    }
                } else {
                    if (context.canRetry()) {
                        session.execute(future.request(), this);
                    } else {
                        promise.tryFailure(cause);
                    }
                }
            }
        }
    }
}
