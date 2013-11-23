package cassandra.cql;

import cassandra.CassandraSession;
import cassandra.protocol.CassandraMessage;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class ResultSet implements Iterator<Row>, Iterable<Row> {

    public static final ResultSet EMPTY_RESULT_SET = new ResultSet(null, null, new ArrayDeque<List<ByteBuffer>>(0), null, null);

    private final AtomicReference<ByteBuffer> pagingState;
    private final AbstractStatement<?> statement;
    private final Queue<List<ByteBuffer>> rows;
    private final List<Trace> traces;
    private Trace lastTrace;
    private RowMetadata metadata;
    private boolean autoPaging;

    ResultSet(AbstractStatement<?> statement, RowMetadata metadata, Queue<List<ByteBuffer>> rows, ByteBuffer pagingState, UUID tracingId) {
        this.pagingState = new AtomicReference<ByteBuffer>(pagingState);
        this.statement = statement;
        this.rows = rows;
        this.metadata = metadata;
        autoPaging = true;
        if (tracingId != null) {
            Trace trace = new Trace(statement.getSession(), tracingId);
            traces = new ArrayList<Trace>();
            traces.add(trace);
            lastTrace = trace;
        } else {
            traces = Collections.emptyList();
        }
    }

    public RowMetadata getMetadata() {
        return metadata;
    }

    public boolean isAutoPaging() {
        return autoPaging;
    }

    public ResultSet setAutoPaging(boolean autoPaging) {
        this.autoPaging = autoPaging;
        return this;
    }

    public boolean hasMorePages() {
        return pagingState.get() != null;
    }

    public boolean hasRoutingKey() {
        return statement.hasRoutingKey();
    }

    public PagingState getPagingState() {
        ByteBuffer pagingState = this.pagingState.get();
        if (pagingState == null) {
            return null;
        }
        return PagingState.copyFrom(pagingState);
    }

    public boolean hasTraces() {
        return !traces.isEmpty();
    }

    public List<Trace> getTraces() {
        return traces;
    }

    public Trace getLastTrace() {
        return lastTrace;
    }

    public List<Row> asList() {
        List<Row> list = new ArrayList<Row>(rows.size());
        for (Row row : this) {
            list.add(row);
        }
        return list;
    }

    public void queryNext() {
        if (!hasMorePages()) {
            return;
        }

        CassandraMessage.QueryParameters queryParameters = new CassandraMessage.QueryParameters(statement.getConsistency(),
                statement.getParameters(),
                metadata != null,
                statement.getPageSizeLimit(),
                pagingState.getAndSet(null),
                statement.getSerialConsistency());
        CassandraSession.ResultFuture future = statement.getSession().executeAsync(statement, queryParameters);
        CassandraMessage.Result result = future.get();

        if (result.hasTracingId()) {
            Trace trace = new Trace(statement.getSession(), result.getTracingId());
            traces.add(trace);
            lastTrace = trace;
        }

        switch (result.kind) {
            case ROWS:
                CassandraMessage.Result.Rows resultRows = (CassandraMessage.Result.Rows)result;
                rows.addAll(resultRows.rows);
                if (metadata == null && resultRows.metadata.columns != null) {
                    metadata = new RowMetadata(resultRows.metadata.columns);
                }
                pagingState.set(resultRows.metadata.pagingState);
                break;
            default:
                break;
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (!rows.isEmpty()) {
            return true;
        }
        if (!autoPaging) {
            return false;
        }
        queryNext();
        return !rows.isEmpty();
    }

    @Override
    public Row next() {
        if (!hasNext()) {
            return null;
        }
        return new Row(getMetadata(), rows.poll());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
