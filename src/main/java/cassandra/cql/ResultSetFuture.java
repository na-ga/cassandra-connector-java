package cassandra.cql;

import cassandra.CassandraFuture;
import cassandra.CassandraSession;
import cassandra.protocol.CassandraMessage;

public class ResultSetFuture {

    private final CassandraSession.ResultFuture resultFuture;
    private final AbstractStatement<?> statement;

    public ResultSetFuture(CassandraSession.ResultFuture resultFuture, AbstractStatement<?> statement) {
        if (resultFuture == null) {
            throw new NullPointerException("resultFuture");
        }
        if (statement == null) {
            throw new NullPointerException("statement");
        }
        this.resultFuture = resultFuture;
        this.statement = statement;
    }

    public CassandraSession.ResultFuture resultFuture() {
        return resultFuture;
    }

    public boolean isSuccess() {
        return resultFuture.promise().isSuccess();
    }

    public Throwable cause() {
        return resultFuture.promise().cause();
    }

    public ResultSet get() {
        return get(CassandraFuture.DEADLINE);
    }

    public ResultSet get(long timeout) {
        CassandraMessage.Result result = resultFuture.get(timeout);
        ResultSet resultSet;
        switch (result.kind) {
            case ROWS:
                CassandraMessage.Result.Rows rows = (CassandraMessage.Result.Rows)result;
                RowMetadata metadata = null;
                if (statement instanceof PreparedStatement) {
                    metadata = ((PreparedStatement)statement).getMetadata();
                }
                if (rows.metadata.columns != null) {
                    metadata = new RowMetadata(rows.metadata.columns);
                }
                resultSet = new ResultSet(statement, metadata, rows.rows, rows.metadata.pagingState, result.getTracingId());
                break;
            default:
                resultSet = ResultSet.EMPTY_RESULT_SET;
                break;
        }
        return resultSet;
    }
}
