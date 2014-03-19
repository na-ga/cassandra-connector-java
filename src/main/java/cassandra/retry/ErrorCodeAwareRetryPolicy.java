package cassandra.retry;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;

import cassandra.CassandraException;
import cassandra.cql.WriteType;

public class ErrorCodeAwareRetryPolicy implements RetryPolicy {

    public static final ErrorCodeAwareRetryPolicy INSTANCE = new ErrorCodeAwareRetryPolicy();

    @Override
    public boolean canRetry(RetryContext context) {
        Throwable cause = context.getLastThrowable();
        if (cause == null) {
            return true;
        }
        if (cause instanceof ConnectException) {
            return true;
        }
        if (cause instanceof ClosedChannelException) {
            return true;
        }
        if (cause instanceof CassandraException) {
            CassandraException exception = (CassandraException)cause;
            switch (exception.code) {
                case WRITE_TIMEOUT:
                    CassandraException.WriteTimeout writeTimeout = (CassandraException.WriteTimeout)exception;
                    return writeTimeout.writeType == WriteType.BATCH_LOG;
                case READ_TIMEOUT:
                    CassandraException.ReadTimeout readTimeout = (CassandraException.ReadTimeout)exception;
                    return readTimeout.received >= readTimeout.blockFor && !readTimeout.dataPresent;
                case OVERLOADED:
                case IS_BOOTSTRAPPING:
                    return context.getNextEndpoint() != null;
                case UNPREPARED:
                    return true;
                case SERVER_ERROR:
                case PROTOCOL_ERROR:
                case BAD_CREDENTIALS:
                case UNAVAILABLE:
                case TRUNCATE_ERROR:
                case SYNTAX_ERROR:
                case UNAUTHORIZED:
                case INVALID:
                case CONFIG_ERROR:
                case ALREADY_EXISTS:
                default:
                    return false;
            }
        }
        return false;
    }
}
