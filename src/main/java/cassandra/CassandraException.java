package cassandra;

import cassandra.cql.Consistency;
import cassandra.cql.WriteType;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public abstract class CassandraException extends RuntimeException {

    public static enum Code {
        SERVER_ERROR(0x0000),
        PROTOCOL_ERROR(0x000A),
        BAD_CREDENTIALS(0x0100),
        UNAVAILABLE(0x1000),
        OVERLOADED(0x1001),
        IS_BOOTSTRAPPING(0x1002),
        TRUNCATE_ERROR(0x1003),
        WRITE_TIMEOUT(0x1100),
        READ_TIMEOUT(0x1200),
        SYNTAX_ERROR(0x2000),
        UNAUTHORIZED(0x2100),
        INVALID(0x2200),
        CONFIG_ERROR(0x2300),
        ALREADY_EXISTS(0x2400),
        UNPREPARED(0x2500);

        public final int value;

        private Code(int value) {
            this.value = value;
        }

        public static Code valueOf(int value) {
            for (Code code : Code.values()) {
                if (code.value == value) {
                    return code;
                }
            }
            throw new IllegalStateException(String.format("unknown error code %d", value));
        }
    }

    private static final long serialVersionUID = 2553317675894391235L;

    public final Code code;

    protected CassandraException(Code code, String msg) {
        super(msg);
        this.code = code;
    }

    protected CassandraException(Code code, String msg, Throwable cause) {
        super(msg, cause);
        this.code = code;
    }

    public static class ServerError extends CassandraException {

        public ServerError(String msg) {
            super(Code.SERVER_ERROR, String.format("An unexpected error occured server-side: %s", msg));
        }
    }

    public static class ProtocolError extends CassandraException {

        public ProtocolError(String msg) {
            super(Code.PROTOCOL_ERROR, String.format("An unexpected error occured client-side: %s", msg));
        }
    }

    public static class BadCredentials extends CassandraException {

        public BadCredentials(String msg) {
            super(Code.BAD_CREDENTIALS, msg);
        }
    }

    public static class Unavailable extends CassandraException {

        public Consistency consistency;
        public int required;
        public int alive;

        public Unavailable(Consistency consistency, int required, int alive) {
            super(Code.UNAVAILABLE, "Cannot achieve consistency level " + consistency);
            this.consistency = consistency;
            this.required = required;
            this.alive = alive;
        }
    }

    public static class Overloaded extends CassandraException {

        public Overloaded() {
            super(Code.OVERLOADED, "Request cannot be processed because the coordinator node is overloaded");
        }
    }

    public static class IsBootstrapping extends CassandraException {

        public IsBootstrapping() {
            super(Code.IS_BOOTSTRAPPING, "Cannot read from a bootstrapping node");
        }
    }

    public static class Truncate extends CassandraException {

        public Truncate(Throwable e) {
            super(Code.TRUNCATE_ERROR, "Error during truncate: " + e.getMessage(), e);
        }

        public Truncate(String msg) {
            super(Code.TRUNCATE_ERROR, msg);
        }
    }

    public static abstract class Timeout extends CassandraException {

        public final Consistency consistency;
        public final int received;
        public final int blockFor;

        protected Timeout(Code code, Consistency consistency, int received, int blockFor) {
            super(code, String.format("Operation timed out - received only %d responses.", received));
            this.consistency = consistency;
            this.received = received;
            this.blockFor = blockFor;
        }
    }

    public static class WriteTimeout extends Timeout {

        public final WriteType writeType;

        public WriteTimeout(WriteType writeType, Consistency consistency, int received, int blockFor) {
            super(Code.WRITE_TIMEOUT, consistency, received, blockFor);
            this.writeType = writeType;
        }
    }

    public static class ReadTimeout extends Timeout {

        public final boolean dataPresent;

        public ReadTimeout(Consistency consistency, int received, int blockFor, boolean dataPresent) {
            super(Code.READ_TIMEOUT, consistency, received, blockFor);
            this.dataPresent = dataPresent;
        }
    }

    public static class SyntaxError extends CassandraException {

        public SyntaxError(String msg) {
            super(Code.SYNTAX_ERROR, msg);
        }
    }

    public static class Unauthorized extends CassandraException {

        public Unauthorized(String msg) {
            super(Code.UNAUTHORIZED, msg);
        }
    }

    public static class Invalid extends CassandraException {

        public Invalid(String msg) {
            super(Code.INVALID, msg);
        }
    }

    public static class ConfigError extends CassandraException {

        public ConfigError(String msg) {
            super(Code.CONFIG_ERROR, msg);
        }

        public ConfigError(String msg, Throwable e) {
            super(Code.CONFIG_ERROR, msg, e);
        }
    }

    public static class AlreadyExists extends CassandraException {

        public final String keyspace;
        public final String table;

        public AlreadyExists(String keyspace, String table) {
            this(keyspace, table, String.format("Cannot add already existing table \"%s\" to keyspace \"%s\"", table, keyspace));
        }

        public AlreadyExists(String keyspace) {
            this(keyspace, "", String.format("Cannot add existing keyspace \"%s\"", keyspace));
        }

        private AlreadyExists(String keyspace, String table, String msg) {
            super(Code.ALREADY_EXISTS, msg);
            this.keyspace = keyspace;
            this.table = table;
        }
    }

    public static class Unprepared extends CassandraException {

        public final byte[] id;

        public Unprepared(byte[] id) {
            super(Code.UNPREPARED, String.format("Prepared query with ID %s not found" +
                    " (either the query was not prepared on this host (maybe the host has been restarted?)" +
                    " or you have prepared too many queries and it has been evicted from the internal cache)", ByteBufUtil.hexDump(Unpooled.wrappedBuffer(id))));
            this.id = id;
        }
    }
}
