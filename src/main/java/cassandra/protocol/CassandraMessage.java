package cassandra.protocol;

import cassandra.CassandraDriver;
import cassandra.CassandraException;
import cassandra.cql.BatchStatement;
import cassandra.cql.Consistency;
import cassandra.cql.PreparedStatement;
import cassandra.cql.RowMetadata.Column;
import cassandra.cql.WriteType;
import cassandra.cql.type.CQL3Type;
import cassandra.protocol.internal.Message;
import cassandra.protocol.internal.MessageInputStream;
import cassandra.protocol.internal.MessageOutputStream;
import cassandra.protocol.internal.MessageParser;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

public abstract class CassandraMessage extends Message {

    public static class Header extends Message {

        public static enum Flag {
            COMPRESSED, TRACING
        }

        public static final MessageParser<Header> PARSER = new MessageParser<Header>() {
            @Override
            public Header parseFrom(MessageInputStream input) {
                return new Header(input);
            }
        };

        public static Header parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public static Header valueOf(CassandraMessage message) {
            Header header = new Header(message.getType());
            header.setVersion(CassandraDriver.NATIVE_PROTOCOL_VERSION_NUMBER);
            header.setCompressedFlag(message.getType() != Type.STARTUP && message.hasCompression());
            header.setTracingFlag(message.isTracing());
            header.setStreamId(message.streamId);
            header.setMessageLength(message.getApproximateSize());
            return header;
        }

        private int version;
        private EnumSet<Flag> flags;
        private int streamId;
        private CassandraMessage.Type messageType;
        private int messageLength;

        public Header(CassandraMessage.Type messageType) {
            flags = EnumSet.noneOf(Flag.class);
            setMessageType(messageType);
        }

        private Header(MessageInputStream input) {
            version = input.readInt8();
            version &= 0x7F;
            flags = input.readEnumSet8(Flag.class);
            streamId = input.readInt8();
            int opcode = input.readInt8();
            messageType = CassandraMessage.Type.valueOf(opcode);
            messageLength = input.readInt32();
        }

        public int getVersion() {
            return version;
        }

        public Header setVersion(int version) {
            this.version = version;
            return this;
        }

        public boolean getCompressedFlag() {
            return flags.contains(Flag.COMPRESSED);
        }

        public Header setCompressedFlag(boolean compressedFlag) {
            if (compressedFlag) {
                flags.add(Flag.COMPRESSED);
            } else {
                flags.remove(Flag.COMPRESSED);
            }
            return this;
        }

        public boolean getTracingFlag() {
            return flags.contains(Flag.TRACING);
        }

        public Header setTracingFlag(boolean tracingFlag) {
            if (tracingFlag) {
                flags.add(Flag.TRACING);
            } else {
                flags.remove(Flag.TRACING);
            }
            return this;
        }

        public CassandraMessage.Type getMessageType() {
            return messageType;
        }

        protected Header setMessageType(CassandraMessage.Type messageType) {
            this.messageType = messageType;
            return this;
        }

        public int getStreamId() {
            return streamId;
        }

        public Header setStreamId(int streamId) {
            this.streamId = streamId;
            return this;
        }

        public int getMessageLength() {
            return messageLength;
        }

        public Header setMessageLength(int messageLength) {
            this.messageLength = messageLength;
            return this;
        }

        @Override
        public MessageParser<Header> getParserForType() {
            return PARSER;
        }

        @Override
        public int getApproximateSize() {
            return 8;
        }

        @Override
        public void writeTo(MessageOutputStream output) {
            if (messageType.direction == CassandraMessage.Direction.REQUEST) {
                output.writeInt8(version & 0x7F);
            } else {
                output.writeInt8(version | 0x80);
            }
            output.writeEnumSet8(flags);
            output.writeInt8(streamId);
            output.writeInt8(messageType.opcode);
            output.writeInt32(messageLength);
        }
    }

    public static enum Direction {
        REQUEST, RESPONSE
    }

    public static enum Type {
        ERROR(0, Direction.RESPONSE, Error.PARSER),
        STARTUP(1, Direction.REQUEST, Startup.PARSER),
        READY(2, Direction.RESPONSE, Ready.PARSER),
        AUTHENTICATE(3, Direction.RESPONSE, Authenticate.PARSER),
        //CREDENTIALS(4, Direction.REQUEST, null),
        OPTIONS(5, Direction.REQUEST, Options.PARSER),
        SUPPORTED(6, Direction.RESPONSE, Supported.PARSER),
        QUERY(7, Direction.REQUEST, Query.PARSER),
        RESULT(8, Direction.RESPONSE, Result.PARSER),
        PREPARE(9, Direction.REQUEST, Prepare.PARSER),
        EXECUTE(10, Direction.REQUEST, Execute.PARSER),
        REGISTER(11, Direction.REQUEST, Register.PARSER),
        EVENT(12, Direction.RESPONSE, Event.PARSER),
        BATCH(13, Direction.REQUEST, Batch.PARSER),
        AUTH_CHALLENGE(14, Direction.RESPONSE, AuthChallenge.PARSER),
        AUTH_RESPONSE(15, Direction.REQUEST, AuthResponse.PARSER),
        AUTH_SUCCESS(16, Direction.RESPONSE, AuthSuccess.PARSER);

        public final int opcode;
        public final Direction direction;
        public final MessageParser<? extends CassandraMessage> parser;

        private Type(int opcode, Direction direction, MessageParser<? extends CassandraMessage> parser) {
            this.opcode = opcode;
            this.direction = direction;
            this.parser = parser;
        }

        public static Type valueOf(int opcode) {
            for (Type type : Type.values()) {
                if (type.opcode == opcode) {
                    return type;
                }
            }
            throw new IllegalStateException(String.format("unknown opcode %d", opcode));
        }
    }

    public static CassandraMessage parseFrom(Header header, MessageInputStream input) {
        CassandraMessage message;
        if (header.getMessageType().direction == Direction.RESPONSE && header.getTracingFlag()) {
            UUID tracingId = input.readUUID();
            Response response = ((Response)header.getMessageType().parser.parseFrom(input));
            response.setTracingId(tracingId);
            message = response;
        } else {
            message = header.getMessageType().parser.parseFrom(input);
        }
        message.setCompression(header.getCompressedFlag());
        message.setTracing(header.getTracingFlag());
        message.setStreamId(header.getStreamId());
        return message;
    }

    private Type type;
    private boolean compression, tracing;
    private int streamId;

    protected CassandraMessage(Type type) {
        if (type == null) {
            throw new NullPointerException("type");
        }

        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public boolean hasCompression() {
        return compression;
    }

    public void setCompression(boolean compression) {
        this.compression = compression;
    }

    public boolean isTracing() {
        return tracing;
    }

    public void setTracing(boolean tracing) {
        this.tracing = tracing;
    }

    public int getStreamId() {
        return streamId;
    }

    public void setStreamId(int streamId) {
        this.streamId = streamId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MessageParser<CassandraMessage> getParserForType() {
        return (MessageParser<CassandraMessage>)getType().parser;
    }

    @Override
    public void writeTo(MessageOutputStream output) {
        writePartialTo(output);
    }

    protected abstract void writePartialTo(MessageOutputStream output);

    @Override
    public String toString() {
        return getType().toString();
    }

    public static abstract class Request extends CassandraMessage {

        protected Request(Type type) {
            super(type);

            if (type.direction != Direction.REQUEST) {
                throw new IllegalArgumentException();
            }
        }
    }

    public static abstract class Response extends CassandraMessage {

        private UUID tracingId;

        protected Response(Type type) {
            super(type);

            if (type.direction != Direction.RESPONSE) {
                throw new IllegalArgumentException();
            }
        }

        public boolean hasTracingId() {
            return tracingId != null;
        }

        public UUID getTracingId() {
            return tracingId;
        }

        public void setTracingId(UUID tracingId) {
            if (tracingId != null) {
                setTracing(true);
                this.tracingId = tracingId;
            }
        }
    }

    public static class Options extends Request {

        public static final MessageParser<Options> PARSER = new MessageParser<Options>() {
            @Override
            public Options parseFrom(MessageInputStream input) {
                return new Options(input);
            }
        };

        public static Options parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public Options() {
            super(Type.OPTIONS);
        }

        private Options(MessageInputStream input) {
            super(Type.OPTIONS);
        }

        @Override
        public int getApproximateSize() {
            return 0;
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            // header-only
        }
    }

    public static class Supported extends Response {

        public static final MessageParser<Supported> PARSER = new MessageParser<Supported>() {
            @Override
            public Supported parseFrom(MessageInputStream input) {
                return new Supported(input);
            }
        };

        public static Supported parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final Map<String, List<String>> options;

        public Supported(Map<String, List<String>> options) {
            super(Type.SUPPORTED);
            this.options = options;
        }

        private Supported(MessageInputStream input) {
            this(input.readStringToStringListMap());
        }

        @Override
        public int getApproximateSize() {
            return MessageOutputStream.computeStringToStringListMapSize(options);
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeStringToStringListMap(options);
        }
    }

    public static class Startup extends Request {

        public static final MessageParser<Startup> PARSER = new MessageParser<Startup>() {
            @Override
            public Startup parseFrom(MessageInputStream input) {
                return new Startup(input);
            }
        };

        public static Startup parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final Map<String, String> options;

        public Startup(Map<String, String> options) {
            super(Type.STARTUP);
            this.options = options;
        }

        private Startup(MessageInputStream input) {
            this(input.readStringMap());
        }

        @Override
        public int getApproximateSize() {
            return MessageOutputStream.computeStringMapSize(options);
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeStringMap(options);
        }
    }

    public static class Authenticate extends Response {

        public static final MessageParser<Authenticate> PARSER = new MessageParser<Authenticate>() {
            @Override
            public Authenticate parseFrom(MessageInputStream input) {
                return new Authenticate(input);
            }
        };

        public static Authenticate parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final String authenticator;

        public Authenticate(String authenticator) {
            super(Type.AUTHENTICATE);
            this.authenticator = authenticator;
        }

        private Authenticate(MessageInputStream input) {
            this(input.readString());
        }

        @Override
        public int getApproximateSize() {
            return MessageOutputStream.computeStringSize(authenticator);
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeString(authenticator);
        }
    }

    public static class AuthResponse extends Request {

        public static final MessageParser<AuthResponse> PARSER = new MessageParser<AuthResponse>() {
            @Override
            public AuthResponse parseFrom(MessageInputStream input) {
                return new AuthResponse(input);
            }
        };

        public static AuthResponse parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final byte[] token;

        public AuthResponse(byte[] token) {
            super(Type.AUTH_RESPONSE);
            this.token = token;
        }

        private AuthResponse(MessageInputStream input) {
            this(input.readValue().array());
        }

        @Override
        public int getApproximateSize() {
            return MessageOutputStream.computeValueSize(token);
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeValue(token);
        }
    }

    public static class AuthChallenge extends Response {

        public static final MessageParser<AuthChallenge> PARSER = new MessageParser<AuthChallenge>() {
            @Override
            public AuthChallenge parseFrom(MessageInputStream input) {
                return new AuthChallenge(input);
            }
        };

        public static AuthChallenge parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final byte[] token;

        public AuthChallenge(byte[] token) {
            super(Type.AUTH_CHALLENGE);
            this.token = token;
        }

        private AuthChallenge(MessageInputStream input) {
            this(input.readValue().array());
        }

        @Override
        public int getApproximateSize() {
            return MessageOutputStream.computeValueSize(token);
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeValue(token);
        }
    }

    public static class AuthSuccess extends Response {

        public static final MessageParser<AuthSuccess> PARSER = new MessageParser<AuthSuccess>() {
            @Override
            public AuthSuccess parseFrom(MessageInputStream input) {
                return new AuthSuccess(input);
            }
        };

        public static AuthSuccess parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final byte[] token;

        public AuthSuccess(byte[] token) {
            super(Type.AUTH_SUCCESS);
            this.token = token;
        }

        private AuthSuccess(MessageInputStream input) {
            this(input.readValue().array());
        }

        @Override
        public int getApproximateSize() {
            return MessageOutputStream.computeValueSize(token);
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeValue(token);
        }
    }

    public static class Register extends Request {

        public static final MessageParser<Register> PARSER = new MessageParser<Register>() {
            @Override
            public Register parseFrom(MessageInputStream input) {
                return new Register(input);
            }
        };

        public static Register parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final List<Event.Type> events;

        public Register(List<Event.Type> events) {
            super(Type.REGISTER);
            this.events = events;
        }

        private Register(MessageInputStream input) {
            super(Type.REGISTER);
            int size = input.readInt16();
            events = new ArrayList<Event.Type>(size);
            for (int i = 0; i < size; i++) {
                Event.Type event = input.readEnum(Event.Type.class);
                events.add(event);
            }
        }

        @Override
        public int getApproximateSize() {
            int size = 2;
            for (Event.Type event : events) {
                size += MessageOutputStream.computeEnumSize(event);
            }
            return size;
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeInt16(events.size());
            for (Event.Type event : events) {
                output.writeEnum(event);
            }
        }
    }

    public static class Ready extends Response {

        public static final MessageParser<Ready> PARSER = new MessageParser<Ready>() {
            @Override
            public Ready parseFrom(MessageInputStream input) {
                return new Ready(input);
            }
        };

        public static Ready parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public Ready() {
            super(Type.READY);
        }

        private Ready(MessageInputStream input) {
            this();
        }

        @Override
        public int getApproximateSize() {
            return 0;
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            // header-only
        }
    }

    public static abstract class Event extends Response {

        public static final MessageParser<Event> PARSER = new MessageParser<Event>() {
            @Override
            public Event parseFrom(MessageInputStream input) {
                Event.Type type = input.readEnum(Event.Type.class);
                switch (type) {
                    case TOPOLOGY_CHANGE:
                        return new TopologyChange(input);
                    case STATUS_CHANGE:
                        return new StatusChange(input);
                    case SCHEMA_CHANGE:
                        return new SchemaChange(input);
                    default:
                        throw new IllegalStateException(String.format("unknown event %s", type));
                }
            }
        };

        public static Event parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public static enum Type {
            TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE
        }

        public final Type type;

        protected Event(Type type) {
            super(CassandraMessage.Type.EVENT);
            this.type = type;
        }

        public static class TopologyChange extends Event {

            public static TopologyChange newNode(InetSocketAddress node) {
                return new TopologyChange(Change.NEW_NODE, node);
            }

            public static TopologyChange removedNode(InetSocketAddress node) {
                return new TopologyChange(Change.REMOVED_NODE, node);
            }

            public static TopologyChange movedNode(InetSocketAddress node) {
                return new TopologyChange(Change.MOVED_NODE, node);
            }

            public enum Change {
                NEW_NODE, REMOVED_NODE, MOVED_NODE
            }

            public final Change change;
            public final InetSocketAddress node;

            public TopologyChange(Change change, InetSocketAddress node) {
                super(Type.TOPOLOGY_CHANGE);
                this.change = change;
                this.node = node;
            }

            private TopologyChange(MessageInputStream input) {
                super(Type.TOPOLOGY_CHANGE);
                change = input.readEnum(TopologyChange.Change.class);
                node = input.readInet();
            }

            @Override
            public int getApproximateSize() {
                int size = MessageOutputStream.computeEnumSize(type);
                size += MessageOutputStream.computeEnumSize(change);
                size += MessageOutputStream.computeInetSize(node);
                return size;
            }

            @Override
            protected void writePartialTo(MessageOutputStream output) {
                output.writeEnum(type);
                output.writeEnum(change);
                output.writeInet(node);
            }

            @Override
            public String toString() {
                return String.format("%s.%s %s", type, change, node);
            }
        }

        public static class StatusChange extends Event {

            public static StatusChange up(InetSocketAddress node) {
                return new StatusChange(Status.UP, node);
            }

            public static StatusChange down(InetSocketAddress node) {
                return new StatusChange(Status.DOWN, node);
            }

            public enum Status {
                UP, DOWN
            }

            public final Status status;
            public final InetSocketAddress node;

            public StatusChange(Status status, InetSocketAddress node) {
                super(Type.STATUS_CHANGE);
                this.status = status;
                this.node = node;
            }

            private StatusChange(MessageInputStream input) {
                super(Type.STATUS_CHANGE);
                status = input.readEnum(StatusChange.Status.class);
                node = input.readInet();
            }

            @Override
            public int getApproximateSize() {
                int size = MessageOutputStream.computeEnumSize(type);
                size += MessageOutputStream.computeEnumSize(status);
                size += MessageOutputStream.computeInetSize(node);
                return size;
            }

            @Override
            protected void writePartialTo(MessageOutputStream output) {
                output.writeEnum(type);
                output.writeEnum(status);
                output.writeInet(node);
            }

            @Override
            public String toString() {
                return String.format("%s.%s %s", type, status, node);
            }
        }

        public static class SchemaChange extends Event {

            public enum Change {
                CREATED, UPDATED, DROPPED
            }

            public final Change change;
            public final String keyspace;
            public final String table;

            public SchemaChange(Change change, String keyspace, String table) {
                super(Type.SCHEMA_CHANGE);
                this.change = change;
                this.keyspace = keyspace;
                this.table = table;
            }

            private SchemaChange(MessageInputStream input) {
                super(Type.SCHEMA_CHANGE);
                change = input.readEnum(SchemaChange.Change.class);
                keyspace = input.readString();
                table = input.readString();
            }

            @Override
            public int getApproximateSize() {
                int size = MessageOutputStream.computeEnumSize(type);
                size += MessageOutputStream.computeEnumSize(change);
                size += MessageOutputStream.computeStringSize(keyspace);
                size += MessageOutputStream.computeStringSize(table);
                return size;
            }

            @Override
            protected void writePartialTo(MessageOutputStream output) {
                output.writeEnum(type);
                output.writeEnum(change);
                output.writeString(keyspace);
                output.writeString(table);
            }

            @Override
            public String toString() {
                if (table.isEmpty()) {
                    return String.format("%s.%s %s", type, change, keyspace);
                } else {
                    return String.format("%s.%s %s.%s", type, change, keyspace, table);
                }
            }
        }
    }

    public static class Batch extends Request {

        public static final MessageParser<Batch> PARSER = new MessageParser<Batch>() {
            @Override
            public Batch parseFrom(MessageInputStream input) {
                return new Batch(input);
            }
        };

        public static Batch parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public static class QueryValue {

            public final Object stringOrId;
            public final List<ByteBuffer> values;

            public QueryValue(Object stringOrId, List<ByteBuffer> values) {
                this.stringOrId = stringOrId;
                this.values = values;
            }
        }

        public final BatchStatement.Type type;
        public final List<QueryValue> queries;
        public final Consistency consistency;

        public Batch(BatchStatement.Type type, List<QueryValue> queries, Consistency consistency) {
            super(Type.BATCH);
            this.type = type;
            this.queries = queries;
            this.consistency = consistency;
        }

        private Batch(MessageInputStream input) {
            super(Type.BATCH);
            type = BatchStatement.Type.valueOf(input.readInt8());
            int size = input.readInt16();
            queries = new ArrayList<QueryValue>(size);
            for (int i = 0; i < size; i++) {
                int kind = input.readInt8();
                Object stringOrId;
                if (kind == 0) {
                    stringOrId = input.readLongString();
                } else {
                    stringOrId = new PreparedStatement.StatementId(input.readBytes());
                }
                List<ByteBuffer> values = input.readValueList();
                queries.add(new QueryValue(stringOrId, values));
            }
            consistency = Consistency.valueOf(input.readInt16());
        }

        @Override
        public int getApproximateSize() {
            int size = 1 + 2;
            for (Batch.QueryValue query : queries) {
                size += 1;
                if (query.stringOrId instanceof String) {
                    size += MessageOutputStream.computeLongStringSize((String)query.stringOrId);
                } else {
                    size += MessageOutputStream.computeBytesSize(((PreparedStatement.StatementId)query.stringOrId).array());
                }
                size += MessageOutputStream.computeValueListSize(query.values);
            }
            size += 2;
            return size;
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeInt8(type.value);
            output.writeInt16(queries.size());
            for (Batch.QueryValue query : queries) {
                if (query.stringOrId instanceof String) {
                    output.writeInt8(0);
                    output.writeLongString((String)query.stringOrId);
                } else {
                    output.writeInt8(1);
                    output.writeBytes(((PreparedStatement.StatementId)query.stringOrId).array());
                }
                output.writeValueList(query.values);
            }
            output.writeInt16(consistency.code);
        }
    }

    public static class Execute extends Request {

        public static final MessageParser<Execute> PARSER = new MessageParser<Execute>() {
            @Override
            public Execute parseFrom(MessageInputStream input) {
                return new Execute(input);
            }
        };

        public static Execute parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final PreparedStatement.StatementId statementId;
        public final QueryParameters queryParameters;

        public Execute(PreparedStatement.StatementId statementId, QueryParameters queryParameters) {
            super(Type.EXECUTE);
            this.statementId = statementId;
            this.queryParameters = queryParameters;
        }

        private Execute(MessageInputStream input) {
            this(new PreparedStatement.StatementId(input.readBytes()), QueryParameters.parseFrom(input));
        }

        @Override
        public int getApproximateSize() {
            int size = MessageOutputStream.computeBytesSize(statementId.array());
            size += queryParameters.getApproximateSize();
            return size;
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeBytes(statementId.array());
            queryParameters.writeTo(output);
        }
    }

    public static class Prepare extends Request {

        public static final MessageParser<Prepare> PARSER = new MessageParser<Prepare>() {
            @Override
            public Prepare parseFrom(MessageInputStream input) {
                return new Prepare(input);
            }
        };

        public static Prepare parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final String query;

        public Prepare(String query) {
            super(Type.PREPARE);
            this.query = query;
        }

        private Prepare(MessageInputStream input) {
            this(input.readLongString());
        }

        @Override
        public int getApproximateSize() {
            return MessageOutputStream.computeLongStringSize(query);
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeLongString(query);
        }
    }

    public static class Query extends Request {

        public static final MessageParser<Query> PARSER = new MessageParser<Query>() {
            @Override
            public Query parseFrom(MessageInputStream input) {
                return new Query(input);
            }
        };

        public static Query parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final String query;
        public final QueryParameters queryParameters;

        public Query(String query, QueryParameters queryParameters) {
            super(Type.QUERY);
            this.query = query;
            this.queryParameters = queryParameters;
        }

        private Query(MessageInputStream input) {
            this(input.readLongString(), QueryParameters.parseFrom(input));
        }

        @Override
        public int getApproximateSize() {
            int size = MessageOutputStream.computeLongStringSize(query);
            size += queryParameters.getApproximateSize();
            return size;
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeLongString(query);
            queryParameters.writeTo(output);
        }
    }

    public static abstract class Result extends Response {

        public static final MessageParser<Result> PARSER = new MessageParser<Result>() {
            @Override
            public Result parseFrom(MessageInputStream input) {
                int id = input.readInt32();
                Kind kind = Kind.valueOf(id);
                switch (kind) {
                    case VOID:
                        return new Void(input);
                    case ROWS:
                        return new Rows(input);
                    case SET_KEYSPACE:
                        return new SetKeyspace(input);
                    case PREPARED:
                        return new Prepared(input);
                    case SCHEMA_CHANGE:
                        return new SchemaChange(input);
                    default:
                        throw new IllegalStateException(String.format("unknown kind id %d in RESULT message ", id));
                }
            }
        };

        public static Result parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        @Override
        public void writeTo(MessageOutputStream output) {
            if (hasTracingId()) {
                output.writeUUID(getTracingId());
            }
            super.writeTo(output);
        }

        public enum Kind {
            VOID(1), ROWS(2), SET_KEYSPACE(3), PREPARED(4), SCHEMA_CHANGE(5);

            public final int id;

            private Kind(int id) {
                this.id = id;
            }

            public static Kind valueOf(int id) {
                for (Kind kind : Kind.values()) {
                    if (kind.id == id) {
                        return kind;
                    }
                }
                throw new IllegalStateException(String.format("unknown kind id %d in RESULT message ", id));
            }
        }

        public final Kind kind;

        protected Result(Kind kind) {
            super(Type.RESULT);
            this.kind = kind;
        }

        public static class Void extends Result {

            public Void() {
                super(Kind.VOID);
            }

            private Void(MessageInputStream input) {
                this();
            }

            @Override
            public int getApproximateSize() {
                return 4;
            }

            @Override
            protected void writePartialTo(MessageOutputStream output) {
                output.writeInt32(kind.id);
            }
        }

        public static class Rows extends Result {

            public final Metadata metadata;
            public final Queue<List<ByteBuffer>> rows;

            public Rows(Metadata metadata, Queue<List<ByteBuffer>> rows) {
                super(Kind.ROWS);
                this.metadata = metadata;
                this.rows = rows;
            }

            private Rows(MessageInputStream input) {
                super(Kind.ROWS);
                metadata = Metadata.parseFrom(input);
                int rowCount = input.readInt32();
                int columnCount = metadata.columnCount;
                rows = new ArrayDeque<List<ByteBuffer>>(rowCount);
                for (int i = 0; i < rowCount; i++) {
                    List<ByteBuffer> row = new ArrayList<ByteBuffer>(columnCount);
                    for (int j = 0; j < columnCount; j++) {
                        row.add(input.readValue());
                    }
                    rows.add(row);
                }
            }

            @Override
            public int getApproximateSize() {
                int size = 4;
                size += metadata.getApproximateSize();
                size += 4;
                for (List<ByteBuffer> row : rows) {
                    for (ByteBuffer column : row) {
                        size += MessageOutputStream.computeValueSize(column);
                    }
                }
                if (hasTracingId()) {
                    size += 16;
                }
                return size;
            }

            @Override
            protected void writePartialTo(MessageOutputStream output) {
                output.writeInt32(kind.id);
                metadata.writeTo(output);
                output.writeInt32(rows.size());
                for (List<ByteBuffer> row : rows) {
                    for (ByteBuffer column : row) {
                        output.writeValue(column);
                    }
                }
            }
        }

        public static class SetKeyspace extends Result {

            public final String keyspace;

            public SetKeyspace(String keyspace) {
                super(Kind.SET_KEYSPACE);
                this.keyspace = keyspace;
            }

            private SetKeyspace(MessageInputStream input) {
                this(input.readString());
            }

            @Override
            public int getApproximateSize() {
                return 4 + MessageOutputStream.computeStringSize(keyspace);
            }

            @Override
            protected void writePartialTo(MessageOutputStream output) {
                output.writeInt32(kind.id);
                output.writeString(keyspace);
            }
        }

        public static class Prepared extends Result {

            public final PreparedStatement.StatementId statementId;
            public final Metadata metadata, resultMetadata;

            public Prepared(PreparedStatement.StatementId statementId, Metadata metadata, Metadata resultMetadata) {
                super(Kind.PREPARED);
                this.statementId = statementId;
                this.metadata = metadata;
                this.resultMetadata = resultMetadata;
            }

            private Prepared(MessageInputStream input) {
                this(new PreparedStatement.StatementId(input.readBytes()), Metadata.parseFrom(input), Metadata.parseFrom(input));
            }

            @Override
            public int getApproximateSize() {
                int size = 4;
                size += MessageOutputStream.computeBytesSize(statementId.array());
                size += metadata.getApproximateSize();
                size += resultMetadata.getApproximateSize();
                if (hasTracingId()) {
                    size += 16;
                }
                return size;
            }

            @Override
            protected void writePartialTo(MessageOutputStream output) {
                output.writeInt32(kind.id);
                output.writeBytes(statementId.array());
                metadata.writeTo(output);
                resultMetadata.writeTo(output);
            }
        }

        public static class SchemaChange extends Result {

            public enum Change {
                CREATED, UPDATED, DROPPED
            }

            public final Change change;
            public final String keyspace;
            public final String table;

            public SchemaChange(Change change, String keyspace, String table) {
                super(Kind.SCHEMA_CHANGE);
                this.change = change;
                this.keyspace = keyspace;
                this.table = table;
            }

            private SchemaChange(MessageInputStream input) {
                this(input.readEnum(SchemaChange.Change.class), input.readString(), input.readString());
            }

            @Override
            public int getApproximateSize() {
                int size = 4;
                size += MessageOutputStream.computeEnumSize(change);
                size += MessageOutputStream.computeStringSize(keyspace);
                size += MessageOutputStream.computeStringSize(table);
                return size;
            }

            @Override
            protected void writePartialTo(MessageOutputStream output) {
                output.writeInt32(kind.id);
                output.writeEnum(change);
                output.writeString(keyspace);
                output.writeString(table);
            }
        }
    }

    public static class Error extends Response {

        public static final MessageParser<Error> PARSER = new MessageParser<Error>() {
            @Override
            public Error parseFrom(MessageInputStream input) {
                return new Error(input);
            }
        };

        public static Error parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public final CassandraException.Code code;
        public final String message;
        public final CassandraException exception;

        public Error(CassandraException.Code code, String message, CassandraException exception) {
            super(Type.ERROR);
            this.code = code;
            this.message = message;
            this.exception = exception;
        }

        private Error(MessageInputStream input) {
            super(Type.ERROR);
            code = CassandraException.Code.valueOf(input.readInt32());
            message = input.readString();
            switch (code) {
                case SERVER_ERROR:
                    exception = new CassandraException.ServerError(message);
                    break;
                case PROTOCOL_ERROR:
                    exception = new CassandraException.ProtocolError(message);
                    break;
                case BAD_CREDENTIALS:
                    exception = new CassandraException.BadCredentials(message);
                    break;
                case UNAVAILABLE:
                    exception = new CassandraException.Unavailable(Consistency.valueOf(input.readUInt16()), input.readInt32(), input.readInt32());
                    break;
                case OVERLOADED:
                    exception = new CassandraException.Overloaded();
                    break;
                case IS_BOOTSTRAPPING:
                    exception = new CassandraException.IsBootstrapping();
                    break;
                case TRUNCATE_ERROR:
                    exception = new CassandraException.Truncate(message);
                    break;
                case WRITE_TIMEOUT:
                case READ_TIMEOUT:
                    Consistency consistency = Consistency.valueOf(input.readUInt16());
                    int received = input.readInt32();
                    int blockFor = input.readInt32();
                    if (CassandraException.Code.WRITE_TIMEOUT == code) {
                        WriteType writeType = input.readEnum(WriteType.class);
                        exception = new CassandraException.WriteTimeout(writeType, consistency, received, blockFor);
                    } else {
                        exception = new CassandraException.ReadTimeout(consistency, received, blockFor, input.readBool());
                    }
                    break;
                case SYNTAX_ERROR:
                    exception = new CassandraException.SyntaxError(message);
                    break;
                case UNAUTHORIZED:
                    exception = new CassandraException.Unauthorized(message);
                    break;
                case INVALID:
                    exception = new CassandraException.Invalid(message);
                    break;
                case CONFIG_ERROR:
                    exception = new CassandraException.ConfigError(message);
                    break;
                case ALREADY_EXISTS:
                    exception = new CassandraException.AlreadyExists(input.readString(), input.readString());
                    break;
                case UNPREPARED:
                    exception = new CassandraException.Unprepared(input.readBytes());
                    break;
                default:
                    throw new IllegalStateException(String.format("unknown error code %s", code));
            }
        }

        @Override
        public int getApproximateSize() {
            int size = 4;
            size += MessageOutputStream.computeStringSize(message);
            switch (code) {
                case UNAVAILABLE:
                    size += 2 + 4 + 4;
                    break;
                case WRITE_TIMEOUT:
                    CassandraException.WriteTimeout writeTimeout = (CassandraException.WriteTimeout)exception;
                    size += MessageOutputStream.computeEnumSize(writeTimeout.writeType) + 2 + 4 + 4;
                case READ_TIMEOUT:
                    size += 2 + 4 + 4 + 1;
                    break;
                case ALREADY_EXISTS:
                    CassandraException.AlreadyExists alreadyExists = (CassandraException.AlreadyExists)exception;
                    size += MessageOutputStream.computeStringSize(alreadyExists.keyspace);
                    size += MessageOutputStream.computeStringSize(alreadyExists.table);
                    break;
                case UNPREPARED:
                    CassandraException.Unprepared unprepared = (CassandraException.Unprepared)exception;
                    size += MessageOutputStream.computeBytesSize(unprepared.id);
                    break;
                default:
                    break;
            }
            return size;
        }

        @Override
        protected void writePartialTo(MessageOutputStream output) {
            output.writeInt32(code.value);
            output.writeString(message);
            switch (code) {
                case UNAVAILABLE:
                    CassandraException.Unavailable unavailable = (CassandraException.Unavailable)exception;
                    output.writeInt16(unavailable.consistency.code);
                    output.writeInt32(unavailable.required);
                    output.writeInt32(unavailable.alive);
                    break;
                case WRITE_TIMEOUT:
                    CassandraException.WriteTimeout writeTimeout = (CassandraException.WriteTimeout)exception;
                    output.writeEnum(writeTimeout.writeType);
                    output.writeInt16(writeTimeout.consistency.code);
                    output.writeInt32(writeTimeout.received);
                    output.writeInt32(writeTimeout.blockFor);
                    break;
                case READ_TIMEOUT:
                    CassandraException.ReadTimeout readTimeout = (CassandraException.ReadTimeout)exception;
                    output.writeInt16(readTimeout.consistency.code);
                    output.writeInt32(readTimeout.received);
                    output.writeInt32(readTimeout.blockFor);
                    output.writeBool(readTimeout.dataPresent);
                    break;
                case ALREADY_EXISTS:
                    CassandraException.AlreadyExists alreadyExists = (CassandraException.AlreadyExists)exception;
                    output.writeString(alreadyExists.keyspace);
                    output.writeString(alreadyExists.table);
                    break;
                case UNPREPARED:
                    CassandraException.Unprepared unprepared = (CassandraException.Unprepared)exception;
                    output.writeBytes(unprepared.id);
                    break;
                default:
                    break;
            }
        }
    }

    public static class QueryParameters extends Message {

        public static final MessageParser<QueryParameters> PARSER = new MessageParser<QueryParameters>() {
            @Override
            public QueryParameters parseFrom(MessageInputStream input) {
                return new QueryParameters(input);
            }
        };

        public static QueryParameters parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public static enum Flag {
            VALUES, SKIP_METADATA, PAGE_SIZE, WITH_PAGING_STATE, WITH_SERIAL_CONSISTENCY
        }

        public static final QueryParameters DEFAULT = new QueryParameters(Consistency.ONE, MessageInputStream.EMPTY_VALUE_ARRAY, false, -1, null, Consistency.SERIAL);

        public final Consistency consistency, serialConsistency;
        public final ByteBuffer[] values;
        public final boolean skipMetadata;
        public final int pageSize;
        public final ByteBuffer pagingState;

        public QueryParameters(Consistency consistency, ByteBuffer[] values, boolean skipMetadata, int pageSize, ByteBuffer pagingState, Consistency serialConsistency) {
            this.consistency = consistency;
            this.values = values;
            this.skipMetadata = skipMetadata;
            this.pageSize = pageSize;
            this.pagingState = pagingState;
            this.serialConsistency = serialConsistency;
        }

        private QueryParameters(MessageInputStream input) {
            consistency = Consistency.valueOf(input.readInt16());
            EnumSet<Flag> flags = input.readEnumSet8(Flag.class);
            if (flags.contains(Flag.VALUES)) {
                values = input.readValueArray();
            } else {
                values = MessageInputStream.EMPTY_VALUE_ARRAY;
            }
            skipMetadata = flags.contains(Flag.SKIP_METADATA);
            if (flags.contains(Flag.PAGE_SIZE)) {
                pageSize = input.readInt32();
            } else {
                pageSize = -1;
            }
            if (flags.contains(Flag.WITH_PAGING_STATE)) {
                pagingState = input.readValue();
            } else {
                pagingState = null;
            }
            if (flags.contains(Flag.WITH_SERIAL_CONSISTENCY)) {
                serialConsistency = Consistency.valueOf(input.readInt16());
            } else {
                serialConsistency = Consistency.SERIAL;
            }
        }

        @Override
        public int getApproximateSize() {
            int size = 2 + 1;
            if (values != null && values.length > 0) {
                size += MessageOutputStream.computeValueArraySize(values);
            }
            if (pageSize >= 0) {
                size += 4;
            }
            if (pagingState != null) {
                size += MessageOutputStream.computeValueSize(pagingState);
            }
            if (serialConsistency != Consistency.SERIAL) {
                size += 2;
            }
            return size;
        }

        @Override
        public void writeTo(MessageOutputStream output) {
            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (values != null && values.length > 0) {
                flags.add(Flag.VALUES);
            }
            if (skipMetadata) {
                flags.add(Flag.SKIP_METADATA);
            }
            if (pageSize >= 0) {
                flags.add(Flag.PAGE_SIZE);
            }
            if (pagingState != null) {
                flags.add(Flag.WITH_PAGING_STATE);
            }
            if (serialConsistency != Consistency.SERIAL) {
                flags.add(Flag.WITH_SERIAL_CONSISTENCY);
            }
            output.writeInt16(consistency.code);
            output.writeEnumSet8(flags);
            if (flags.contains(Flag.VALUES)) {
                output.writeValueArray(values);
            }
            if (flags.contains(Flag.PAGE_SIZE)) {
                output.writeInt32(pageSize);
            }
            if (flags.contains(Flag.WITH_PAGING_STATE)) {
                output.writeValue(pagingState);
            }
            if (flags.contains(Flag.WITH_SERIAL_CONSISTENCY)) {
                output.writeInt16(serialConsistency.code);
            }
        }
    }

    public static class Metadata extends Message {

        public static final MessageParser<Metadata> PARSER = new MessageParser<Metadata>() {
            @Override
            public Metadata parseFrom(MessageInputStream input) {
                return new Metadata(input);
            }
        };

        public static Metadata parseFrom(MessageInputStream input) {
            return PARSER.parseFrom(input);
        }

        public static enum Flag {
            GLOBAL_TABLES_SPEC, HAS_MORE_PAGES, NO_METADATA
        }

        public final int columnCount;
        public final Column[] columns;
        public final ByteBuffer pagingState;

        public Metadata(int columnCount, Column[] columns, ByteBuffer pagingState) {
            this.columnCount = columnCount;
            this.columns = columns;
            this.pagingState = pagingState;
        }

        private Metadata(MessageInputStream input) {
            EnumSet<Flag> flags = input.readEnumSet32(Flag.class);
            columnCount = input.readInt32();
            if (flags.contains(Flag.HAS_MORE_PAGES)) {
                pagingState = input.readValue();
            } else {
                pagingState = null;
            }
            if (flags.contains(Flag.NO_METADATA)) {
                columns = null;
            } else {
                boolean globalTablesSpec = flags.contains(Flag.GLOBAL_TABLES_SPEC);
                String globalKeyspace = null;
                String globalTable = null;
                if (globalTablesSpec) {
                    globalKeyspace = input.readString();
                    globalTable = input.readString();
                }
                columns = new Column[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    String keyspace = globalKeyspace;
                    if (!globalTablesSpec) {
                        keyspace = input.readString();
                    }
                    String table = globalTable;
                    if (!globalTablesSpec) {
                        table = input.readString();
                    }
                    String name = input.readString();
                    CQL3Type type = input.readCQLType();
                    columns[i] = new Column(keyspace, table, name, type);
                }
            }
        }

        @Override
        public int getApproximateSize() {
            int size = 4 + 4;
            if (pagingState != null) {
                size += MessageOutputStream.computeValueSize(pagingState);
            }
            boolean noMetadata = columns == null || columns.length == 0;
            if (!noMetadata) {
                boolean globalTablesSpec = true;
                for (int i = 1; i < columns.length; i++) {
                    if (!columns[i].keyspace().equals(columns[0].keyspace()) || !columns[i].table().equals(columns[0].table())) {
                        globalTablesSpec = false;
                        break;
                    }
                }
                if (globalTablesSpec) {
                    size += MessageOutputStream.computeStringSize(columns[0].keyspace());
                    size += MessageOutputStream.computeStringSize(columns[0].table());
                }
                for (Column column : columns) {
                    if (!globalTablesSpec) {
                        size += MessageOutputStream.computeStringSize(column.keyspace());
                        size += MessageOutputStream.computeStringSize(column.table());
                    }
                    size += MessageOutputStream.computeStringSize(column.name());
                    size += MessageOutputStream.computeCQLTypeSize(column.type());
                }
            }
            return size;
        }

        @Override
        public void writeTo(MessageOutputStream output) {
            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (columns == null || columns.length == 0) {
                flags.add(Flag.NO_METADATA);
            } else {
                boolean globalTablesSpec = true;
                for (int i = 1; i < columns.length; i++) {
                    if (!columns[i].keyspace().equals(columns[0].keyspace()) || !columns[i].table().equals(columns[0].table())) {
                        globalTablesSpec = false;
                        break;
                    }
                }
                if (globalTablesSpec) {
                    flags.add(Flag.GLOBAL_TABLES_SPEC);
                }
            }
            if (pagingState != null) {
                flags.add(Flag.HAS_MORE_PAGES);
            }
            output.writeEnumSet32(flags);
            output.writeInt32(columnCount);
            if (flags.contains(Flag.HAS_MORE_PAGES)) {
                output.writeValue(pagingState);
            }
            if (!flags.contains(Flag.NO_METADATA)) {
                if (flags.contains(Flag.GLOBAL_TABLES_SPEC)) {
                    output.writeString(columns[0].keyspace());
                    output.writeString(columns[0].table());
                }
                for (Column column : columns) {
                    if (!flags.contains(Flag.GLOBAL_TABLES_SPEC)) {
                        output.writeString(column.keyspace());
                        output.writeString(column.table());
                    }
                    output.writeString(column.name());
                    output.writeCQLType(column.type());
                }
            }
        }
    }
}
