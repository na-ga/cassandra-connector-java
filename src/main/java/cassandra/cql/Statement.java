package cassandra.cql;

import cassandra.CassandraSession;
import cassandra.cql.type.CQL3Type;

import java.nio.ByteBuffer;

public class Statement extends AbstractStatement<Statement> {

    public Statement(CassandraSession session, String query) {
        setSession(session);
        setQuery(query);
    }

    public Statement(CassandraSession session, String query, Object... values) {
        this(session, query);
        setParameters(bindDirty(values));
    }

    private Statement(Statement statement) {
        super(statement);
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public Statement clone() {
        return new Statement(this);
    }

    private ByteBuffer[] bindDirty(Object... values) {
        if (values == null) {
            return null;
        }
        ByteBuffer[] encodedValues = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value != null) {
                encodedValues[i] = CQL3Type.typeFor(value).serialize(value);
            } else {
                encodedValues[i] = null;
            }
        }
        return encodedValues;
    }
}
