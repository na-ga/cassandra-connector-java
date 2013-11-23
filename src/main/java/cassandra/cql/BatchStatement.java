package cassandra.cql;

import cassandra.CassandraSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchStatement extends AbstractStatement<BatchStatement> implements Iterable<AbstractStatement<?>> {

    public static enum Type {
        LOGGED(0), UNLOGGED(1), COUNTER(2);

        public final int value;

        private Type(int value) {
            this.value = value;
        }

        public static Type valueOf(int value) {
            switch (value) {
                case 0:
                    return LOGGED;
                case 1:
                    return UNLOGGED;
                case 2:
                    return COUNTER;
                default:
                    return null;
            }
        }
    }

    private final List<AbstractStatement<?>> statements;
    private Type type;

    public BatchStatement(CassandraSession session) {
        setSession(session);
        statements = new ArrayList<AbstractStatement<?>>();
        type = Type.LOGGED;
    }

    private BatchStatement(BatchStatement statement) {
        super(statement);
        statements = new ArrayList<AbstractStatement<?>>();
        for (AbstractStatement<?> stmt : statement) {
            statements.add(stmt.clone());
        }
        type = statement.type;
    }

    public Type getType() {
        return type;
    }

    public BatchStatement setType(Type type) {
        if (type == null) {
            throw new NullPointerException("type");
        }
        this.type = type;
        return this;
    }

    public BatchStatement add(AbstractStatement<?> statement) {
        if (statement == null) {
            throw new NullPointerException("statement");
        }
        if (statement instanceof BatchStatement) {
            statements.addAll(((BatchStatement)statement).statements);
        } else if (statement instanceof PreparedStatement) {
            statements.add(statement.clone());
        } else {
            statements.add(statement);
        }
        return this;
    }

    @Override
    public Iterator<AbstractStatement<?>> iterator() {
        return statements.iterator();
    }

    public int size() {
        return statements.size();
    }

    public boolean isEmpty() {
        return statements.isEmpty();
    }

    public BatchStatement clear() {
        statements.clear();
        return this;
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public BatchStatement clone() {
        return new BatchStatement(this);
    }
}
