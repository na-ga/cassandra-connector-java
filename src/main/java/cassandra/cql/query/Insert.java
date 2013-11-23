package cassandra.cql.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Insert extends Query<Insert> {

    private List<String> names;
    private List<Object> values;
    private Clause options;
    private boolean ifNotExists;

    Insert(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
        names = new ArrayList<String>();
        values = new ArrayList<Object>();
    }

    public List<String> names() {
        return Collections.unmodifiableList(names);
    }

    public List<Object> values() {
        return Collections.unmodifiableList(values);
    }

    public boolean hasOptions() {
        return options != null;
    }

    public Clause options() {
        return options;
    }

    public Insert value(String name, Object value) {
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (name.isEmpty()) {
            throw new IllegalArgumentException("empty name");
        }
        names.add(name);
        values.add(value);
        return this;
    }

    public Insert values(String[] names, Object[] values) {
        if (names == null) {
            throw new NullPointerException("name");
        }
        if (values == null) {
            throw new NullPointerException("name");
        }
        if (names.length != values.length) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < names.length; i++) {
            value(names[i], values[i]);
        }
        return this;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public Insert ifNotExists() {
        ifNotExists = true;
        return this;
    }

    public Insert usingTimestamp(long value) {
        return using(timestamp(value));
    }

    public Insert usingTTL(int value) {
        return using(ttl(value));
    }

    public Insert using(Clause.Using using) {
        if (using == null) {
            throw new NullPointerException("using");
        }
        if (options == null) {
            options = using;
        } else if (options instanceof Clause.And) {
            ((Clause.And)options).clauses().add(using);
        } else {
            Clause.And and = new Clause.And();
            and.clauses().add(options);
            and.clauses().add(using);
            options = and;
        }
        return this;
    }

    @Override
    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }
}
