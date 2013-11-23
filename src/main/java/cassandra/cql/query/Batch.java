package cassandra.cql.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Batch extends Query<Batch> {

    private List<Query> queries;
    private Clause options;
    private boolean logged;
    private Boolean counterUpdate;

    Batch() {
        queries = new ArrayList<Query>();
        logged = true;
    }

    public List<Query> queries() {
        return Collections.unmodifiableList(queries);
    }

    public boolean hasOptions() {
        return options != null;
    }

    public Clause options() {
        return options;
    }

    public boolean isLogged() {
        return logged;
    }

    public Batch unlogged() {
        logged = false;
        return this;
    }

    public boolean isCounterUpdate() {
        if (counterUpdate == null) {
            return false;
        }
        return counterUpdate;
    }

    public Batch usingTimestamp(long value) {
        return using(timestamp(value));
    }

    public Batch using(Clause.Using using) {
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

    public Batch add(Insert insert) {
        return add0(insert);
    }

    public Batch add(Update update) {
        return add0(update);
    }

    public Batch add(Delete delete) {
        return add0(delete);
    }

    public Batch add(Query... queries) {
        if (queries == null) {
            throw new NullPointerException("queries");
        }
        for (Query query : queries) {
            add0(query);
        }
        return this;
    }

    private Batch add0(Query query) {
        if (query == null) {
            throw new NullPointerException("query");
        }
        if (!(query instanceof Insert) && !(query instanceof Update) && !(query instanceof Delete)) {
            throw new IllegalArgumentException("BATCH statement only allows UPDATE, INSERT and DELETE statements");
        }
        boolean counterUpdate = query instanceof Update && ((Update)query).hasCounterUpdate();
        if (this.counterUpdate == null) {
            this.counterUpdate = counterUpdate;
        } else if (isCounterUpdate() != counterUpdate) {
            throw new IllegalArgumentException(String.format("cannot mix counter operations and non-counter operations in a batch statement"));
        }
        if (queries.isEmpty()) {
            keyspace = query.keyspace();
            table = query.table();
        }
        queries.add(query);
        return this;
    }

    @Override
    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }
}
