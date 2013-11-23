package cassandra.cql.query;

public class Update extends Query<Update> {

    private Assignment assignment;
    private Clause options;
    private Clause where;
    private Clause conditions;
    private boolean counterUpdate;

    Update(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    public boolean hasAssinment() {
        return assignment != null;
    }

    public Assignment assignment() {
        return assignment;
    }

    public boolean hasWhereClause() {
        return where != null;
    }

    public Clause whereClause() {
        return where;
    }

    public boolean hasOptions() {
        return options != null;
    }

    public Clause options() {
        return options;
    }

    public boolean hasConditions() {
        return conditions != null;
    }

    public Clause conditions() {
        return conditions;
    }

    public boolean hasCounterUpdate() {
        return counterUpdate;
    }

    public Update set(Assignment assignment) {
        if (assignment == null) {
            throw new NullPointerException("assignment");
        }
        if (this.assignment == null) {
            this.assignment = assignment;
        } else if (this.assignment instanceof Assignment.And) {
            ((Assignment.And)this.assignment).assignments().add(assignment);
        } else {
            Assignment.And and = new Assignment.And();
            and.assignments().add(this.assignment);
            and.assignments().add(assignment);
            this.assignment = and;
        }
        if (assignment instanceof Assignment.CounterAssignment) {
            counterUpdate = true;
        }
        return this;
    }

    public Update set(Assignment... assignments) {
        if (assignments == null) {
            throw new NullPointerException("assignments");
        }
        for (Assignment assignment : assignments) {
            set(assignment);
        }
        return this;
    }

    public Update where(Clause.Operator clause) {
        if (clause == null) {
            throw new NullPointerException("clause");
        }
        if (where == null) {
            where = clause;
        } else if (where instanceof Clause.And) {
            ((Clause.And)where).clauses().add(clause);
        } else {
            Clause.And and = new Clause.And();
            and.clauses().add(where);
            and.clauses().add(clause);
            where = and;
        }
        return this;
    }

    public Update where(Clause.Operator... clauses) {
        if (clauses == null) {
            throw new NullPointerException("clauses");
        }
        for (Clause.Operator clause : clauses) {
            where(clause);
        }
        return this;
    }

    public Update setIf(Clause.Equal... conditions) {
        if (conditions == null) {
            throw new NullPointerException("conditions");
        }
        for (Clause.Equal condition : conditions) {
            setIf(condition);
        }
        return this;
    }

    public Update setIf(Clause.Equal condition) {
        if (condition == null) {
            throw new NullPointerException("condition");
        }
        if (conditions == null) {
            conditions = condition;
        } else if (conditions instanceof Clause.And) {
            ((Clause.And)conditions).clauses().add(condition);
        } else {
            Clause.And and = new Clause.And();
            and.clauses().add(conditions);
            and.clauses().add(condition);
            conditions = and;
        }
        return this;
    }

    public Update usingTimestamp(long value) {
        return using(timestamp(value));
    }

    public Update usingTTL(int value) {
        return using(ttl(value));
    }

    public Update using(Clause.Using using) {
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
