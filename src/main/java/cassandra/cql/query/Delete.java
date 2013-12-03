package cassandra.cql.query;

import java.util.LinkedList;

public class Delete extends Query<Delete> {

    private Clause.Selection selection;
    private Clause where;
    private Clause options;

    Delete(String keyspace, String table, Clause.Selection selection) {
        this.keyspace = keyspace;
        this.table = table;
        this.selection = selection;
    }

    public boolean hasSelectionClause() {
        return selection != null;
    }

    public Clause selectionClause() {
        return selection;
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

    public Delete usingTimestamp(long value) {
        options = timestamp(value);
        return this;
    }

    public Delete where(Clause.Equal clause) {
        return where0(clause);
    }

    public Delete where(Clause.In clause) {
        return where0(clause);
    }

    private Delete where0(Clause.Operator clause) {
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

    @Override
    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }

    public static class Builder {

        protected LinkedList<Clause.Selection.Selector> selectors;

        public Delete from(String table) {
            return from(null, table);
        }

        public Delete from(String keyspace, String table) {
            if (table == null) {
                throw new NullPointerException("table");
            }
            if (table.isEmpty()) {
                throw new IllegalArgumentException("empty table");
            }
            Clause.Selection selection = null;
            if (selectors != null) {
                selection = new Clause.Selection(false, selectors.toArray(new Clause.Selection.Selector[selectors.size()]));
            }
            return new Delete(keyspace, table, selection);
        }
    }

    public static class Selection extends Builder {

        public Delete.Builder all() {
            if (selectors != null && !selectors.isEmpty()) {
                throw new IllegalStateException("column not empty");
            }
            return this;
        }

        public Selection column(String name) {
            if (name == null) {
                throw new NullPointerException("name");
            }
            if (name.isEmpty()) {
                throw new IllegalArgumentException("empty name");
            }
            addLast(name);
            return this;
        }

        public Selection list(String name, int index) {
            if (name == null) {
                throw new NullPointerException("name");
            }
            if (name.isEmpty()) {
                throw new IllegalArgumentException("empty name");
            }
            if (index < 0) {
                throw new IllegalArgumentException(String.format("%s[index]: %d (expected: >= 0)", name, index));
            }
            addLast(name).value = index;
            return this;
        }

        public Selection map(String name, Object key) {
            if (name == null) {
                throw new NullPointerException("name");
            }
            if (name.isEmpty()) {
                throw new IllegalArgumentException("empty name");
            }
            if (key == null) {
                throw new NullPointerException("key");
            }
            addLast(name).value = key;
            return this;
        }

        private Clause.Selection.Selector addLast(String name) {
            if (selectors == null) {
                selectors = new LinkedList<Clause.Selection.Selector>();
            }
            Clause.Selection.Selector selector = new Clause.Selection.Selector();
            selectors.addLast(selector);
            selector.name = name;
            return selector;
        }
    }
}
