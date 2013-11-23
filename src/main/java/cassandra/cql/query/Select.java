package cassandra.cql.query;

import cassandra.cql.PagingState;

import java.util.LinkedList;

public class Select extends Query<Select> {

    private Clause.Selection selection;
    private Clause where;
    private Sort sort;
    private Limit limit;
    private boolean allowFiltering;

    Select(String keyspace, String table, Clause.Selection selection) {
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

    public Select where(Clause.Operator clause) {
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

    public Select where(Clause.Operator... clauses) {
        if (clauses == null) {
            throw new NullPointerException("clauses");
        }
        for (Clause.Operator clause : clauses) {
            where(clause);
        }
        return this;
    }

    public boolean hasSort() {
        return sort != null && !sort.orders().isEmpty();
    }

    public Sort sort() {
        return sort;
    }

    public Select orderBy(Order order) {
        if (order == null) {
            throw new NullPointerException("order");
        }
        if (sort == null) {
            sort = new Sort();
        }
        sort.orders().add(order);
        return this;
    }

    public Select orderBy(Order... orders) {
        if (orders == null) {
            throw new NullPointerException("orders");
        }
        for (Order order : orders) {
            orderBy(order);
        }
        return this;
    }

    public boolean hasLimit() {
        return limit != null && limit.count() >= 0;
    }

    public Limit limit() {
        return limit;
    }

    public Select limit(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException(String.format("LIMIT: %d (expected: >= 0)", limit));
        }
        this.limit = new Limit(limit);
        return this;
    }

    public Select pagingState(PagingState pagingState) {
        this.pagingState = pagingState;
        return this;
    }

    public boolean isAllowFiltering() {
        return allowFiltering;
    }

    public Select allowFiltering() {
        allowFiltering = true;
        return this;
    }

    @Override
    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }

    public static class Builder {

        protected LinkedList<Clause.Selection.Selector> selectors;
        protected boolean distinct;

        public Select from(String table) {
            return from(null, table);
        }

        public Select from(String keyspace, String table) {
            if (table == null) {
                throw new NullPointerException("table");
            }
            if (table.isEmpty()) {
                throw new IllegalArgumentException("empty table");
            }
            Clause.Selection selection = null;
            if (selectors != null && !selectors.isEmpty()) {
                selection = new Clause.Selection(distinct, selectors.toArray(new Clause.Selection.Selector[selectors.size()]));
            }
            return new Select(keyspace, table, selection);
        }
    }

    public static abstract class Selection extends Builder {

        public Selection distinct() {
            this.distinct = true;
            return this;
        }

        public Select.Builder all() {
            if (selectors != null && !selectors.isEmpty()) {
                throw new IllegalStateException("column not empty");
            }
            return this;
        }

        public Select.Builder count() {
            return count(null);
        }

        public Select.Builder count(String alias) {
            if (selectors != null && !selectors.isEmpty()) {
                throw new IllegalStateException("column not empty");
            }
            if (alias != null && !alias.isEmpty()) {
                column("COUNT(*)").as(alias);
            } else {
                column("COUNT(*)");
            }
            distinct = false;
            return this;
        }

        public abstract Selection.Builder column(String name);
        
        public Selection.Builder ttl(String name) {
            return column(functionAlias("TTL", name));
        }

        public Selection.Builder writeTime(String name) {
            return column(functionAlias("WRITETIME", name));
        }

        public Selection.Builder dateOf(String name) {
            return column(functionAlias("dateOf", name));
        }

        public Selection.Builder unixTimestampOf(String name) {
            return column(functionAlias("unixTimestampOf", name));
        }

        public Selection.Builder token(String name) {
            return column(functionAlias("token", name));
        }

        public Selection.Builder blobAsDecimal(String name) {
            return column(functionAlias("blobAsDecimal", name));
        }

        public Selection.Builder minTimeuuid(String name) {
            return column(functionAlias("minTimeuuid", name));
        }

        public Selection.Builder maxTimeuuid(String name) {
            return column(functionAlias("maxTimeuuid", name));
        }

        public Selection.Builder blobAsAscii(String name) {
            return column(functionAlias("blobAsAscii", name));
        }

        public Selection.Builder blobAsBigint(String name) {
            return column(functionAlias("blobAsBigint", name));
        }

        public Selection.Builder blobAsBoolean(String name) {
            return column(functionAlias("blobAsBoolean", name));
        }

        public Selection.Builder blobAsCounter(String name) {
            return column(functionAlias("blobAsCounter", name));
        }

        public Selection.Builder blobAsDouble(String name) {
            return column(functionAlias("blobAsDouble", name));
        }

        public Selection.Builder blobAsFloat(String name) {
            return column(functionAlias("blobAsFloat", name));
        }

        public Selection.Builder blobAsInet(String name) {
            return column(functionAlias("blobAsInet", name));
        }

        public Selection.Builder blobAsInt(String name) {
            return column(functionAlias("blobAsInt", name));
        }

        public Selection.Builder blobAsText(String name) {
            return column(functionAlias("blobAsText", name));
        }

        public Selection.Builder blobAsTimestamp(String name) {
            return column(functionAlias("blobAsTimestamp", name));
        }

        public Selection.Builder blobAsUuid(String name) {
            return column(functionAlias("blobAsUuid", name));
        }

        public Selection.Builder blobAsVarchar(String name) {
            return column(functionAlias("blobAsVarchar", name));
        }

        public Selection.Builder blobAsVarint(String name) {
            return column(functionAlias("blobAsVarint", name));
        }

        public Selection.Builder blobAsTimeUuid(String name) {
            return column(functionAlias("blobAsTimeUuid", name));
        }

        public Selection.Builder asciiAsBlob(String name) {
            return column(functionAlias("asciiAsBlob", name));
        }

        public Selection.Builder bigintAsBlob(String name) {
            return column(functionAlias("bigintAsBlob", name));
        }

        public Selection.Builder booleanAsBlob(String name) {
            return column(functionAlias("booleanAsBlob", name));
        }

        public Selection.Builder counterAsBlob(String name) {
            return column(functionAlias("counterAsBlob", name));
        }

        public Selection.Builder decimalAsBlob(String name) {
            return column(functionAlias("decimalAsBlob", name));
        }

        public Selection.Builder doubleAsBlob(String name) {
            return column(functionAlias("doubleAsBlob", name));
        }

        public Selection.Builder floatAsBlob(String name) {
            return column(functionAlias("floatAsBlob", name));
        }

        public Selection.Builder inetAsBlob(String name) {
            return column(functionAlias("inetAsBlob", name));
        }

        public Selection.Builder intAsBlob(String name) {
            return column(functionAlias("intAsBlob", name));
        }

        public Selection.Builder textAsBlob(String name) {
            return column(functionAlias("textAsBlob", name));
        }

        public Selection.Builder timestampAsBlob(String name) {
            return column(functionAlias("timestampAsBlob", name));
        }

        public Selection.Builder uuidAsBlob(String name) {
            return column(functionAlias("uuidAsBlob", name));
        }

        public Selection.Builder varcharAsBlob(String name) {
            return column(functionAlias("varcharAsBlob", name));
        }

        public Selection.Builder varintAsBlob(String name) {
            return column(functionAlias("varintAsBlob", name));
        }

        public Selection.Builder timeUuidAsBlob(String name) {
            return column(functionAlias("timeUuidAsBlob", name));
        }

        protected String functionAlias(String function, String name) {
            return String.format("%s(%s)", function, name);
        }
        
        public static class Builder extends Selection {

            public Selection.Builder column(String name) {
                if (name == null) {
                    throw new NullPointerException("name");
                }
                if (name.isEmpty()) {
                    throw new IllegalArgumentException("empty name");
                }
                Clause.Selection.Selector selector = new Clause.Selection.Selector();
                selector.name = name;
                if (selectors == null) {
                    selectors = new LinkedList<Clause.Selection.Selector>();
                }
                selectors.addLast(selector);
                return this;
            }
            
            public Selection as(String alias) {
                if (alias == null) {
                    throw new NullPointerException("alias");
                }
                if (alias.isEmpty()) {
                    throw new IllegalArgumentException("empty alias");
                }
                Clause.Selection.Selector selector = selectors.getLast();
                if (selector == null) {
                    throw new IllegalStateException("column is empty");
                }
                selector.alias = alias;
                return this;
            }
        }
    }
}
