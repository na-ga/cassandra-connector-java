package cassandra.cql.query;

import cassandra.cql.RoutingKey;
import cassandra.metadata.ColumnMetadata;
import cassandra.metadata.TableMetadata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryBuilder implements QueryVisitor {

    private final List<ColumnMetadata> partitionKey;
    private final ByteBuffer[] routingKey;
    private final StringBuilder query;
    private List<Object> parameters;

    public QueryBuilder(TableMetadata table) {
        if (table != null) {
            partitionKey = table.getPartitionKey();
            routingKey = new ByteBuffer[partitionKey.size()];
        } else {
            partitionKey = null;
            routingKey = null;
        }
        query = new StringBuilder();
    }

    public boolean hasRoutingKey() {
        if (routingKey == null || routingKey.length == 0) {
            return false;
        }
        for (ByteBuffer key : routingKey) {
            if (key == null) {
                return false;
            }
        }
        return true;
    }

    public RoutingKey routingKey() {
        if (!hasRoutingKey()) {
            return null;
        }
        return RoutingKey.copyFrom(routingKey);
    }

    public boolean hasParameters() {
        return parameters != null && !parameters.isEmpty();
    }

    public List<Object> parameters() {
        return parameters;
    }

    public String build() {
        return query.toString();
    }

    @Override
    public void visit(Trigger node) {
        //
    }

    @Override
    public void visit(Truncate node) {
        query.append("TRUNCATE ");
        if (node.hasKeyspace()) {
            query.append(node.keyspace).append(".");
        }
        query.append(node.table);
    }

    @Override
    public void visit(Select node) {
        query.append("SELECT ");
        if (node.hasSelectionClause()) {
            node.selectionClause().accept(this);
        } else {
            query.append("*");
        }
        query.append(" FROM ");
        if (node.hasKeyspace()) {
            query.append(node.keyspace).append(".");
        }
        query.append(node.table);
        if (node.hasWhereClause()) {
            query.append(" WHERE ");
            node.whereClause().accept(this);
        }
        if (node.hasSort()) {
            node.sort().accept(this);
        }
        if (node.hasLimit()) {
            node.limit().accept(this);
        }
        if (node.isAllowFiltering()) {
            query.append(" ALLOW FILTERING");
        }
    }

    @Override
    public void visit(Insert node) {
        query.append("INSERT INTO ");
        if (node.hasKeyspace()) {
            query.append(node.keyspace).append(".");
        }
        query.append(node.table);
        query.append("(");
        boolean first = true;
        for (String name : node.names()) {
            if (first) {
                first = false;
            } else {
                query.append(",");
            }
            query.append(name);
        }
        query.append(") VALUES (");
        for (int i = 0; i < node.names().size(); i++) {
            if (i != 0) {
                query.append(",");
            }
            appendValue(node.names().get(i), node.values().get(i));
        }
        query.append(")");
        if (node.isIfNotExists()) {
            query.append(" IF NOT EXISTS");
        }
        if (node.hasOptions()) {
            query.append(" USING ");
            node.options().accept(this);
        }
    }

    @Override
    public void visit(Update node) {
        query.append("UPDATE ");
        if (node.hasKeyspace()) {
            query.append(node.keyspace).append(".");
        }
        query.append(node.table);
        if (node.hasOptions()) {
            query.append(" USING ");
            node.options().accept(this);
        }
        query.append(" SET ");
        node.assignment().accept(this);
        if (node.hasWhereClause()) {
            query.append(" WHERE ");
            node.whereClause().accept(this);
        }
        if (node.hasConditions()) {
            query.append(" IF ");
            node.conditions().accept(this);
        }
    }

    @Override
    public void visit(Delete node) {
        query.append("DELETE ");
        if (node.hasSelectionClause()) {
            node.selectionClause().accept(this);
        }
        query.append(" FROM ");
        if (node.hasKeyspace()) {
            query.append(node.keyspace).append(".");
        }
        query.append(node.table);
        if (node.hasOptions()) {
            query.append(" USING ");
            node.options().accept(this);
        }
        if (node.hasWhereClause()) {
            query.append(" WHERE ");
            node.whereClause().accept(this);
        }
    }

    @Override
    public void visit(Batch node) {
        if (node.isCounterUpdate()) {
            query.append("BEGIN COUNTER BATCH");
        } else {
            if (node.isLogged()) {
                query.append("BEGIN BATCH");
            } else {
                query.append("BEGIN UNLOGGED BATCH");
            }
        }
        if (node.hasOptions()) {
            query.append(" USING ");
            node.options().accept(this);
        }
        query.append(" ");
        for (Query each : node.queries()) {
            each.accept(this);
            query.append(";");
        }
        query.append("APPLY BATCH;");
    }

    @Override
    public void visit(Clause.And node) {
        boolean first = true;
        for (Clause each : node.clauses()) {
            if (first) {
                first = false;
            } else {
                query.append(" AND ");
            }
            each.accept(this);
        }
    }

    @Override
    public void visit(Clause.Selection node) {
        if (node.isDistinct()) {
            query.append(" DISTINCT ");
        }
        boolean first = true;
        for (Clause.Selection.Selector selector : node.selectors()) {
            if (first) {
                first = false;
            } else {
                query.append(",");
            }
            query.append(selector.name());
            if (selector.hasValue()) {
                query.append("[");
                appendValue(selector.value());
                query.append("]");
            }
        }
    }

    @Override
    public void visit(Clause.Using node) {
        query.append(node.name()).append(" ?");
        addParameter(node.value());
    }

    @Override
    public void visit(Clause.Equal node) {
        query.append(node.name()).append("=");
        appendValue(node.name(), node.value());
    }

    @Override
    public void visit(Clause.LessThan node) {
        query.append(node.name()).append("<");
        appendValue(node.name(), node.value());
    }

    @Override
    public void visit(Clause.LessThanEquals node) {
        query.append(node.name()).append("<=");
        appendValue(node.name(), node.value());
    }

    @Override
    public void visit(Clause.GreaterThan node) {
        query.append(node.name()).append(">");
        appendValue(node.name(), node.value());
    }

    @Override
    public void visit(Clause.GreaterThanEquals node) {
        query.append(node.name()).append(">=");
        appendValue(node.name(), node.value());
    }

    @Override
    public void visit(Clause.In node) {
        query.append(node.name()).append(" IN (");
        boolean first = true;
        for (Object each : node.value()) {
            if (first) {
                first = false;
                appendValue(node.name(), each);
            } else {
                query.append(",");
                appendValue(each);
            }
        }
        query.append(")");
    }

    @Override
    public void visit(Assignment.And node) {
        boolean first = true;
        for (Assignment each : node.assignments()) {
            if (first) {
                first = false;
            } else {
                query.append(",");
            }
            each.accept(this);
        }
    }

    @Override
    public void visit(Assignment.Increment node) {
        query.append(node.name()).append("=+?"); // TODO with func
        addParameter(node.value());
    }

    @Override
    public void visit(Assignment.Decrement node) {
        query.append(node.name()).append("=-?"); // TODO with func
        addParameter(node.value());
    }

    @Override
    public void visit(Assignment.Set node) {
        query.append(node.name()).append("=");
        appendValue(node.value());
    }

    @Override
    public void visit(Assignment.CollectionAdd node) {
        query.append(node.name()).append("=");
        if (node.isPrepend()) {
            query.append("?+").append(node.name());
        } else {
            query.append(node.name()).append("+?");
        }
        addParameter(node.value());
    }

    @Override
    public void visit(Assignment.CollectionRemove node) {
        query.append(node.name()).append("=");
        query.append(node.name()).append("-?");
        addParameter(node.value());
    }

    @Override
    public void visit(Assignment.ListSet node) {
        query.append(node.name()).append("[?]=");
        addParameter(node.index());
        appendValue(node.value());
    }

    @Override
    public void visit(Assignment.MapPut node) {
        query.append(node.name());
        query.append("[?]=");
        addParameter(node.key());
        appendValue(node.value());
    }

    @Override
    public void visit(Assignment.MapPutAll node) {
        query.append(node.name()).append("=");
        query.append(node.name()).append("+");
        appendValue(node.value());
    }

    @Override
    public void visit(Sort node) {
        query.append(" ORDER BY ");
        boolean first = true;
        for (Order each : node.orders()) {
            if (first) {
                first = false;
            } else {
                query.append(",");
            }
            each.accept(this);
        }
    }

    @Override
    public void visit(Order.Ascending node) {
        query.append(node.name());
    }

    @Override
    public void visit(Order.Descending node) {
        query.append(node.name()).append(" DESC");
    }

    @Override
    public void visit(Limit node) {
        query.append(" LIMIT ?");
        addParameter(node.count());
    }

    private QueryBuilder appendValue(Object value) {
        return appendValue(null, value);
    }

    private QueryBuilder appendValue(String name, Object value) {
        if (value instanceof Function) {
            Function function = (Function)value;
            query.append(function.name()).append("(");
            if (function.hasParameters()) {
                boolean first = true;
                for (Object each : function.parameters()) {
                    if (first) {
                        first = false;
                    } else {
                        query.append(",");
                    }
                    query.append("?");
                    addParameter(each);
                }
            }
            query.append(")");
        } else {
            query.append("?");
            addParameter(name, value);
        }
        return this;
    }

    private QueryBuilder addParameter(Object value) {
        return addParameter(null, value);
    }

    private QueryBuilder addParameter(String name, Object value) {
        if (parameters == null) {
            parameters = new ArrayList<Object>();
        }
        parameters.add(value);
        if (partitionKey != null && !partitionKey.isEmpty() && name != null && value != null) {
            for (int i = 0; i < partitionKey.size(); i++) {
                ColumnMetadata column = partitionKey.get(i);
                if (name.equals(column.getName())) {
                    routingKey[i] = column.getCqlType().serialize(value);
                }
            }
        }
        return this;
    }
}
