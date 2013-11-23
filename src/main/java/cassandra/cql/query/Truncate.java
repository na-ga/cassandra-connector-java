package cassandra.cql.query;

public class Truncate extends Query<Truncate> {

    Truncate(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    @Override
    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }
}
