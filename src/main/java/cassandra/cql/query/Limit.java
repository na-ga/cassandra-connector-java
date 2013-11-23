package cassandra.cql.query;

public class Limit {

    private int count;

    public Limit(int count) {
        this.count = count;
    }

    public int count() {
        return count;
    }

    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }
}
