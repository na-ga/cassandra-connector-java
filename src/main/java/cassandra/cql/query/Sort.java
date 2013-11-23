package cassandra.cql.query;

import java.util.ArrayList;
import java.util.List;

public class Sort {

    private List<Order> orders;

    public Sort() {
        this(new ArrayList<Order>());
    }

    public Sort(List<Order> orders) {
        this.orders = orders;
    }

    public List<Order> orders() {
        return orders;
    }

    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }
}
