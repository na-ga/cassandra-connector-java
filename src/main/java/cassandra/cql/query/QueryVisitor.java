package cassandra.cql.query;

public interface QueryVisitor {

    void visit(Trigger node);

    void visit(Truncate node);

    void visit(Select node);

    void visit(Insert node);

    void visit(Update node);

    void visit(Delete node);

    void visit(Batch node);

    void visit(Clause.And node);

    void visit(Clause.Selection node);

    void visit(Clause.Using node);

    void visit(Clause.Equal node);

    void visit(Clause.LessThan node);

    void visit(Clause.LessThanEquals node);

    void visit(Clause.GreaterThan node);

    void visit(Clause.GreaterThanEquals node);

    void visit(Clause.In node);

    void visit(Assignment.And node);

    void visit(Assignment.Increment node);

    void visit(Assignment.Decrement node);

    void visit(Assignment.Set node);

    void visit(Assignment.CollectionAdd node);

    void visit(Assignment.CollectionRemove node);

    void visit(Assignment.ListSet node);

    void visit(Assignment.MapPut node);

    void visit(Assignment.MapPutAll node);

    void visit(Sort node);

    void visit(Order.Ascending node);

    void visit(Order.Descending node);

    void visit(Limit node);
}
