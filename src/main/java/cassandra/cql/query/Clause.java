package cassandra.cql.query;

import java.util.ArrayList;
import java.util.List;

public interface Clause {

    void accept(QueryVisitor visitor);

    public static abstract class AbstractClause<T> implements Clause {

        protected String name;
        protected T value;

        protected AbstractClause(String name, T value) {
            if (name == null) {
                throw new NullPointerException("name");
            }
            if (name.isEmpty()) {
                throw new IllegalArgumentException("empty name");
            }
            if (value == null) {
                throw new NullPointerException("value");
            }
            this.name = name;
            this.value = value;
        }

        public String name() {
            return name;
        }

        public T value() {
            return value;
        }
    }

    public static class And implements Clause {

        private List<Clause> clauses;

        public And() {
            this(new ArrayList<Clause>());
        }

        public And(List<Clause> clauses) {
            this.clauses = clauses;
        }

        public List<Clause> clauses() {
            return clauses;
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class Selection implements Clause {

        private boolean distinct;
        private Selector[] selectors;

        public Selection(boolean distinct, Selector... selectors) {
            if (selectors == null) {
                throw new NullPointerException("selectors");
            }
            this.distinct = distinct;
            this.selectors = selectors;
        }

        public Selector[] selectors() {
            return selectors;
        }

        public boolean isDistinct() {
            return distinct;
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }

        public static class Selector {

            String name, alias;
            Object value;

            public String name() {
                if (alias != null) {
                    return name + " AS " + alias;
                } else {
                    return name;
                }
            }

            public boolean hasValue() {
                return value != null;
            }

            public Object value() {
                return value;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Selector selector = (Selector)o;
                return !(alias != null ? !alias.equals(selector.alias) : selector.alias != null) && !(name != null ? !name.equals(selector.name) : selector.name != null);
            }

            @Override
            public int hashCode() {
                int result = name != null ? name.hashCode() : 0;
                result = 31 * result + (alias != null ? alias.hashCode() : 0);
                return result;
            }
        }
    }

    public static class Using extends AbstractClause<Object> {

        public Using(String name, Object value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static interface Operator extends Clause {
        // Tag
    }

    public static class Equal extends AbstractClause<Object> implements Operator {

        public Equal(String name, Object value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class LessThan extends AbstractClause<Object> implements Operator {

        public LessThan(String name, Object value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class LessThanEquals extends AbstractClause<Object> implements Operator {

        public LessThanEquals(String name, Object value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class GreaterThan extends AbstractClause<Object> implements Operator {

        public GreaterThan(String name, Object value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class GreaterThanEquals extends AbstractClause<Object> implements Operator {

        public GreaterThanEquals(String name, Object value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class In extends AbstractClause<Object[]> implements Operator {

        public In(String name, Object[] values) {
            super(name, values);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }
}
