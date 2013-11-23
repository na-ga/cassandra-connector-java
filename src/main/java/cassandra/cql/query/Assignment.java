package cassandra.cql.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface Assignment {

    void accept(QueryVisitor visitor);

    public static abstract class AbstractAssignment<T> implements Assignment {

        protected String name;
        protected T value;

        protected AbstractAssignment(String name, T value) {
            if (name == null) {
                throw new NullPointerException("name");
            }
            if (name.isEmpty()) {
                throw new IllegalArgumentException("empty name");
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

    public static class And implements Assignment {

        private List<Assignment> assignments;

        public And() {
            this(new ArrayList<Assignment>());
        }

        public And(List<Assignment> assignments) {
            this.assignments = assignments;
        }

        public List<Assignment> assignments() {
            return assignments;
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class Set extends AbstractAssignment<Object> {

        public Set(String name, Object value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static interface CounterAssignment extends Assignment {
        // Tag
    }

    public static class Increment extends AbstractAssignment<Long> implements CounterAssignment {

        public Increment(String name, Long value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class Decrement extends AbstractAssignment<Long> implements CounterAssignment {

        public Decrement(String name, Long value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static interface CollectionAssignment extends Assignment {
        // Tag
    }

    public static class CollectionAdd extends AbstractAssignment<Collection<?>> implements CollectionAssignment {

        private boolean prepend;

        public CollectionAdd(String name, Collection<?> value, boolean prepend) {
            super(name, value);
            this.prepend = prepend;
        }

        public boolean isPrepend() {
            return prepend && value instanceof List;
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class CollectionRemove extends AbstractAssignment<Collection<?>> implements CollectionAssignment {

        public CollectionRemove(String name, Collection<?> value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class ListSet extends AbstractAssignment<Object> implements CollectionAssignment {

        private int index;

        public ListSet(String name, int index, Object value) {
            super(name, value);
            if (index < 0) {
                throw new IllegalArgumentException(String.format("index: %d (expected: >= 0)", index));
            }
            if (value instanceof List) {
                throw new IllegalArgumentException("nested type not allowed");
            }
            this.index = index;
        }

        public int index() {
            return index;
        }

        @Override
        public void accept(QueryVisitor visitor) {
            //
        }
    }

    public static class ListRemoveAt extends AbstractAssignment<Integer> implements CollectionAssignment {

        public ListRemoveAt(String name, Integer value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            //
        }
    }

    public static class MapPut extends AbstractAssignment<Object> {

        private Object key;

        public MapPut(String name, Object key, Object value) {
            super(name, value);
            if (key == null) {
                throw new NullPointerException("key");
            }
            this.key = key;
        }

        public Object key() {
            return key;
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class MapPutAll extends AbstractAssignment<Map<?, ?>> {

        public MapPutAll(String name, Map<?, ?> value) {
            super(name, value);
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }
}
