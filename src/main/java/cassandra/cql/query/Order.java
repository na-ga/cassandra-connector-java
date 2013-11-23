package cassandra.cql.query;

public interface Order {

    void accept(QueryVisitor visitor);

    public static class Ascending implements Order {

        private String name;

        public Ascending(String name) {
            if (name == null) {
                throw new NullPointerException("name");
            }
            this.name = name;
        }

        public String name() {
            return name;
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class Descending implements Order {

        private String name;

        public Descending(String name) {
            if (name == null) {
                throw new NullPointerException("name");
            }
            this.name = name;
        }

        public String name() {
            return name;
        }

        @Override
        public void accept(QueryVisitor visitor) {
            visitor.visit(this);
        }
    }
}
