package cassandra.cql.query;

public class Function {

    private String name;
    private Object[] parameters;

    public Function(String name, Object... parameters) {
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (name.isEmpty()) {
            throw new IllegalArgumentException("empty name");
        }
        this.name = name;
        this.parameters = parameters;
    }

    public String name() {
        return name;
    }

    public boolean hasParameters() {
        return parameters != null && parameters.length != 0;
    }

    public Object[] parameters() {
        return parameters;
    }
}
