package cassandra.cql.type;

public class CQL3TypeError extends RuntimeException {

    private static final long serialVersionUID = 5896674397393773493L;

    public CQL3TypeError() {
    }

    public CQL3TypeError(String msg) {
        super(msg);
    }

    public CQL3TypeError(String msg, Throwable cause) {
        super(msg, cause);
    }

    public CQL3TypeError(Throwable cause) {
        super(cause);
    }
}
