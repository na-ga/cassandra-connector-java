package cassandra.cql;

public enum Consistency {

    ANY(0),
    ONE(1),
    TWO(2),
    THREE(3),
    QUORUM(4),
    ALL(5),
    LOCAL_QUORUM(6),
    EACH_QUORUM(7),
    SERIAL(8),
    LOCAL_SERIAL(9),
    LOCAL_ONE(10);

    public final int code;

    private Consistency(int code) {
        this.code = code;
    }

    public static Consistency valueOf(int code) {
        for (Consistency consistency : Consistency.values()) {
            if (consistency.code == code) {
                return consistency;
            }
        }
        throw new IllegalStateException(String.format("unknown consistency level %d", code));
    }
}
