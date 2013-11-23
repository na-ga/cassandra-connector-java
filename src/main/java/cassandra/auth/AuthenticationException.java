package cassandra.auth;

public class AuthenticationException extends RuntimeException {

    private static final long serialVersionUID = 6784561437210994565L;

    public AuthenticationException(Throwable cause) {
        super(cause);
    }

    public AuthenticationException() {
        super();
    }

    public AuthenticationException(String msg) {
        super(msg);
    }

    public AuthenticationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
