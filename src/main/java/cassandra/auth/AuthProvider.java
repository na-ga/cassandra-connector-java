package cassandra.auth;

public interface AuthProvider {

    public static final AuthProvider NULL_PROVIDER = new AuthProvider() {
        @Override
        public Authenticator newAuthenticator(String authenticator) throws AuthenticationException {
            throw new AuthenticationException(String.format("no matching authenticator found: %s", authenticator));
        }
    };

    Authenticator newAuthenticator(String authenticator) throws AuthenticationException;
}
