package cassandra.auth;

import java.nio.charset.Charset;

public class PlainTextAuthProvider implements AuthProvider {

    private final String username, password;

    public PlainTextAuthProvider(String username, String password) {
        if (username == null) {
            throw new NullPointerException("username");
        }
        if (username.isEmpty()) {
            throw new IllegalArgumentException("empty username");
        }
        if (password == null) {
            throw new NullPointerException("password");
        }
        this.username = username;
        this.password = password;
    }

    @Override
    public Authenticator newAuthenticator(String authenticator) throws AuthenticationException {
        return new PlainTextAuthenticator(username, password);
    }

    public static class PlainTextAuthenticator implements Authenticator {

        private static final byte NUL = 0;

        private final byte[] token;

        PlainTextAuthenticator(String username, String password) {
            byte[] user = username.getBytes(Charset.forName("UTF8"));
            byte[] pass = password.getBytes(Charset.forName("UTF8"));
            token = new byte[2 + user.length + pass.length];
            token[0] = NUL;
            System.arraycopy(user, 0, token, 1, user.length);
            token[pass.length + 1] = NUL;
            System.arraycopy(pass, 0, token, user.length + 2, pass.length);
        }

        @Override
        public byte[] initialResponse() {
            return token;
        }

        @Override
        public byte[] evaluateResponse(byte[] serverResponse) throws AuthenticationException {
            return null;
        }
    }
}
