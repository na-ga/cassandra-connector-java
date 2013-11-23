package cassandra.auth;

public interface Authenticator {

    byte[] initialResponse();

    byte[] evaluateResponse(byte[] serverResponse) throws AuthenticationException;
}
