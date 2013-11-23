package cassandra.retry;

import java.net.InetAddress;
import java.util.Iterator;

public class RetryContext {

    private final RetryPolicy retryPolicy;
    private final Iterator<InetAddress> activeEndpoints;
    private InetAddress currentEndpoint;
    private int retryCount;
    private Throwable lastException;

    public RetryContext(RetryPolicy retryPolicy, Iterator<InetAddress> activeEndpoints) {
        if (retryPolicy == null) {
            throw new NullPointerException("retryPolicy");
        }
        if (activeEndpoints == null) {
            throw new NullPointerException("activeEndpoints");
        }
        if (!activeEndpoints.hasNext()) {
            throw new IllegalArgumentException("empty activeEndpoints");
        }
        this.retryPolicy = retryPolicy;
        this.activeEndpoints = activeEndpoints;
        currentEndpoint = activeEndpoints.next();
    }

    public boolean canRetry() {
        return retryPolicy.canRetry(this);
    }

    public int getRetryCount() {
        return retryCount;
    }

    public InetAddress getCurrentEndpoint() {
        return currentEndpoint;
    }

    public InetAddress getNextEndpoint() {
        if (activeEndpoints.hasNext()) {
            currentEndpoint = activeEndpoints.next();
        } else {
            currentEndpoint = null;
        }
        return currentEndpoint;
    }

    public Throwable getLastThrowable() {
        return lastException;
    }

    public RetryContext setFailure(Throwable cause) {
        lastException = cause;
        retryCount++;
        return this;
    }
}
