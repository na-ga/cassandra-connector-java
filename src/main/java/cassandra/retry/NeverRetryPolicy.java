package cassandra.retry;

public class NeverRetryPolicy implements RetryPolicy {

    @Override
    public boolean canRetry(RetryContext context) {
        return false;
    }
}
