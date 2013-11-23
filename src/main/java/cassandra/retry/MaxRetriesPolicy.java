package cassandra.retry;

public class MaxRetriesPolicy extends RetryPolicy.Wrapper {

    public static final int DEFAULT_MAX_ATTEMPTS = 5;

    private final int maxAttempts;

    public MaxRetriesPolicy(RetryPolicy retryPolicy) {
        this(retryPolicy, DEFAULT_MAX_ATTEMPTS);
    }

    public MaxRetriesPolicy(RetryPolicy retryPolicy, int maxAttempts) {
        super(retryPolicy);
        if (maxAttempts < 0) {
            throw new IllegalArgumentException(String.format("maxAttempts: %d (expected: >= 0)", maxAttempts));
        }
        this.maxAttempts = maxAttempts;
    }

    @Override
    public boolean canRetry(RetryContext context) {
        return retryPolicy.canRetry(context) && context.getRetryCount() <= maxAttempts;
    }
}
