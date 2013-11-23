package cassandra.retry;

public interface RetryPolicy {

    public static final RetryPolicy DEFAULT = new MaxRetriesPolicy(ErrorCodeAwareRetryPolicy.INSTANCE);

    boolean canRetry(RetryContext context);

    public static abstract class Wrapper implements RetryPolicy {

        protected RetryPolicy retryPolicy;

        protected Wrapper(RetryPolicy retryPolicy) {
            if (retryPolicy == null) {
                throw new NullPointerException("retryPolicy");
            }
            this.retryPolicy = retryPolicy;
        }
    }
}
