package cassandra.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingRetryPolicy extends RetryPolicy.Wrapper {

    public static enum LogLevel {

        TRACE, DEBUG, INFO, WARN, ERROR
    }

    private final Logger logger;
    private final LogLevel level;

    public LoggingRetryPolicy(RetryPolicy retryPolicy) {
        this(retryPolicy, LoggingRetryPolicy.class);
    }

    public LoggingRetryPolicy(RetryPolicy retryPolicy, String name) {
        this(retryPolicy, name, LogLevel.INFO);
    }

    public LoggingRetryPolicy(RetryPolicy retryPolicy, Class<?> clazz) {
        this(retryPolicy, clazz, LogLevel.INFO);
    }

    public LoggingRetryPolicy(RetryPolicy retryPolicy, String name, LogLevel level) {
        super(retryPolicy);
        logger = LoggerFactory.getLogger(name);
        this.level = level;
    }

    public LoggingRetryPolicy(RetryPolicy retryPolicy, Class<?> clazz, LogLevel level) {
        super(retryPolicy);
        logger = LoggerFactory.getLogger(clazz);
        this.level = level;
    }

    @Override
    public boolean canRetry(RetryContext context) {
        boolean canRetry = retryPolicy.canRetry(context);

        if (canRetry) {
            log(level, "");
        } else {
            log(level, "");
        }

        return canRetry;
    }

    private void log(LogLevel level, String msg, Throwable cause) {
        switch (level) {
            case TRACE:
                logger.trace(msg, cause);
                break;
            case DEBUG:
                logger.debug(msg, cause);
                break;
            case INFO:
                logger.info(msg, cause);
                break;
            case WARN:
                logger.warn(msg, cause);
                break;
            case ERROR:
                logger.error(msg, cause);
                break;
            default:
                throw new Error();
        }
    }

    private void log(LogLevel level, String msg) {
        switch (level) {
            case TRACE:
                logger.trace(msg);
                break;
            case DEBUG:
                logger.debug(msg);
                break;
            case INFO:
                logger.info(msg);
                break;
            case WARN:
                logger.warn(msg);
                break;
            case ERROR:
                logger.error(msg);
                break;
            default:
                throw new Error();
        }
    }

    private void log(LogLevel level, String format, Object arg) {
        switch (level) {
            case TRACE:
                logger.trace(format, arg);
                break;
            case DEBUG:
                logger.debug(format, arg);
                break;
            case INFO:
                logger.info(format, arg);
                break;
            case WARN:
                logger.warn(format, arg);
                break;
            case ERROR:
                logger.error(format, arg);
                break;
            default:
                throw new Error();
        }
    }

    private void log(LogLevel level, String format, Object argA, Object argB) {
        switch (level) {
            case TRACE:
                logger.trace(format, argA, argB);
                break;
            case DEBUG:
                logger.debug(format, argA, argB);
                break;
            case INFO:
                logger.info(format, argA, argB);
                break;
            case WARN:
                logger.warn(format, argA, argB);
                break;
            case ERROR:
                logger.error(format, argA, argB);
                break;
            default:
                throw new Error();
        }
    }

    private void log(LogLevel level, String format, Object... arguments) {
        switch (level) {
            case TRACE:
                logger.trace(format, arguments);
                break;
            case DEBUG:
                logger.debug(format, arguments);
                break;
            case INFO:
                logger.info(format, arguments);
                break;
            case WARN:
                logger.warn(format, arguments);
                break;
            case ERROR:
                logger.error(format, arguments);
                break;
            default:
                throw new Error();
        }
    }
}
