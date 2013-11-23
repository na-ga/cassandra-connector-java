package cassandra;

import cassandra.protocol.CassandraMessage.Request;
import cassandra.protocol.CassandraMessage.Response;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class CassandraFuture {

    public static interface Listener {

        public static final Listener CLOSE = new Listener() {
            @Override
            public void completed(CassandraFuture future) throws Exception {
                future.connection().close();
            }
        };

        public static final Listener CLOSE_ON_FAILURE = new Listener() {
            @Override
            public void completed(CassandraFuture future) throws Exception {
                if (!future.isSuccess()) {
                    future.connection().close();
                }
            }
        };

        void completed(CassandraFuture future) throws Exception;
    }

    public static final long DEADLINE = 10000;

    private final CassandraConnection connection;
    private final Request request;
    private final Promise<Response> promise;
    private final long createdAt;

    public CassandraFuture(EventExecutorGroup eventExecutor, CassandraConnection connection, Request request) {
        this.connection = connection;
        this.request = request;
        promise = new DefaultPromise<Response>(eventExecutor.next());
        createdAt = System.currentTimeMillis();
    }

    public CassandraConnection connection() {
        return connection;
    }

    public Request request() {
        return request;
    }

    public Future<Response> asJavaFuture() {
        return promise;
    }

    public CassandraFuture await() {
        await(DEADLINE);
        return this;
    }

    public boolean await(long timeout) {
        return promise.awaitUninterruptibly(timeout(timeout));
    }

    public Response get() {
        return get(promise, DEADLINE);
    }

    public Response get(long timeout) {
        return get(promise, timeout);
    }

    public boolean isDone() {
        return promise.isDone();
    }

    public boolean isSuccess() {
        return promise.isSuccess();
    }

    public boolean setSuccess(Response result) {
        return promise.trySuccess(result);
    }

    public Throwable cause() {
        return promise.cause();
    }

    public boolean setFailure(Throwable cause) {
        return promise.tryFailure(cause);
    }

    public boolean isExpired() {
        return isExpired(System.currentTimeMillis());
    }

    public boolean isExpired(long now) {
        return (createdAt + DEADLINE) < now;
    }

    public long createdAt() {
        return createdAt;
    }

    public CassandraFuture addListener(final Listener listener) {
        if (listener == null) {
            throw new NullPointerException("listener");
        }
        promise.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Response>>() {
            @Override
            public void operationComplete(io.netty.util.concurrent.Future<Response> future) throws Exception {
                listener.completed(CassandraFuture.this);
            }
        });
        return this;
    }

    static <T> T get(Promise<T> promise, long timeout) {
        Throwable cause;
        if (!promise.awaitUninterruptibly(timeout(timeout))) {
            cause = new TimeoutException();
        } else {
            cause = promise.cause();
        }
        if (cause != null) {
            PlatformDependent.throwException(cause);
        }
        return promise.getNow();
    }

    static long timeout(long timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException(String.format("timeout: %d (expected: >= 0)", timeout));
        }
        return Math.min(timeout, DEADLINE);
    }
}
