package cassandra;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.util.internal.PlatformDependent.newConcurrentHashMap;

public class CassandraFutureMap implements Iterable<CassandraFuture> {

    private static final Throwable REQUEST_TIMEOUT = new TimeoutException();

    private final ConcurrentMap<Long, CassandraFuture> futures;
    private final AtomicInteger nextStreamId;

    public CassandraFutureMap() {
        futures = newConcurrentHashMap();
        nextStreamId = new AtomicInteger();
    }

    public CassandraFuture addFuture(CassandraFuture future) {
        int channelId = future.connection().hashCode();
        long streamId = encodeStreamId(channelId, nextStreamId());
        while (futures.putIfAbsent(streamId, future) != null) {
            streamId = encodeStreamId(channelId, nextStreamId());
        }
        future.request().setStreamId((int)streamId);
        return future;
    }

    public CassandraFuture removeFuture(Channel channel, int streamId) {
        return futures.remove(encodeStreamId(channel.hashCode(), streamId));
    }

    public List<CassandraFuture> clearExpiredFutures() {
        List<CassandraFuture> futureList = null;
        long now = System.currentTimeMillis();
        Iterator<CassandraFuture> iterator = iterator();
        while (iterator.hasNext()) {
            CassandraFuture future = iterator.next();
            if (future.isExpired(now)) {
                if (futureList == null) {
                    futureList = new ArrayList<CassandraFuture>();
                }
                future.setFailure(REQUEST_TIMEOUT);
                iterator.remove();
                futureList.add(future);
            }
        }
        if (futureList == null) {
            return Collections.emptyList();
        } else {
            return futureList;
        }
    }

    public List<CassandraFuture> fail(Channel channel, Throwable cause) {
        List<CassandraFuture> futureList = null;
        Iterator<CassandraFuture> iterator = iterator();
        while (iterator.hasNext()) {
            CassandraFuture future = iterator.next();
            if (future.connection().hashCode() == channel.hashCode()) {
                if (futureList == null) {
                    futureList = new ArrayList<CassandraFuture>();
                }
                future.setFailure(cause);
                iterator.remove();
                futureList.add(future);
            }
        }
        if (futureList == null) {
            return Collections.emptyList();
        } else {
            return futureList;
        }
    }

    public int nextStreamId() {
        return nextStreamId.getAndIncrement() & 0x7F;
    }

    @Override
    public Iterator<CassandraFuture> iterator() {
        return futures.values().iterator();
    }

    public static long encodeStreamId(int channelId, int streamId) {
        return ((long)channelId << 32) | (streamId & 0xFFFFFFFFL);
    }
}
