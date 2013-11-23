package cassandra;

import cassandra.protocol.CassandraMessageCodec;
import cassandra.protocol.CassandraMessageHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.*;

import javax.net.ssl.SSLEngine;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CassandraDriver {

    public static final String CQL_VERSION = "3.1.1";
    public static final String NATIVE_PROTOCOL_VERSION = "2";
    public static final int NATIVE_PROTOCOL_VERSION_NUMBER = Integer.parseInt(NATIVE_PROTOCOL_VERSION);

    private static final int FUTURE_MAP_CLEAN_INTERVAL_SECS = 10;
    private static final AtomicReference<EventExecutorGroup> GLOBAL_EVENT_EXECUTOR;

    static {
        GLOBAL_EVENT_EXECUTOR = new AtomicReference<EventExecutorGroup>(GlobalEventExecutor.INSTANCE);
    }

    private final EventLoopGroup worker;
    private final LoggingHandler tracer;
    private final CassandraFutureMap futureMap;
    private final CassandraMessageHandler handler;
    private final ShutdownListener shutdownListener;

    public CassandraDriver() {
        this(0);
    }

    public CassandraDriver(int workers) {
        this(new NioEventLoopGroup(workers, new DefaultThreadFactory("cassandra-worker")));
    }

    public CassandraDriver(EventLoopGroup worker) {
        if (worker == null) {
            throw new NullPointerException("worker");
        }
        if (worker.isShuttingDown() || worker.isShutdown()) {
            throw new IllegalArgumentException("worker shutdown");
        }
        this.worker = worker;
        tracer = new LoggingHandler(CassandraConnection.class, LogLevel.TRACE);
        futureMap = new CassandraFutureMap();
        handler = new CassandraMessageHandler(futureMap);
        shutdownListener = new ShutdownListener();
        worker.terminationFuture().addListener(shutdownListener);
        startCleaner();
    }

    public CassandraCluster.Builder newClusterBuilder() {
        return CassandraCluster.newBuilder().setDriver(this);
    }

    public CassandraConnection newConnection(InetAddress inetAddress) {
        return newConnection(inetAddress, CassandraOptions.DEFAULT);
    }

    public CassandraConnection newConnection(InetAddress inetAddress, CassandraOptions options) {
        return newConnection(new InetSocketAddress(inetAddress, options.getPort()), options);
    }

    public CassandraConnection newConnection(InetSocketAddress socketAddress) {
        return newConnection(socketAddress, CassandraOptions.DEFAULT);
    }

    public CassandraConnection newConnection(InetSocketAddress socketAddress, CassandraOptions options) {
        if (isShutdown()) {
            throw new IllegalStateException("driver shutdown");
        }
        Channel channel = new NioSocketChannel();
        channel.pipeline().addLast(tracer);
        if (options.getSslContext() != null) {
            SSLEngine engine = options.getSslContext().createSSLEngine();
            engine.setUseClientMode(true);
            if (options.getCipherSuites() != null) {
                engine.setEnabledCipherSuites(options.getCipherSuites());
            }
            channel.pipeline().addLast(new SslHandler(engine));
        }
        channel.pipeline().addLast(new CassandraMessageCodec(options.getCompression()));
        channel.pipeline().addLast(handler);
        channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
        channel.config().setOption(ChannelOption.TCP_NODELAY, true);
        channel.config().setOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.getConnectTimeoutMillis());
        ChannelFuture registerFuture = worker.register(channel);
        if (registerFuture.cause() != null) {
            if (channel.isRegistered()) {
                channel.close();
            } else {
                channel.unsafe().closeForcibly();
            }
            throw new IllegalStateException(registerFuture.cause().getMessage());
        }
        return new CassandraConnection(options, futureMap, channel, socketAddress);
    }

    public CassandraFutureMap futureMap() {
        return futureMap;
    }

    public boolean isShutdown() {
        return worker.isShuttingDown() || worker.isShutdown();
    }

    public void shutdown() {
        worker.shutdownGracefully();
        shutdownEventExecutor(GLOBAL_EVENT_EXECUTOR.get());
    }

    public void addShutdownHook(CassandraCluster cluster) {
        shutdownListener.clusters.add(cluster);
    }

    public void removeShutdownHook(CassandraCluster cluster) {
        shutdownListener.clusters.remove(cluster);
    }

    private void startCleaner() {
        worker.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                futureMap.clearExpiredFutures();
            }
        }, FUTURE_MAP_CLEAN_INTERVAL_SECS, FUTURE_MAP_CLEAN_INTERVAL_SECS, TimeUnit.SECONDS);
    }

    public static EventExecutorGroup getGlobalEventExecutor() {
        return GLOBAL_EVENT_EXECUTOR.get();
    }

    public static boolean setGlobalEventExecutor(int nThread) {
        return setGlobalEventExecutor(nThread, "cassandra-eventexecutor");
    }

    public static boolean setGlobalEventExecutor(int nThread, String name) {
        if (nThread < 1) {
            throw new IllegalArgumentException(String.format("nThread: %d (expected: >= 1)", nThread));
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (name.isEmpty()) {
            throw new IllegalArgumentException("empty name");
        }
        return setGlobalEventExecutor(new DefaultEventExecutorGroup(nThread, new DefaultThreadFactory(name)));
    }

    public static boolean setGlobalEventExecutor(EventExecutorGroup globalEventExecutor) {
        if (globalEventExecutor == null) {
            throw new NullPointerException("globalEventExecutor");
        }
        if (!(globalEventExecutor instanceof GlobalEventExecutor)) {
            if (globalEventExecutor.isShuttingDown() || globalEventExecutor.isShutdown()) {
                throw new IllegalArgumentException("globalEventExecutor shutdown");
            }
        }
        EventExecutorGroup old = GLOBAL_EVENT_EXECUTOR.get();
        boolean updated = GLOBAL_EVENT_EXECUTOR.compareAndSet(old, globalEventExecutor);
        if (updated) {
            shutdownEventExecutor(old);
        }
        return updated;
    }

    private static void shutdownEventExecutor(EventExecutorGroup eventExecutor) {
        if (eventExecutor != GlobalEventExecutor.INSTANCE) {
            eventExecutor.shutdownGracefully();
        }
    }

    private class ShutdownListener implements GenericFutureListener<Future<Object>> {

        private Queue<CassandraCluster> clusters = new LinkedList<CassandraCluster>();

        @Override
        public void operationComplete(Future<Object> future) throws Exception {
            try {
                while (!clusters.isEmpty()) {
                    CassandraCluster cluster = clusters.remove();
                    if (cluster != null) {
                        cluster.close();
                    }
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
