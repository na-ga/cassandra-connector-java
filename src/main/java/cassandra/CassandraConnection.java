package cassandra;

import cassandra.CassandraOptions.Compression;
import cassandra.auth.AuthProvider;
import cassandra.auth.Authenticator;
import cassandra.protocol.CassandraMessage.*;
import cassandra.protocol.CassandraMessage.Event.Type;
import cassandra.protocol.CassandraMessageHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static cassandra.CassandraDriver.CQL_VERSION;

public class CassandraConnection {

    public static interface StateListener {

        public static final StateListener DEFAULT = new Adapter() {
        };

        void onOpen(CassandraConnection connection);

        void onOpenFail(CassandraConnection connection, Throwable cause);

        void onClose(CassandraConnection connection);

        void onRegister(CassandraConnection connection, List<Type> events);

        void onRegisterFail(CassandraConnection connection, List<Type> events, Throwable cause);

        void onUnregister(CassandraConnection connection);

        void onEvent(CassandraConnection connection, Event event);

        public static abstract class Adapter implements StateListener {

            @Override
            public void onOpen(CassandraConnection connection) {
            }

            @Override
            public void onOpenFail(CassandraConnection connection, Throwable cause) {
            }

            @Override
            public void onClose(CassandraConnection connection) {
            }

            @Override
            public void onRegister(CassandraConnection connection, List<Type> events) {
            }

            @Override
            public void onRegisterFail(CassandraConnection connection, List<Type> events, Throwable cause) {
            }

            @Override
            public void onUnregister(CassandraConnection connection) {
            }

            @Override
            public void onEvent(CassandraConnection connection, Event event) {
            }
        }
    }

    private static final RegisterHandler REGISTER_HANDLER = new RegisterHandler();

    private final CassandraOptions options;
    private final CassandraFutureMap futureMap;
    private final Channel channel;
    private final InetSocketAddress remoteAddress;
    private final Initializer initializer;
    private final AtomicBoolean started, registered, closed;
    private final AtomicReference<String> keyspace;
    private StateListener listener;

    CassandraConnection(CassandraOptions options, CassandraFutureMap futureMap, Channel channel, InetSocketAddress remoteAddress) {
        this.options = options;
        this.futureMap = futureMap;
        this.channel = channel;
        this.remoteAddress = remoteAddress;
        initializer = new Initializer();
        started = new AtomicBoolean();
        registered = new AtomicBoolean();
        closed = new AtomicBoolean();
        keyspace = new AtomicReference<String>(null);
    }

    public CassandraOptions options() {
        return options;
    }

    public boolean isActive() {
        return started.get() && !closed.get();
    }

    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    public CassandraConnection open(StateListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener");
        }
        if (started.compareAndSet(false, true)) {
            this.listener = listener;
            channel.closeFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    closed.set(true);

                    if (registered.compareAndSet(true, false)) {
                        CassandraConnection.this.listener.onUnregister(CassandraConnection.this);
                    }
                    if (initializer.startupFuture.isSuccess()) {
                        CassandraConnection.this.listener.onClose(CassandraConnection.this);
                    }
                }
            });
            final ChannelPromise connectPromise = channel.newPromise();
            channel.eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    channel.connect(remoteAddress, connectPromise);
                    connectPromise.addListener(ChannelFutureListener.CLOSE_ON_FAILURE).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                initializer.run();
                            } else {
                                initializer.fail(future.cause());
                            }
                        }
                    });
                }
            });
        }
        return this;
    }

    public CassandraFuture openFuture() {
        return initializer.startupFuture;
    }

    public CassandraFuture send(Request request) {
        if (!started.get()) {
            throw new NotYetConnectedException();
        }
        CassandraFuture future = newFuture(request);
        if (!initializer.startupFuture.await().isSuccess()) {
            future.setFailure(initializer.startupFuture.cause());
            return future;
        }
        if (!channel.isActive()) {
            future.setFailure(new ClosedChannelException());
            return future;
        }
        request.setCompression(options.getCompression() != Compression.NONE);
        futureMap.addFuture(future);
        channel.writeAndFlush(request);
        return future;
    }

    public String keyspace() {
        return keyspace.get();
    }

    public boolean keyspace(String keyspace) {
        if (keyspace == null) {
            throw new NullPointerException("keyspace");
        }
        if (keyspace.isEmpty()) {
            throw new IllegalArgumentException("empty keyspace");
        }
        String old = this.keyspace.get();
        if (this.keyspace.compareAndSet(old, keyspace)) {
            send(new Query("USE " + keyspace, QueryParameters.DEFAULT)).addListener(CassandraFuture.Listener.CLOSE_ON_FAILURE);
            return true;
        }
        return false;
    }

    public boolean isRegistered() {
        return registered.get();
    }

    public void register(Event.Type... eventTypes) {
        register(false, eventTypes);
    }

    public void register(boolean sync, Event.Type... eventTypes) {
        if (registered.compareAndSet(false, true)) {
            List<Type> events;
            if (eventTypes == null || eventTypes.length == 0) {
                events = Arrays.asList(Event.Type.TOPOLOGY_CHANGE, Event.Type.STATUS_CHANGE, Event.Type.SCHEMA_CHANGE);
            } else {
                events = new ArrayList<Type>(eventTypes.length);
                for (Event.Type event : eventTypes) {
                    if (!events.contains(event)) {
                        events.add(event);
                    }
                }
            }
            CassandraFuture future = send(new Register(events)).addListener(REGISTER_HANDLER);
            if (sync) {
                future.await();
            }
        }
    }

    public void handleEvent(Event event) {
        if (registered.get()) {
            listener.onEvent(this, event);
        }
    }

    public void close() {
        channel.close();
    }

    @Override
    public int hashCode() {
        return channel.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o != null && o instanceof CassandraConnection && channel.equals(((CassandraConnection)o).channel);
    }

    private void setRegisterSuccess(Register register) {
        if (channel.pipeline().get(CassandraMessageHandler.class).register(channel, this)) {
            listener.onRegister(this, register.events);
        }
    }

    private void setRegisterFailure(Register register, Throwable cause) {
        if (registered.compareAndSet(true, false)) {
            listener.onRegisterFail(this, register.events, cause);
        }
    }

    private CassandraFuture newFuture(Request request) {
        return new CassandraFuture(CassandraDriver.getGlobalEventExecutor(), this, request);
    }

    private static class RegisterHandler implements CassandraFuture.Listener {

        @Override
        public void completed(CassandraFuture future) throws Exception {
            CassandraConnection connection = future.connection();
            Register register = (Register)future.request();
            if (future.isSuccess()) {
                connection.setRegisterSuccess(register);
            } else {
                connection.setRegisterFailure(register, future.cause());
            }
        }
    }

    private class Initializer implements CassandraFuture.Listener, Runnable {

        private final CassandraFuture startupFuture;
        private Authenticator authenticator;

        private Initializer() {
            Map<String, String> options = new HashMap<String, String>();
            options.put("CQL_VERSION", CQL_VERSION);
            if (options().getCompression() != Compression.NONE) {
                options.put("COMPRESSION", options().getCompression().toString());
            }
            Startup startup = new Startup(options);
            startupFuture = newFuture(startup);
        }

        @Override
        public void run() {
            write(startupFuture.request());
        }

        @Override
        public void completed(CassandraFuture future) throws Exception {
            if (!future.isSuccess()) {
                fail(future.cause());
                return;
            }
            Response response = future.get();
            try {
                if (response instanceof Authenticate) {
                    Authenticate authenticate = (Authenticate)response;
                    AuthProvider provider = options.getAuthProvider();
                    authenticator = provider.newAuthenticator(authenticate.authenticator);
                    AuthResponse authResponse = new AuthResponse(authenticator.initialResponse());
                    write(authResponse);
                } else if (response instanceof AuthChallenge) {
                    AuthChallenge challenge = (AuthChallenge)response;
                    AuthResponse authResponse = new AuthResponse(authenticator.evaluateResponse(challenge.token));
                    write(authResponse);
                } else if (response instanceof AuthSuccess || response instanceof Ready) {
                    if (startupFuture.setSuccess(response)) {
                        listener.onOpen(CassandraConnection.this);
                    }
                } else {
                    fail(new IllegalStateException(String.format("received unexpected message %s", response.getClass())));
                }
            } catch (Throwable cause) {
                fail(cause);
            }
        }

        private CassandraFuture write(Request request) {
            CassandraFuture future = newFuture(request).addListener(this);
            futureMap.addFuture(future);
            channel.writeAndFlush(request);
            return future;
        }

        private void fail(Throwable cause) {
            startupFuture.setFailure(cause);
            listener.onOpenFail(CassandraConnection.this, cause);
            channel.close();
        }
    }
}
