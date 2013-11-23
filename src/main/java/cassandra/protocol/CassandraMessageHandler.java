package cassandra.protocol;

import cassandra.CassandraConnection;
import cassandra.CassandraException;
import cassandra.CassandraFuture;
import cassandra.CassandraFutureMap;
import cassandra.protocol.CassandraMessage.Event;
import cassandra.protocol.CassandraMessage.Request;
import cassandra.protocol.CassandraMessage.Response;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CodecException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ConcurrentMap;

import static io.netty.util.internal.PlatformDependent.newConcurrentHashMap;

@ChannelHandler.Sharable
public class CassandraMessageHandler extends SimpleChannelInboundHandler<CassandraMessage> {

    private static final Logger logger = LoggerFactory.getLogger(CassandraMessageHandler.class);

    private final CassandraFutureMap futureMap;
    private final ConcurrentMap<Channel, CassandraConnection> registeredConnections;

    public CassandraMessageHandler(CassandraFutureMap futureMap) {
        this.futureMap = futureMap;
        registeredConnections = newConcurrentHashMap();
    }

    public boolean register(Channel channel, CassandraConnection connection) {
        return registeredConnections.putIfAbsent(channel, connection) == null;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        futureMap.fail(ctx.channel(), new ClosedChannelException());
        registeredConnections.remove(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("{} uncaught exception thrown, {}", ctx.channel(), cause.getMessage(), cause);
        if (cause instanceof CodecException) {
            return;
        }
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CassandraMessage message) throws Exception {
        if (message instanceof Request) {
            logger.debug("{} received unexpected message, {}", ctx.channel(), message);
            ctx.close();
            return;
        }

        if (message instanceof Event) {
            Event event = (Event)message;
            CassandraConnection connection = registeredConnections.get(ctx.channel());
            if (connection != null) {
                connection.handleEvent(event);
            } else {
                logger.debug("{} received event with no connection registered, {}", ctx.channel(), event);
            }
            return;
        }

        CassandraFuture future = futureMap.removeFuture(ctx.channel(), message.getStreamId());

        if (future != null) {
            if (message instanceof CassandraMessage.Error) {
                CassandraException exception = ((CassandraMessage.Error)message).exception;
                future.setFailure(exception);
                logger.debug("{} received error response, {}", ctx.channel(), exception.getMessage());
            } else {
                future.setSuccess((Response)message);
            }
        } else {
            logger.debug("{} received response with no request registered, {}", ctx.channel(), message);
            ctx.close();
        }
    }
}
