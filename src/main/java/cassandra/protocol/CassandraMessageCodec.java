package cassandra.protocol;

import cassandra.CassandraOptions;
import cassandra.protocol.internal.Compressor;
import cassandra.protocol.internal.MessageInputStream;
import cassandra.protocol.internal.MessageOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class CassandraMessageCodec extends CombinedChannelDuplexHandler<CassandraMessageCodec.Decoder, CassandraMessageCodec.Encoder> {

    public static final int MAX_FRAME_LENGTH = 1024 * 1024 * 256;

    private final Compressor compressor;

    public CassandraMessageCodec(CassandraOptions.Compression compression) {
        init(new Decoder(), new Encoder());
        compressor = Compressor.Factory.compressor(compression);
    }

    class Decoder extends LengthFieldBasedFrameDecoder {

        Decoder() {
            super(MAX_FRAME_LENGTH, 4, 4, 0, 0, true);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf frame = (ByteBuf)super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            MessageInputStream input = new MessageInputStream(frame);
            try {
                CassandraMessage.Header header = CassandraMessage.Header.parseFrom(input);
                if (header.getCompressedFlag()) {
                    ByteBuf decompressed = compressor.decompress(frame);
                    header.setMessageLength(decompressed.readableBytes());
                    input = new MessageInputStream(decompressed);
                }
                return CassandraMessage.parseFrom(header, input);
            } catch (Exception e) {
                if (!(e instanceof CodecException)) {
                    throw new CodecException(e);
                } else {
                    throw e;
                }
            }
        }

        @Override
        protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
            return buffer.slice(index, length);
        }
    }

    class Encoder extends MessageToMessageEncoder<CassandraMessage> {

        @Override
        protected void encode(ChannelHandlerContext ctx, CassandraMessage message, List<Object> out) throws Exception {
            MessageOutputStream headerOut = null;
            MessageOutputStream bodyOut = null;
            boolean release = true;
            try {
                CassandraMessage.Header header = CassandraMessage.Header.valueOf(message);
                bodyOut = new MessageOutputStream(ctx.alloc().buffer(header.getMessageLength()));
                message.writeTo(bodyOut);
                if (header.getCompressedFlag()) {
                    ByteBuf compressed = compressor.compress(bodyOut.buffer());
                    header.setMessageLength(compressed.readableBytes());
                    bodyOut.close();
                    bodyOut = new MessageOutputStream(compressed);
                }
                headerOut = new MessageOutputStream(ctx.alloc().buffer(header.getApproximateSize()));
                header.writeTo(headerOut);
                out.add(headerOut.buffer());
                out.add(bodyOut.buffer());
                release = false;
            } catch (Exception e) {
                if (!(e instanceof CodecException)) {
                    throw new CodecException(e);
                } else {
                    throw e;
                }
            } finally {
                if (release) {
                    if (headerOut != null) {
                        headerOut.close();
                    }
                    if (bodyOut != null) {
                        bodyOut.close();
                    }
                }
            }
        }
    }
}
