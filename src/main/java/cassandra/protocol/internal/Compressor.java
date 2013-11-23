package cassandra.protocol.internal;

import cassandra.CassandraOptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public interface Compressor {

    ByteBuf compress(ByteBuf bytes) throws IOException;

    ByteBuf decompress(ByteBuf bytes) throws IOException;

    public static class Factory {

        public static boolean canUse(CassandraOptions.Compression compression) {
            switch (compression) {
                case SNAPPY:
                    return SnappyCompressor.INSTANCE != null;
                case LZ4:
                    return LZ4Compressor.INSTANCE != null;
                default:
                    return false;
            }
        }

        public static Compressor compressor(CassandraOptions.Compression compression) {
            if (compression == CassandraOptions.Compression.SNAPPY && SnappyCompressor.INSTANCE != null) {
                return SnappyCompressor.INSTANCE;
            } else if (compression == CassandraOptions.Compression.LZ4 && LZ4Compressor.INSTANCE != null) {
                return LZ4Compressor.INSTANCE;
            } else {
                return NoopCompressor.INSTANCE;
            }
        }
    }

    public static class SnappyCompressor implements Compressor {

        public static final SnappyCompressor INSTANCE;

        static {
            SnappyCompressor instance;
            try {
                instance = new SnappyCompressor();
            } catch (NoClassDefFoundError e) {
                instance = null;
            } catch (Throwable cause) {
                instance = null;
            }
            INSTANCE = instance;
        }

        @Override
        public ByteBuf compress(ByteBuf bytes) throws IOException {
            byte[] input = new byte[bytes.readableBytes()];
            bytes.readBytes(input);
            byte[] output = new byte[Snappy.maxCompressedLength(input.length)];
            int written = Snappy.compress(input, 0, input.length, output, 0);
            return Unpooled.wrappedBuffer(output, 0, written);
        }

        @Override
        public ByteBuf decompress(ByteBuf bytes) throws IOException {
            byte[] input = new byte[bytes.readableBytes()];
            bytes.readBytes(input);
            if (!Snappy.isValidCompressedBuffer(input, 0, input.length)) {
                throw new IllegalStateException("Provided message does not appear to be Snappy compressed");
            }
            byte[] output = new byte[Snappy.uncompressedLength(input)];
            int size = Snappy.uncompress(input, 0, input.length, output, 0);
            return Unpooled.wrappedBuffer(output, 0, size);
        }
    }

    public static class LZ4Compressor implements Compressor {

        public static final LZ4Compressor INSTANCE;

        static {
            LZ4Compressor instance;
            try {
                instance = new LZ4Compressor();
            } catch (NoClassDefFoundError e) {
                instance = null;
            } catch (Throwable cause) {
                instance = null;
            }
            INSTANCE = instance;
        }

        private static final int INTEGER_BYTES = 4;
        private final net.jpountz.lz4.LZ4Compressor compressor;
        private final net.jpountz.lz4.LZ4FastDecompressor decompressor;

        private LZ4Compressor() {
            LZ4Factory factory = LZ4Factory.fastestInstance();
            compressor = factory.fastCompressor();
            decompressor = factory.fastDecompressor();
        }

        @Override
        public ByteBuf compress(ByteBuf bytes) throws IOException {
            byte[] input = new byte[bytes.readableBytes()];
            bytes.readBytes(input);
            int maxCompressedLength = compressor.maxCompressedLength(input.length);
            byte[] output = new byte[INTEGER_BYTES + maxCompressedLength];
            output[0] = (byte)(input.length >>> 24);
            output[1] = (byte)(input.length >>> 16);
            output[2] = (byte)(input.length >>> 8);
            output[3] = (byte)(input.length);
            try {
                int written = compressor.compress(input, 0, input.length, output, INTEGER_BYTES, maxCompressedLength);
                return Unpooled.wrappedBuffer(output, 0, INTEGER_BYTES + written);
            } catch (LZ4Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public ByteBuf decompress(ByteBuf bytes) throws IOException {
            byte[] input = new byte[bytes.readableBytes()];
            bytes.readBytes(input);
            int uncompressedLength = ((input[0] & 0xFF) << 24)
                    | ((input[1] & 0xFF) << 16)
                    | ((input[2] & 0xFF) << 8)
                    | ((input[3] & 0xFF));
            byte[] output = new byte[uncompressedLength];
            try {
                int read = decompressor.decompress(input, INTEGER_BYTES, output, 0, uncompressedLength);
                if (read != input.length - INTEGER_BYTES) {
                    throw new IOException("Compressed lengths mismatch");
                }
                return Unpooled.wrappedBuffer(output);
            } catch (LZ4Exception e) {
                throw new IOException(e);
            }
        }
    }

    public static class NoopCompressor implements Compressor {

        public static final NoopCompressor INSTANCE = new NoopCompressor();

        @Override
        public ByteBuf compress(ByteBuf bytes) throws IOException {
            return bytes;
        }

        @Override
        public ByteBuf decompress(ByteBuf bytes) throws IOException {
            return bytes;
        }
    }
}
