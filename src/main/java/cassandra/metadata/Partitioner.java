package cassandra.metadata;

import cassandra.cql.type.CQL3Type;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public abstract class Partitioner {

    public static Partitioner getPartitioner(String partitioner) {
        if (partitioner == null) {
            return null;
        }
        if (partitioner.endsWith(".Murmur3Partitioner")) {
            return Murmur3Partitioner.INSTANCE;
        } else if (partitioner.endsWith(".RandomPartitioner")) {
            return RandomPartitioner.INSTANCE;
        } else if (partitioner.endsWith(".ByteOrderedPartitioner")) {
            return ByteOrderedPartitioner.INSTANCE;
        } else if (partitioner.endsWith(".OrderPreservingPartitioner")) {
            return OrderPreservingPartitioner.INSTANCE;
        } else {
            return null;
        }
    }

    public static abstract class Token implements Comparable<Token> {
        // Tag
    }

    public abstract Token getToken(ByteBuffer partitionKey);

    public abstract Token getToken(String token);

    public static class Murmur3Partitioner extends Partitioner {

        public static final Murmur3Partitioner INSTANCE = new Murmur3Partitioner();
        public static final LongToken MINIMUM = new LongToken(Long.MIN_VALUE);

        @Override
        public LongToken getToken(ByteBuffer partitionKey) {
            if (partitionKey.remaining() == 0) {
                return MINIMUM;
            }
            long hash = MurmurHash.hash3_x64_128(partitionKey, partitionKey.position(), partitionKey.remaining(), 0)[0];
            if (hash == Long.MIN_VALUE) {
                return new LongToken(Long.MAX_VALUE);
            }
            return new LongToken(hash);
        }

        @Override
        public LongToken getToken(String token) {
            return new LongToken(new Long(token));
        }

        public static class LongToken extends Token {

            final Long value;

            public LongToken(Long value) {
                if (value == null) {
                    throw new NullPointerException("value");
                }
                this.value = value;
            }

            @Override
            public int hashCode() {
                return value.hashCode();
            }

            @Override
            public boolean equals(Object o) {
                return this == o || o != null && (getClass() == o.getClass()) && value.equals(((LongToken)o).value);
            }

            @Override
            public String toString() {
                return value.toString();
            }

            @Override
            public int compareTo(Token token) {
                return value.compareTo(((LongToken)token).value);
            }
        }
    }

    public static class RandomPartitioner extends Partitioner {

        public static final RandomPartitioner INSTANCE = new RandomPartitioner();
        public static final BigIntegerToken MINIMUM = new BigIntegerToken(new BigInteger("-1"));

        private final ThreadLocal<MessageDigest> md5 = new ThreadLocal<MessageDigest>() {
            @Override
            protected MessageDigest initialValue() {
                try {
                    return MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalStateException(e);
                }
            }
        };

        @Override
        public BigIntegerToken getToken(ByteBuffer partitionKey) {
            if (partitionKey.remaining() == 0) {
                return MINIMUM;
            }
            MessageDigest digest = md5.get();
            digest.reset();
            digest.update(partitionKey.duplicate());
            return new BigIntegerToken(new BigInteger(digest.digest()).abs());
        }

        @Override
        public BigIntegerToken getToken(String token) {
            return new BigIntegerToken(new BigInteger(token));
        }

        public static class BigIntegerToken extends Token {

            final BigInteger value;

            public BigIntegerToken(BigInteger value) {
                if (value == null) {
                    throw new NullPointerException("value");
                }
                this.value = value;
            }

            @Override
            public int hashCode() {
                return value.hashCode();
            }

            @Override
            public boolean equals(Object o) {
                return this == o || o != null && (getClass() == o.getClass()) && value.equals(((BigIntegerToken)o).value);
            }

            @Override
            public String toString() {
                return value.toString();
            }

            @Override
            public int compareTo(Token token) {
                return value.compareTo(((BigIntegerToken)token).value);
            }
        }
    }

    public static class ByteOrderedPartitioner extends Partitioner {

        public static final ByteOrderedPartitioner INSTANCE = new ByteOrderedPartitioner();
        public static final BytesToken MINIMUM = new BytesToken(new byte[0]);

        @Override
        public BytesToken getToken(ByteBuffer partitionKey) {
            if (partitionKey.remaining() == 0) {
                return MINIMUM;
            }
            byte[] bytes;
            int length = partitionKey.remaining();
            if (partitionKey.hasArray()) {
                int offset = partitionKey.arrayOffset() + partitionKey.position();
                if (offset == 0 && length == partitionKey.array().length)
                    bytes = partitionKey.array();
                else
                    bytes = Arrays.copyOfRange(partitionKey.array(), offset, offset + length);
            } else {
                bytes = new byte[length];
                partitionKey.duplicate().get(bytes);
            }
            return new BytesToken(bytes);
        }

        @Override
        public BytesToken getToken(String token) {
            return new BytesToken(Hex.hexToBytes(token));
        }

        public static class BytesToken extends Token {

            final byte[] value;

            public BytesToken(byte[] value) {
                if (value == null) {
                    throw new NullPointerException("value");
                }
                this.value = value;
            }

            @Override
            public int hashCode() {
                return 31 + Arrays.hashCode(value);
            }

            @Override
            public boolean equals(Object o) {
                return this == o || o != null && (getClass() == o.getClass()) && Arrays.equals(value, ((BytesToken)o).value);
            }

            @Override
            public String toString() {
                return Hex.bytesToHex(value);
            }

            public int compareTo(Token token) {
                return ByteBufUtil.compare(Unpooled.wrappedBuffer(value), Unpooled.wrappedBuffer(((BytesToken)token).value));
            }
        }
    }

    public static class OrderPreservingPartitioner extends Partitioner {

        public static final OrderPreservingPartitioner INSTANCE = new OrderPreservingPartitioner();

        @Override
        public StringToken getToken(ByteBuffer partitionKey) {
            try {
                return new StringToken(CQL3Type.StringCodec.UTF8_INSTANCE.decode(partitionKey));
            } catch (Exception e) {
                return new StringToken(Hex.bytesToHex(partitionKey));
            }
        }

        @Override
        public StringToken getToken(String token) {
            return new StringToken(token);
        }

        public static class StringToken extends Token {

            final String value;

            public StringToken(String value) {
                if (value == null) {
                    throw new NullPointerException("value");
                }
                this.value = value;
            }

            @Override
            public int hashCode() {
                return value.hashCode();
            }

            @Override
            public boolean equals(Object o) {
                return this == o || o != null && (getClass() == o.getClass()) && value.equals(((StringToken)o).value);
            }

            @Override
            public int compareTo(Token token) {
                return value.compareTo(((StringToken)token).value);
            }

            @Override
            public String toString() {
                return value;
            }
        }
    }

    //

    public static class Hex {

        @SuppressWarnings("unchecked")
        private static final Constructor<String> stringConstructor = getProtectedConstructor(String.class, int.class, int.class, char[].class);
        private static final byte[] charToByte = new byte[256];
        private static final char[] byteToChar = new char[16];

        static {
            for (char c = 0; c < charToByte.length; ++c) {
                if (c >= '0' && c <= '9')
                    charToByte[c] = (byte)(c - '0');
                else if (c >= 'A' && c <= 'F')
                    charToByte[c] = (byte)(c - 'A' + 10);
                else if (c >= 'a' && c <= 'f')
                    charToByte[c] = (byte)(c - 'a' + 10);
                else
                    charToByte[c] = (byte)-1;
            }
            for (int i = 0; i < 16; ++i) {
                byteToChar[i] = Integer.toHexString(i).charAt(0);
            }
        }

        public static byte[] hexToBytes(String str) {
            if (str.length() % 2 == 1)
                throw new NumberFormatException("An hex string representing bytes must have an even length");
            byte[] bytes = new byte[str.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                byte halfByte1 = charToByte[str.charAt(i * 2)];
                byte halfByte2 = charToByte[str.charAt(i * 2 + 1)];
                if (halfByte1 == -1 || halfByte2 == -1) {
                    throw new NumberFormatException("Non-hex characters in " + str);
                }
                bytes[i] = (byte)((halfByte1 << 4) | halfByte2);
            }
            return bytes;
        }

        public static String bytesToHex(byte... bytes) {
            char[] c = new char[bytes.length * 2];
            for (int i = 0; i < bytes.length; i++) {
                int bint = bytes[i];
                c[i * 2] = byteToChar[(bint & 0xf0) >> 4];
                c[1 + i * 2] = byteToChar[bint & 0x0f];
            }
            return wrapCharArray(c);
        }

        public static String bytesToHex(ByteBuffer bytes) {
            final int offset = bytes.position();
            final int size = bytes.remaining();
            final char[] c = new char[size * 2];
            for (int i = 0; i < size; i++) {
                final int bint = bytes.get(i + offset);
                c[i * 2] = Hex.byteToChar[(bint & 0xf0) >> 4];
                c[1 + i * 2] = Hex.byteToChar[bint & 0x0f];
            }
            return Hex.wrapCharArray(c);
        }

        public static String wrapCharArray(char[] c) {
            if (c == null)
                return null;
            String s = null;
            if (stringConstructor != null) {
                try {
                    s = stringConstructor.newInstance(0, c.length, c);
                } catch (Exception e) {
                    // Swallowing as we'll just use a copying constructor
                }
            }
            return s == null ? new String(c) : s;
        }

        @SuppressWarnings("unchecked")
        public static Constructor getProtectedConstructor(Class klass, Class... paramTypes) {
            Constructor c;
            try {
                c = klass.getDeclaredConstructor(paramTypes);
                c.setAccessible(true);
                return c;
            } catch (Exception e) {
                return null;
            }
        }
    }

    public static class MurmurHash {

        public static long[] hash3_x64_128(ByteBuffer key, int offset, int length, long seed) {
            final int nblocks = length >> 4;

            long h1 = seed;
            long h2 = seed;

            long c1 = 0x87c37b91114253d5L;
            long c2 = 0x4cf5ad432745937fL;

            for (int i = 0; i < nblocks; i++) {
                long k1 = getblock(key, offset, i * 2);
                long k2 = getblock(key, offset, i * 2 + 1);

                k1 *= c1;
                k1 = rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;

                h1 = rotl64(h1, 27);
                h1 += h2;
                h1 = h1 * 5 + 0x52dce729;

                k2 *= c2;
                k2 = rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;

                h2 = rotl64(h2, 31);
                h2 += h1;
                h2 = h2 * 5 + 0x38495ab5;
            }

            offset += nblocks * 16;

            long k1 = 0;
            long k2 = 0;

            switch (length & 15) {
                case 15:
                    k2 ^= ((long)key.get(offset + 14)) << 48;
                case 14:
                    k2 ^= ((long)key.get(offset + 13)) << 40;
                case 13:
                    k2 ^= ((long)key.get(offset + 12)) << 32;
                case 12:
                    k2 ^= ((long)key.get(offset + 11)) << 24;
                case 11:
                    k2 ^= ((long)key.get(offset + 10)) << 16;
                case 10:
                    k2 ^= ((long)key.get(offset + 9)) << 8;
                case 9:
                    k2 ^= ((long)key.get(offset + 8)) << 0;
                    k2 *= c2;
                    k2 = rotl64(k2, 33);
                    k2 *= c1;
                    h2 ^= k2;

                case 8:
                    k1 ^= ((long)key.get(offset + 7)) << 56;
                case 7:
                    k1 ^= ((long)key.get(offset + 6)) << 48;
                case 6:
                    k1 ^= ((long)key.get(offset + 5)) << 40;
                case 5:
                    k1 ^= ((long)key.get(offset + 4)) << 32;
                case 4:
                    k1 ^= ((long)key.get(offset + 3)) << 24;
                case 3:
                    k1 ^= ((long)key.get(offset + 2)) << 16;
                case 2:
                    k1 ^= ((long)key.get(offset + 1)) << 8;
                case 1:
                    k1 ^= ((long)key.get(offset));
                    k1 *= c1;
                    k1 = rotl64(k1, 31);
                    k1 *= c2;
                    h1 ^= k1;
            }

            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix(h1);
            h2 = fmix(h2);

            h1 += h2;
            h2 += h1;

            return (new long[]{h1, h2});
        }

        protected static long getblock(ByteBuffer key, int offset, int index) {
            int i_8 = index << 3;
            int blockOffset = offset + i_8;
            return ((long)key.get(blockOffset) & 0xff) + (((long)key.get(blockOffset + 1) & 0xff) << 8) +
                    (((long)key.get(blockOffset + 2) & 0xff) << 16) + (((long)key.get(blockOffset + 3) & 0xff) << 24) +
                    (((long)key.get(blockOffset + 4) & 0xff) << 32) + (((long)key.get(blockOffset + 5) & 0xff) << 40) +
                    (((long)key.get(blockOffset + 6) & 0xff) << 48) + (((long)key.get(blockOffset + 7) & 0xff) << 56);
        }

        protected static long rotl64(long v, int n) {
            return ((v << n) | (v >>> (64 - n)));
        }

        protected static long fmix(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
        }
    }
}
