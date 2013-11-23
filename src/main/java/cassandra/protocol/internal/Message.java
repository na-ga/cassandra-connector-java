package cassandra.protocol.internal;

public abstract class Message {

    public abstract int getApproximateSize();

    public abstract void writeTo(MessageOutputStream output);

    public MessageParser<? extends Message> getParserForType() {
        throw new UnsupportedOperationException();
    }

    public static abstract class Builder<T extends Builder<T>> implements Cloneable {

        public abstract T mergeFrom(MessageInputStream input);

        @Override
        @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
        public abstract T clone();
    }
}
