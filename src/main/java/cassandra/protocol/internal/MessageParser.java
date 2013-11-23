package cassandra.protocol.internal;

public abstract class MessageParser<M extends Message> {

    public abstract M parseFrom(MessageInputStream input);
}
