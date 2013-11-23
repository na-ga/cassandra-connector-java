package cassandra.cql.query;

import cassandra.cql.Consistency;
import cassandra.cql.PagingState;
import cassandra.cql.RoutingKey;
import cassandra.retry.RetryPolicy;
import cassandra.routing.RoutingPolicy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public abstract class Query<T extends Query<T>> {

    public static Truncate truncate(String table) {
        return new Truncate(null, table);
    }

    public static Truncate truncate(String keyspace, String table) {
        return new Truncate(keyspace, table);
    }

    public static Select.Selection select() {
        return new Select.Selection.Builder();
    }

    public static Select.Builder select(String... columns) {
        if (columns == null) {
            throw new NullPointerException("columns");
        }
        Select.Selection.Builder builder = new Select.Selection.Builder();
        for (String column : columns) {
            builder.column(column);
        }
        return builder;
    }

    public static Insert insert(String table) {
        return new Insert(null, table);
    }

    public static Insert insert(String keyspace, String table) {
        return new Insert(keyspace, table);
    }

    public static Update update(String table) {
        return new Update(null, table);
    }

    public static Update update(String keyspace, String table) {
        return new Update(keyspace, table);
    }

    public static Delete.Selection delete() {
        return new Delete.Selection();
    }

    public static Delete.Builder delete(String... columns) {
        if (columns == null) {
            throw new NullPointerException("columns");
        }
        Delete.Selection builder = new Delete.Selection();
        for (String column : columns) {
            builder.column(column);
        }
        return builder;
    }

    public static Batch batch() {
        return new Batch();
    }

    public static Batch unloggedBatch() {
        return new Batch().unlogged();
    }

    public static Clause.Equal eq(String name, Object value) {
        return new Clause.Equal(name, value);
    }

    public static Clause.LessThan lt(String name, Object value) {
        return new Clause.LessThan(name, value);
    }

    public static Clause.LessThanEquals lte(String name, Object value) {
        return new Clause.LessThanEquals(name, value);
    }

    public static Clause.GreaterThan gt(String name, Object value) {
        return new Clause.GreaterThan(name, value);
    }

    public static Clause.GreaterThanEquals gte(String name, Object value) {
        return new Clause.GreaterThanEquals(name, value);
    }

    public static Clause.In in(String name, Object... values) {
        return new Clause.In(name, values);
    }

    public static Assignment.Increment inc(String name) {
        return new Assignment.Increment(name, 1L);
    }

    public static Assignment.Increment inc(String name, long value) {
        return new Assignment.Increment(name, value);
    }

    public static Assignment.Decrement dec(String name) {
        return new Assignment.Decrement(name, 1L);
    }

    public static Assignment.Decrement dec(String name, long value) {
        return new Assignment.Decrement(name, value);
    }

    public static Assignment.Set set(String name, Object value) {
        return new Assignment.Set(name, value);
    }

    public static Assignment.ListSet set(String name, int index, Object value) {
        return new Assignment.ListSet(name, index, value);
    }

    public static Assignment.CollectionAdd addFirst(String name, Object value) {
        if (value instanceof List) {
            return addFirst(name, (List<?>)value);
        } else {
            return addFirst(name, Collections.singletonList(value));
        }
    }

    public static Assignment.CollectionAdd addFirst(String name, List<?> value) {
        return new Assignment.CollectionAdd(name, value, true);
    }

    public static Assignment.CollectionAdd addLast(String name, Object value) {
        if (value instanceof List) {
            return addLast(name, (List<?>)value);
        } else {
            return addLast(name, Collections.singletonList(value));
        }
    }

    public static Assignment.CollectionAdd addLast(String name, List<?> value) {
        return new Assignment.CollectionAdd(name, value, false);
    }

    public static Assignment.CollectionRemove removeListElement(String name, Object value) {
        if (value instanceof List) {
            return removeAll(name, (List<?>)value);
        } else {
            return removeAll(name, Collections.singletonList(value));
        }
    }

    public static Assignment.CollectionRemove removeAll(String name, List<?> value) {
        return new Assignment.CollectionRemove(name, value);
    }

    public static Assignment.CollectionAdd add(String name, Object value) {
        if (value instanceof Set) {
            return add(name, (Set<?>)value);
        } else {
            return add(name, Collections.singleton(value));
        }
    }

    public static Assignment.CollectionAdd add(String name, Set<?> value) {
        return new Assignment.CollectionAdd(name, value, false);
    }

    public static Assignment.CollectionRemove removeSetElement(String name, Object value) {
        if (value instanceof Set) {
            return removeAll(name, (Set<?>)value);
        } else {
            return removeAll(name, Collections.singleton(value));
        }
    }

    public static Assignment.CollectionRemove removeAll(String name, Set<?> value) {
        return new Assignment.CollectionRemove(name, value);
    }

    public static Assignment.MapPut put(String name, Object key, Object value) {
        return new Assignment.MapPut(name, key, value);
    }

    public static Assignment.MapPutAll putAll(String name, Map<?, ?> value) {
        return new Assignment.MapPutAll(name, value);
    }

    public static Clause.Using ttl(int value) {
        return using("TTL", value);
    }

    public static Clause.Using timestamp(long value) {
        return using("TIMESTAMP", value);
    }

    public static Clause.Using using(String name, Object value) {
        return new Clause.Using(name, value);
    }

    public static Function token(Object value) {
        return func("token", value);
    }

    public static Function now() {
        return func("now");
    }

    public static Function minTimeuuid(String value) {
        return func("minTimeuuid", value);
    }

    public static Function maxTimeuuid(String value) {
        return func("maxTimeuuid", value);
    }

    public static Function dateOf(UUID value) {
        return func("dateOf", value);
    }

    public static Function unixTimestampOf(UUID value) {
        return func("unixTimestampOf", value);
    }

    public static Function blobAsAscii(ByteBuffer value) {
        return func("blobAsAscii", value);
    }

    public static Function blobAsBigint(ByteBuffer value) {
        return func("blobAsBigint", value);
    }

    public static Function blobAsBoolean(ByteBuffer value) {
        return func("blobAsBoolean", value);
    }

    public static Function blobAsCounter(ByteBuffer value) {
        return func("blobAsCounter", value);
    }

    public static Function blobAsDecimal(ByteBuffer value) {
        return func("blobAsDecimal", value);
    }

    public static Function blobAsDouble(ByteBuffer value) {
        return func("blobAsDouble", value);
    }

    public static Function blobAsFloat(ByteBuffer value) {
        return func("blobAsFloat", value);
    }

    public static Function blobAsInet(ByteBuffer value) {
        return func("blobAsInet", value);
    }

    public static Function blobAsInt(ByteBuffer value) {
        return func("blobAsInt", value);
    }

    public static Function blobAsText(ByteBuffer value) {
        return func("blobAsText", value);
    }

    public static Function blobAsTimestamp(ByteBuffer value) {
        return func("blobAsTimestamp", value);
    }

    public static Function blobAsUuid(ByteBuffer value) {
        return func("blobAsUuid", value);
    }

    public static Function blobAsVarchar(ByteBuffer value) {
        return func("blobAsVarchar", value);
    }

    public static Function blobAsVarint(ByteBuffer value) {
        return func("blobAsVarint", value);
    }

    public static Function blobAsTimeUuid(ByteBuffer value) {
        return func("blobAsTimeUuid", value);
    }

    public static Function asciiAsBlob(String value) {
        return func("asciiAsBlob", value);
    }

    public static Function bigintAsBlob(long value) {
        return func("bigintAsBlob", value);
    }

    public static Function booleanAsBlob(boolean value) {
        return func("booleanAsBlob", value);
    }

    public static Function counterAsBlob(long value) {
        return func("counterAsBlob", value);
    }

    public static Function decimalAsBlob(BigDecimal value) {
        return func("decimalAsBlob", value);
    }

    public static Function doubleAsBlob(double value) {
        return func("doubleAsBlob", value);
    }

    public static Function floatAsBlob(float value) {
        return func("floatAsBlob", value);
    }

    public static Function inetAsBlob(InetAddress value) {
        return func("inetAsBlob", value);
    }

    public static Function intAsBlob(int value) {
        return func("intAsBlob", value);
    }

    public static Function textAsBlob(String value) {
        return func("textAsBlob", value);
    }

    public static Function timestampAsBlob(Date value) {
        return func("timestampAsBlob", value);
    }

    public static Function uuidAsBlob(UUID value) {
        return func("uuidAsBlob", value);
    }

    public static Function varcharAsBlob(String value) {
        return func("varcharAsBlob", value);
    }

    public static Function varintAsBlob(BigInteger value) {
        return func("varintAsBlob", value);
    }

    public static Function timeUuidAsBlob(UUID value) {
        return func("timeUuidAsBlob", value);
    }

    public static Function func(String name, Object... parameters) {
        return new Function(name, parameters);
    }

    public static Order asc(String name) {
        return new Order.Ascending(name);
    }

    public static Order desc(String name) {
        return new Order.Descending(name);
    }

    protected String keyspace, table;
    protected int pageSizeLimit;
    protected PagingState pagingState;
    protected RoutingKey routingKey;
    protected RoutingPolicy routingPolicy;
    protected RetryPolicy retryPolicy;
    protected Consistency consistency, serialConsistency;
    protected boolean prepared = true;
    protected boolean tracing;

    public boolean hasKeyspace() {
        return keyspace != null && !keyspace.isEmpty();
    }

    public String keyspace() {
        return keyspace;
    }

    public boolean hasTable() {
        return table != null && !table.isEmpty();
    }

    public String table() {
        return table;
    }

    public int pageSizeLimit() {
        return pageSizeLimit;
    }

    @SuppressWarnings("unchecked")
    public T pageSizeLimit(int pageSizeLimit) {
        if (pageSizeLimit <= 0) {
            throw new IllegalArgumentException(String.format("pageSizeLimit: %d (expected: > 0)", pageSizeLimit));
        }
        this.pageSizeLimit = pageSizeLimit;
        return (T)this;
    }

    public boolean hasPagingState() {
        return pagingState != null;
    }

    public PagingState pagingState() {
        return pagingState;
    }

    public RoutingKey routingKey() {
        return routingKey;
    }

    @SuppressWarnings("unchecked")
    public T routingKey(RoutingKey routingKey) {
        this.routingKey = routingKey;
        return (T)this;
    }

    public RoutingPolicy routingPolicy() {
        return routingPolicy;
    }

    @SuppressWarnings("unchecked")
    public T routingPolicy(RoutingPolicy routingPolicy) {
        this.routingPolicy = routingPolicy;
        return (T)this;
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    @SuppressWarnings("unchecked")
    public T retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return (T)this;
    }

    public Consistency consistency() {
        return consistency;
    }

    @SuppressWarnings("unchecked")
    public T consistency(Consistency consistency) {
        this.consistency = consistency;
        return (T)this;
    }

    public Consistency serialConsistency() {
        return serialConsistency;
    }

    @SuppressWarnings("unchecked")
    public T serialConsistency(Consistency serialConsistency) {
        this.serialConsistency = serialConsistency;
        return (T)this;
    }

    public boolean isPrepared() {
        return prepared;
    }

    @SuppressWarnings("unchecked")
    public T unprepared() {
        prepared = false;
        return (T)this;
    }

    public boolean isTracing() {
        return tracing;
    }

    @SuppressWarnings("unchecked")
    public T tracing() {
        tracing = true;
        return (T)this;
    }

    public abstract void accept(QueryVisitor visitor);
}
