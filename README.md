# Cassandra Connector Java

A Java driver for [Apache Cassandra](http://cassandra.apache.org/)'s CQL3 binary protocol.

## Initializing Driver

#### creating a new driver using default number of threads:

    CassandraDriver driver = new CassandraDriver(); // default : NCPU * 2

#### or specified number of threads:

    int nThreads = 16;
    CassandraDriver driver = new CassandraDriver(nThreads);

#### or specified EventLoopGroup:

    int nThreads = 16;
    EventLoopGroup loop = new NioEventLoopGroup(nThreads);
    CassandraDriver driver = new CassandraDriver(loop);

## Building Cluster

    // only seed(s) is required
    CassandraCluster cluster = driver.newClusterBuilder()
                                     .addSeed(seed)
                                     .setOptions(CassandraOptions.DEFAULT) // with options
                                     .build();

#### options:

    CassandraOptions.Builder opts = CassandraOptions.newBuilder();

|property name|default value|
|-------------|-------------|
|port|9042|
|connectTimeoutMillis|3000|
|pageSizeLimit|1000|
|compression|NONE|
|consistency|QUORUM|
|serialConsistency|SERIAL|
|routingPolicy|RoundRobinPolicy|
|retryPolicy|MaxRetriesPolicy(ErrorCodeAwareRetryPolicy)|
|authProvider|null|
|sslContext|null|
|cipherSuites|null|

#### multiple-clusters:

    CassandraCluster cluster1 = driver.newClusterBuilder().addSeed(cluster1-seed).build();
    CassandraCluster cluster2 = driver.newClusterBuilder().addSeed(cluster2-seed).build();

    CassandraOptions opts = CassandraOptions.newBuilder()
                                            .setConsistency(Consistency.LOCAL_ONE)
                                            .mergeFrom(cluster1.options())
                                            .build();
    CassandraCluster cluster3 = CassandraCluster.newBuilder()
                                                .setDriver(cluster1.driver())
                                                .setOptions(opts)
                                                .addSeed(cluster3-seed)
                                                .build();

#### event listener:

    CassandraCluster.EventListener listener = new CassandraCluster.EventListener() { ... };
    builder.addEventListener(listener);

#### shutdown:

    cluster.close(); // close all cluster sessions (optional)
    driver.shutdown();

## Sessions

#### global:

    CassandraSession session = cluster.session();

#### per-keyspace:

    CassandraSession session = cluster.session("mykeyspace");

## Using CQL Statements

#### async:

    Statement stmt = session.statement(QUERY);
    ResultSetFuture future = stmt.executeAsync();

or:

    ResultSetFuture future = session.executeAsync(QUERY or STATEMENT);

#### statement:

    Statement stmt = session.statement("SELECT * FROM mytable");
    stmt.execute();

or:

    session.execute("SELECT * FROM mytable");

#### prepared statement:

    PreparedStatement pstmt = session.prepareStatement("INSERT INTO mytable (p1, p2) VALUES (?, ?)");
    pstmt.setObject("p1", p1);
    pstmt.setObject("p2", p2);
    pstmt.execute();

or:

    session.prepareAndExecute("INSERT INTO mytable (p1, p2) VALUES (?, ?)", p1, p2);

or using named bind variables:

    PreparedStatement pstmt = session.prepareStatement("INSERT INTO mytable (p1, p2) VALUES (:myp1, :myp2)");
    pstmt.setObject("myp1", p1);
    pstmt.setObject("myp2", p2);
    pstmt.execute();

#### one-off prepare and execute:

    Statement stmt = session.statement("INSERT INTO mytable (p1, p2) VALUES (?, ?)", p1, p2);
    stmt.execute();

or:

    session.execute("INSERT INTO mytable (p1, p2) VALUES (?, ?)", p1, p2);

#### batch statement:

    PreparedStatement pstmt = session.prepareStatement("INSERT INTO mytable (p1, p2) VALUES (?, ?)");

    BatchStatement batch = session.batch();
    batch.add(pstmt.setObject("p1", p1).setObject("p2", p2))
         .add(pstmt.bind(otherP1, otherP2))
         .add(session.statement("DELETE * FROM mytable WHERE p1=?", oldP1))
         .execute();

#### per-request options:

    stmt.setRoutingPolicy(...);
    stmt.setRetryPolicy(...);
    stmt.setConsistency(...);
    stmt.setSerialConsistency(...);

## Queries

#### using static import:

    import static cassandra.cql.query.Query.*;

#### select:

    Select select = select().column("p2").as("myp2")
                            .from("mytable")
                            .where(eq("p1", p1));
    select.unprepare(); // one-off prepare
    for (Row row : session.execute(select)) {
        // Do something ...
    }

#### batch:

    Batch batch = batch();
    batch.add(insert("mytable1")...)
         .add(update("mytable2")...)
         .add(delete("mytable3")...)
    batch.unlogged(); // unlogged batch
    session.execute(batch);

## Cursors

    for (Row row : session.execute("SELECT * FROM mytable")) {
        // Do something ...
    }

or:

    ResultSet rs = session.execute("SELECT * FROM mytable");
    while (rs.hasNext()) {
        Row row = rs.next();
        // Do something ...
    }

#### result paging:

    // insert dummy data
    Batch batch = Query.batch();
    for (int i = 0; i < 10000; i++) {
        batch.add(Query.insert("mytable").value("p1", i).value("p2", i));
    }
    session.execute(batch);

    Statement stmt = session.statement("SELECT * FROM mytable");
    stmt.setPageSizeLimit(100);
    ResultSet rs;

    do {
        rs = stmt.execute();
        rs.setAutoPaging(false); // turn-off auto paging

        PagingState pagingState = rs.getPagingState();
        stmt.setPagingState(pagingState);

        for (Row row : rs) {
            // Do something ...
        }
    } while (rs.hasMorePages());

or using query builder:

    import static cassandra.cql.query.Query.*;
    ...
    Select select = select().all().from("mytable");
    select.pageSizeLimit(100);

    ResultSet rs;
    do {
        rs = session.execute(select);
        rs.setAutoPaging(false);

        PagingState pagingState = rs.getPagingState();
        String pagingStateHex = pagingState.toHexString();
        select.pagingState(PagingState.copyFrom(pagingStateHex));

        for (Row row : rs) {
            // Do something ...
        }
    } while(rs.hasMorePages());

## Tracing

    Statement stmt = session.statement(""SELECT * FROM mytable"");
    stmt.setTracing(true);
    ResultSet rs = stmt.execute();

or:

    Select select = select().all().from("mytable");
    select.tracing();
    ResultSet rs = session.execute(select);

tracing sessions and events:

    Trace trace = rs.getLastTrace();
    System.out.println(trace.toJsonString());

    for (Event e : trace.getEvents()) {
        System.out.println(e.toJsonString());
    }

## Metadata

    Metadata metadata = cluster.metadata();
    metadata.getClusterName();
    System.out.println(metadata.toJsonString());

    for (KeyspaceMetadata keyspace : metadata.getKeyspaces()) {
        System.out.println(keyspace.toJsonString());

        for (TableMetadata table : keyspace.getTables()) {
            System.out.println(table.toJsonString());

            for (ColumnMetadata column : table.getColumns()) {
                System.out.println(column.toJsonString());
            }
        }
    }

## Dependencies

* JDK 1.6+
* [netty4](http://netty.io)
* [slf4j-api](http://www.slf4j.org)
* [jackson](https://github.com/FasterXML/jackson)
* [metrics](http://metrics.codahale.com)
* [snappy-java](https://github.com/xerial/snappy-java) optional
* [lz4](https://github.com/jpountz/lz4-java) optional

<!--
## Maven repository

    <dependency>
        <groupId>cassandra</groupId>
        <artifactId>cassandra-connector-java</artifactId>
        <version>X.Y.Z</version>
        <scope>compile</scope>
    </dependency>
-->