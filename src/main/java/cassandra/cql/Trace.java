package cassandra.cql;

import cassandra.CassandraSession;
import cassandra.metadata.TraceMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Trace extends TraceMetadata {

    private final CassandraSession session;
    private boolean traceInitDone, eventInitDone;

    public Trace(CassandraSession session, UUID id) {
        if (session == null) {
            throw new NullPointerException("session");
        }
        if (id == null) {
            throw new NullPointerException("id");
        }
        this.session = session;
        setSessionId(id);
    }

    @Override
    protected void initTrace() {
        if (traceInitDone) {
            return;
        }
        synchronized (this) {
            if (traceInitDone) {
                return;
            }
            ResultSet rs = session.execute(String.format("SELECT * FROM system_traces.sessions WHERE session_id=%s", getSessionId()));
            if (rs.hasNext()) {
                Row row = rs.next();
                setCoordinator(row.getInet("coordinator"));
                setDuration(row.getInt("duration"));
                setParameters(row.getMap("parameters", String.class, String.class));
                setRequest(row.getString("request"));
                setStartedAt(row.getDate("started_at"));
                traceInitDone = true;
            }
        }
    }

    @Override
    protected void initEvent() {
        if (eventInitDone) {
            return;
        }
        synchronized (this) {
            if (eventInitDone) {
                return;
            }
            List<Event> events;
            ResultSet rs = session.execute(String.format("SELECT session_id, event_id, unixTimestampOf(event_id) AS timestamp, activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id=%s", getSessionId()));
            if (rs.hasNext()) {
                events = new ArrayList<Event>();
                while (rs.hasNext()) {
                    events.add(new Event(rs.next()));
                }
                setEvents(events);
                eventInitDone = true;
            }
        }
    }
}
