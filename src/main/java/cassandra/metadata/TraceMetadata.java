package cassandra.metadata;

import cassandra.cql.Row;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public abstract class TraceMetadata extends MetadataEntity {

    private UUID sessionId;
    private InetAddress coordinator;
    private int duration;
    private Map<String, String> parameters;
    private String request;
    private Date startedAt;
    private List<Event> events;

    public UUID getSessionId() {
        return sessionId;
    }

    protected void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public InetAddress getCoordinator() {
        initTrace();
        return coordinator;
    }

    protected void setCoordinator(InetAddress coordinator) {
        this.coordinator = coordinator;
    }

    public int getDuration() {
        initTrace();
        return duration;
    }

    protected void setDuration(int duration) {
        this.duration = duration;
    }

    public Map<String, String> getParameters() {
        initTrace();
        return parameters;
    }

    protected void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public String getRequest() {
        initTrace();
        return request;
    }

    protected void setRequest(String request) {
        this.request = request;
    }

    public Date getStartedAt() {
        initTrace();
        return startedAt;
    }

    protected void setStartedAt(Date startedAt) {
        this.startedAt = startedAt;
    }

    @JsonIgnore
    public List<Event> getEvents() {
        initEvent();
        return events;
    }

    protected void setEvents(List<Event> events) {
        this.events = events;
    }

    protected abstract void initTrace();

    protected abstract void initEvent();

    public static class Event extends MetadataEntity {

        private final UUID sessionId;
        private final UUID eventId;
        private final String activity;
        private final InetAddress source;
        private final int sourceElapsed;
        private final String thread;
        private final long timestamp;

        public Event(Row row) {
            sessionId = row.getUUID("session_id");
            eventId = row.getUUID("event_id");
            activity = row.getString("activity");
            source = row.getInet("source");
            sourceElapsed = row.getInt("source_elapsed");
            thread = row.getString("thread");
            if (!row.isNull("timestamp")) {
                timestamp = row.getLong("timestamp");
            } else {
                timestamp = 0;
            }
        }

        public UUID getSessionId() {
            return sessionId;
        }

        public UUID getEventId() {
            return eventId;
        }

        @JsonIgnore
        public long getTimestamp() {
            return timestamp;
        }

        public String getActivity() {
            return activity;
        }

        public InetAddress getSource() {
            return source;
        }

        public int getSourceElapsed() {
            return sourceElapsed;
        }

        public String getThread() {
            return thread;
        }
    }
}
