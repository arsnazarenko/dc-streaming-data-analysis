package org.example.model;

import java.util.Map;
import java.util.UUID;

public class MetricEvent {
    private final UUID eventId;
    private final String hostId;
    private final String zone;
    private final long timestamp;
    private final MetricType metric;
    private final double value;
    private final String unit;
    private final Map<String, Object> tags;

    public MetricEvent(UUID eventId, String hostId, String zone, long timestamp, 
                      MetricType metric, double value, String unit, Map<String, Object> tags) {
        this.eventId = eventId;
        this.hostId = hostId;
        this.zone = zone;
        this.timestamp = timestamp;
        this.metric = metric;
        this.value = value;
        this.unit = unit;
        this.tags = tags;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getHostId() {
        return hostId;
    }

    public String getZone() {
        return zone;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public MetricType getMetric() {
        return metric;
    }

    public double getValue() {
        return value;
    }

    public String getUnit() {
        return unit;
    }

    public Map<String, Object> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "MetricEvent{" +
                "eventId=" + eventId +
                ", hostId='" + hostId + '\'' +
                ", zone='" + zone + '\'' +
                ", timestamp=" + timestamp +
                ", metric=" + metric +
                ", value=" + value +
                ", unit='" + unit + '\'' +
                ", tags=" + tags +
                '}';
    }
}