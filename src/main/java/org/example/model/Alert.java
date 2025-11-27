package org.example.model;

import java.util.UUID;

public class Alert {
    private final UUID alertId;
    private final String hostId;
    private final String zone;
    private final long timestamp;
    private final MetricType metric;
    private final double value;
    private final double threshold;
    private final String message;

    public Alert(UUID alertId, String hostId, String zone, long timestamp, 
                MetricType metric, double value, double threshold, String message) {
        this.alertId = alertId;
        this.hostId = hostId;
        this.zone = zone;
        this.timestamp = timestamp;
        this.metric = metric;
        this.value = value;
        this.threshold = threshold;
        this.message = message;
    }

    public UUID getAlertId() {
        return alertId;
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

    public double getThreshold() {
        return threshold;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "alertId=" + alertId +
                ", hostId='" + hostId + '\'' +
                ", zone='" + zone + '\'' +
                ", timestamp=" + timestamp +
                ", metric=" + metric +
                ", value=" + value +
                ", threshold=" + threshold +
                ", message='" + message + '\'' +
                '}';
    }
}