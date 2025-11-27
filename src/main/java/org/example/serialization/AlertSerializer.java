package org.example.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.example.model.Alert;

public class AlertSerializer implements SerializationSchema<Alert> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(Alert alert) {
        ObjectNode alertNode = objectMapper.createObjectNode();
        alertNode.put("alertId", alert.getAlertId().toString());
        alertNode.put("hostId", alert.getHostId());
        alertNode.put("zone", alert.getZone());
        alertNode.put("timestamp", alert.getTimestamp());
        alertNode.put("metric", alert.getMetric().name());
        alertNode.put("value", alert.getValue());
        alertNode.put("threshold", alert.getThreshold());
        alertNode.put("message", alert.getMessage());
        
        try {
            return objectMapper.writeValueAsBytes(alertNode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize alert", e);
        }
    }
}