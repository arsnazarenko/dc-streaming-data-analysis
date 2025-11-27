package org.example.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;

public class SlaComplianceSerializer implements SerializationSchema<Tuple3<String, String, Double>> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(Tuple3<String, String, Double> element) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("hostId", element.f0);
        node.put("metric", element.f1);
        node.put("violationPercentage", element.f2);
        node.put("timestamp", System.currentTimeMillis());
        
        try {
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize SLA compliance", e);
        }
    }
}