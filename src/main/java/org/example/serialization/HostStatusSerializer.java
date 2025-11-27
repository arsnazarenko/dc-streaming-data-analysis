package org.example.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;

public class HostStatusSerializer implements SerializationSchema<Tuple3<String, String, Double>> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(Tuple3<String, String, Double> element) {
        ObjectNode node = objectMapper.createObjectNode();
        String[] parts = element.f0.split(":");
        String hostId = parts[0];
        String zone = parts[1];
        
        node.put("hostId", hostId);
        node.put("zone", zone);
        node.put("metric", element.f1);
        node.put("averageValue", element.f2);
        node.put("timestamp", System.currentTimeMillis());
        
        try {
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize host status", e);
        }
    }
}