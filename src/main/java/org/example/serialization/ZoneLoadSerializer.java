package org.example.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class ZoneLoadSerializer implements SerializationSchema<Tuple2<String, Double>> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(Tuple2<String, Double> element) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("zone", element.f0);
        node.put("totalCpuLoad", element.f1);
        node.put("timestamp", System.currentTimeMillis());
        
        try {
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize zone load", e);
        }
    }
}