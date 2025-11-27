package org.example.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class CpuTrendSerializer implements SerializationSchema<Tuple2<String, Double>> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(Tuple2<String, Double> element) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("hostId", element.f0);
        node.put("hourlyCpuSum", element.f1);
        node.put("timestamp", System.currentTimeMillis());
        
        try {
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize CPU trend", e);
        }
    }
}