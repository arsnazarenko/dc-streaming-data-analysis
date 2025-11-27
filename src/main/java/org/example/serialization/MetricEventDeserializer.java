package org.example.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.example.model.MetricEvent;
import org.example.model.MetricType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MetricEventDeserializer implements DeserializationSchema<MetricEvent> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public MetricEvent deserialize(byte[] message) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(message);
        
        if (jsonNode == null || jsonNode.isNull()) {
            throw new IOException("Received null or empty JSON message");
        }
        
        JsonNode eventIdNode = jsonNode.get("event_id");
        JsonNode hostIdNode = jsonNode.get("host_id");
        JsonNode zoneNode = jsonNode.get("zone");
        JsonNode timestampNode = jsonNode.get("timestamp");
        JsonNode metricNode = jsonNode.get("metric");
        JsonNode valueNode = jsonNode.get("value");
        JsonNode unitNode = jsonNode.get("unit");

        
        if (eventIdNode == null || hostIdNode == null || zoneNode == null || 
            timestampNode == null || metricNode == null || valueNode == null || unitNode == null) {
            throw new IOException("Missing required fields in JSON message. Received: " + jsonNode.toString());
        }
        
        UUID eventId = UUID.fromString(eventIdNode.asText());
        String hostId = hostIdNode.asText();
        String zone = zoneNode.asText();
        long timestamp = timestampNode.asLong();
        MetricType metric = MetricType.valueOf(metricNode.asText());
        double value = valueNode.asDouble();
        String unit = unitNode.asText();
        
        Map<String, Object> tags = new HashMap<>();
        if (jsonNode.has("tags") && !jsonNode.get("tags").isNull()) {
            JsonNode tagsNode = jsonNode.get("tags");
            tagsNode.properties().forEach(entry -> {
                tags.put(entry.getKey(), entry.getValue().asText());
            });
        }
        
        return new MetricEvent(eventId, hostId, zone, timestamp, metric, value, unit, tags);
    }
    
    @Override
    public boolean isEndOfStream(MetricEvent nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeExtractor.getForClass(MetricEvent.class);
    }
}
