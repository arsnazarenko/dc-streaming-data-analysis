package org.example.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.example.model.Alert;

public class AlertKeySerializer implements SerializationSchema<Alert> {
    
    @Override
    public byte[] serialize(Alert alert) {
        return alert.getHostId().getBytes(); // hostId as key
    }
}