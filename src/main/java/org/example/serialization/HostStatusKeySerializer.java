package org.example.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;

public class HostStatusKeySerializer implements SerializationSchema<Tuple3<String, String, Double>> {
    
    @Override
    public byte[] serialize(Tuple3<String, String, Double> element) {
        String[] parts = element.f0.split(":");
        String hostId = parts[0];
        return hostId.getBytes();
    }
}