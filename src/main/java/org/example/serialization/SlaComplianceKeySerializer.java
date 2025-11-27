package org.example.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;

public class SlaComplianceKeySerializer implements SerializationSchema<Tuple3<String, String, Double>> {
    
    @Override
    public byte[] serialize(Tuple3<String, String, Double> element) {
        return element.f0.getBytes(); // hostId as key
    }
}