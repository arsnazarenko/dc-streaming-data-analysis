package org.example.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class ZoneLoadKeySerializer implements SerializationSchema<Tuple2<String, Double>> {
    
    @Override
    public byte[] serialize(Tuple2<String, Double> element) {
        return element.f0.getBytes(); // zone as key
    }
}