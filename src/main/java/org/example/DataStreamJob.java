package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.model.Alert;
import org.example.model.MetricEvent;
import org.example.model.MetricType;
import org.example.serialization.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class DataStreamJob {
    
    private static final String KAFKA_BROKERS = "kafka1:9092,kafka2:9092,kafka3:9092";
    private static final String METRICS_TOPIC = "dc_metrics";
    private static final String ALERTS_TOPIC = "dc_alerts";
    private static final String HOST_STATUS_TOPIC = "dc_host_status";
    private static final String SLA_COMPLIANCE_TOPIC = "dc_sla_compliance";
    private static final String ZONE_LOAD_TOPIC = "dc_zone_load";
    private static final String CPU_TREND_TOPIC = "dc_cpu_trend";
    private static final String CONSUMER_GROUP = "dc-metrics-consumer";
    
    private static final int AUTO_WATERMARK_INTERVAL_MS = 200;
    private static final int BOUNDED_OUT_OF_ORDERNESS_SECONDS = 2;
    
    private static final int CURRENT_STATUS_WINDOW_SECONDS = 10;
    private static final int SLA_WINDOW_SIZE_MINUTES = 5;
    private static final int SLA_WINDOW_SLIDE_MINUTES = 1;
    private static final int ZONE_LOAD_WINDOW_MINUTES = 1;
    private static final int HOURLY_CPU_TREND_WINDOW_HOURS = 1;
    
    private static final double CPU_USAGE_CRITICAL_THRESHOLD = 95.0;
    private static final double CPU_TEMP_CRITICAL_THRESHOLD = 85.0;
    private static final double CPU_USAGE_SLA_THRESHOLD = 85.0;
    private static final double MEM_USAGE_SLA_THRESHOLD = 90.0;
    private static final double DISK_IO_SLA_THRESHOLD = 1000.0;  // MB/s - high disk usage
    private static final double NET_IO_SLA_THRESHOLD = 600.0;   // MB/s - high network usage
    
    private static final String CPU_USAGE_CRITICAL_MSG = "CPU usage critical: ";
    private static final String CPU_TEMP_CRITICAL_MSG = "CPU temperature critical: ";
    private static final String PERCENT_SUFFIX = "%";
    private static final String CELSIUS_SUFFIX = "°C";
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.getConfig().setAutoWatermarkInterval(AUTO_WATERMARK_INTERVAL_MS);
        
        DataStream<MetricEvent> metrics = createKafkaSource(env);
        
        SingleOutputStreamOperator<Alert> alerts = processRealTimeAlerts(metrics);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> currentStatus = processCurrentHostStatus(metrics);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> slaCompliance = processSlaCompliance(metrics);
        SingleOutputStreamOperator<Tuple2<String, Double>> zoneLoad = processZoneLoad(metrics);
        SingleOutputStreamOperator<Tuple2<String, Double>> hourlyCpuTrend = processHourlyCpuTrend(metrics);
        
        KafkaSink<Alert> alertSink = createAlertSink();
        KafkaSink<Tuple3<String, String, Double>> hostStatusSink = createHostStatusSink();
        KafkaSink<Tuple3<String, String, Double>> slaComplianceSink = createSlaComplianceSink();
        KafkaSink<Tuple2<String, Double>> zoneLoadSink = createZoneLoadSink();
        KafkaSink<Tuple2<String, Double>> cpuTrendSink = createCpuTrendSink();
        
        alerts.sinkTo(alertSink);
        currentStatus.sinkTo(hostStatusSink);
        slaCompliance.sinkTo(slaComplianceSink);
        zoneLoad.sinkTo(zoneLoadSink);
        hourlyCpuTrend.sinkTo(cpuTrendSink);
        
        env.execute("DC Metrics Analysis");
    }
    
    private static DataStream<MetricEvent> createKafkaSource(StreamExecutionEnvironment env) {
        KafkaSource<MetricEvent> source = KafkaSource.<MetricEvent>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(METRICS_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new MetricEventDeserializer())
                .build();
        
        return env.fromSource(source, 
                WatermarkStrategy.<MetricEvent>forBoundedOutOfOrderness(Duration.ofSeconds(BOUNDED_OUT_OF_ORDERNESS_SECONDS))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()), 
                "Kafka Source");
    }
    
    private static KafkaSink<Alert> createAlertSink() {
        return KafkaSink.<Alert>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                // .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ALERTS_TOPIC)
                        .setKeySerializationSchema(new AlertKeySerializer())
                        .setValueSerializationSchema(new AlertSerializer())
                        .build())
                
                
                .build();
    }
    
private static KafkaSink<Tuple3<String, String, Double>> createHostStatusSink() {
        return KafkaSink.<Tuple3<String, String, Double>>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                // .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(HOST_STATUS_TOPIC)
                        .setKeySerializationSchema(new HostStatusKeySerializer())
                        .setValueSerializationSchema(new HostStatusSerializer())
                        .build())
                .build();
    }
    
    private static KafkaSink<Tuple3<String, String, Double>> createSlaComplianceSink() {
        return KafkaSink.<Tuple3<String, String, Double>>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                // .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(SLA_COMPLIANCE_TOPIC)
                        .setKeySerializationSchema(new SlaComplianceKeySerializer())
                        .setValueSerializationSchema(new SlaComplianceSerializer())
                        .build())
                
                .build();
    }
    
    private static KafkaSink<Tuple2<String, Double>> createZoneLoadSink() {
        return KafkaSink.<Tuple2<String, Double>>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                // .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ZONE_LOAD_TOPIC)
                        .setKeySerializationSchema(new ZoneLoadKeySerializer())
                        .setValueSerializationSchema(new ZoneLoadSerializer())
                        .build())
                
                .build();
    }
    
    private static KafkaSink<Tuple2<String, Double>> createCpuTrendSink() {
        return KafkaSink.<Tuple2<String, Double>>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                // .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(CPU_TREND_TOPIC)
                        .setKeySerializationSchema(new CpuTrendKeySerializer())
                        .setValueSerializationSchema(new CpuTrendSerializer())
                        .build())
                .build();
    }
    
    private static SingleOutputStreamOperator<Alert> processRealTimeAlerts(DataStream<MetricEvent> metrics) {
        return metrics.keyBy(MetricEvent::getHostId)
                .process(new KeyedProcessFunction<>() {
                    @Override
                    public void processElement(MetricEvent event, Context ctx, Collector<Alert> out) {
                        if (event.getMetric() == MetricType.CPU_USAGE && event.getValue() > CPU_USAGE_CRITICAL_THRESHOLD) {
                            out.collect(new Alert(
                                java.util.UUID.randomUUID(),
                                event.getHostId(),
                                event.getZone(),
                                event.getTimestamp(),
                                event.getMetric(),
                                event.getValue(),
                                CPU_USAGE_CRITICAL_THRESHOLD,
                                CPU_USAGE_CRITICAL_MSG + event.getValue() + PERCENT_SUFFIX
                            ));
                        }
                        
                        if (event.getMetric() == MetricType.CPU_TEMP && event.getValue() > CPU_TEMP_CRITICAL_THRESHOLD) {
                            out.collect(new Alert(
                                java.util.UUID.randomUUID(),
                                event.getHostId(),
                                event.getZone(),
                                event.getTimestamp(),
                                event.getMetric(),
                                event.getValue(),
                                CPU_TEMP_CRITICAL_THRESHOLD,
                                CPU_TEMP_CRITICAL_MSG + event.getValue() + CELSIUS_SUFFIX
                            ));
                        }
                    }
                });
    }
    
    private static SingleOutputStreamOperator<Tuple3<String, String, Double>> processCurrentHostStatus(DataStream<MetricEvent> metrics) {
        return metrics.keyBy(event -> event.getHostId() + ":" + event.getMetric())
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(CURRENT_STATUS_WINDOW_SECONDS)))
                .aggregate(new HostStatusAggregate());
    }
    
    private static SingleOutputStreamOperator<Tuple3<String, String, Double>> processSlaCompliance(DataStream<MetricEvent> metrics) {
        return metrics.keyBy(MetricEvent::getHostId)
                .window(SlidingEventTimeWindows.of(
                        Duration.ofMinutes(SLA_WINDOW_SIZE_MINUTES), 
                        Duration.ofMinutes(SLA_WINDOW_SLIDE_MINUTES)))
                .process(new SlaProcessWindowFunction());
    }
    
    private static SingleOutputStreamOperator<Tuple2<String, Double>> processZoneLoad(DataStream<MetricEvent> metrics) {
        return metrics.keyBy(MetricEvent::getZone)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(ZONE_LOAD_WINDOW_MINUTES)))
                .aggregate(new ZoneLoadAggregate());
    }
    
    private static SingleOutputStreamOperator<Tuple2<String, Double>> processHourlyCpuTrend(DataStream<MetricEvent> metrics) {
        return metrics.filter(event -> event.getMetric() == MetricType.CPU_USAGE)
                .keyBy(MetricEvent::getHostId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(HOURLY_CPU_TREND_WINDOW_HOURS)))
                .aggregate(new CpuTrendAggregate());
    }
    
    public static class HostStatusAggregate implements AggregateFunction<MetricEvent, Tuple3<Double, Long, String>, Tuple3<String, String, Double>> {
        @Override
        public Tuple3<Double, Long, String> createAccumulator() {
            return Tuple3.of(0.0, 0L, "");
        }
        
        @Override
        public Tuple3<Double, Long, String> add(MetricEvent event, Tuple3<Double, Long, String> accumulator) {
            String key = event.getHostId() + ":" + event.getZone() + ":" + event.getMetric().name();
            return Tuple3.of(accumulator.f0 + event.getValue(), accumulator.f1 + 1, key);
        }
        
        @Override
        public Tuple3<String, String, Double> getResult(Tuple3<Double, Long, String> accumulator) {
            String[] parts = accumulator.f2.split(":");
            String hostId = parts[0];
            String zone = parts[1];
            String metric = parts[2];
            double avg = accumulator.f1 > 0 ? accumulator.f0 / accumulator.f1 : 0.0;
            return Tuple3.of(hostId + ":" + zone, metric, avg);
        }
        
        @Override
        public Tuple3<Double, Long, String> merge(Tuple3<Double, Long, String> a, Tuple3<Double, Long, String> b) {
            return Tuple3.of(a.f0 + b.f0, a.f1 + b.f1, a.f2);
        }
    }
    
    public static class SlaProcessWindowFunction extends ProcessWindowFunction<MetricEvent, Tuple3<String, String, Double>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<MetricEvent> elements, Collector<Tuple3<String, String, Double>> out) {
            Map<MetricType, Integer> totalCounts = new HashMap<>();
            Map<MetricType, Integer> violationCounts = new HashMap<>();
            
            for (MetricEvent event : elements) {
                totalCounts.merge(event.getMetric(), 1, Integer::sum);
                
                boolean isViolation = switch (event.getMetric()) {
                    case CPU_USAGE -> event.getValue() > CPU_USAGE_SLA_THRESHOLD;
                    case MEM_USAGE -> event.getValue() > MEM_USAGE_SLA_THRESHOLD;
                    case DISK_IO_READ, DISK_IO_WRITE -> event.getValue() > DISK_IO_SLA_THRESHOLD;
                    case NET_IN, NET_OUT -> event.getValue() > NET_IO_SLA_THRESHOLD;
                    default -> false;
                };

                if (isViolation) {
                    violationCounts.merge(event.getMetric(), 1, Integer::sum);
                }
            }
            
            for (Map.Entry<MetricType, Integer> entry : totalCounts.entrySet()) {
                MetricType metric = entry.getKey();
                int total = entry.getValue();
                int violations = violationCounts.getOrDefault(metric, 0);
                double violationPercentage = total > 0 ? (double) violations / total * 100.0 : 0.0;
                
                out.collect(Tuple3.of(key, metric.name(), violationPercentage));
            }
        }
    }
    
    public static class ZoneLoadAggregate implements AggregateFunction<MetricEvent, Tuple2<Double, String>, Tuple2<String, Double>> {
        @Override
        public Tuple2<Double, String> createAccumulator() {
            return Tuple2.of(0.0, "");
        }
        
        @Override
        public Tuple2<Double, String> add(MetricEvent event, Tuple2<Double, String> accumulator) {
            if (event.getMetric() == MetricType.CPU_USAGE) {
                return Tuple2.of(accumulator.f0 + event.getValue(), event.getZone());
            }
            return accumulator;
        }
        
        @Override
        public Tuple2<String, Double> getResult(Tuple2<Double, String> accumulator) {
            return Tuple2.of(accumulator.f1, accumulator.f0);
        }
        
        @Override
        public Tuple2<Double, String> merge(Tuple2<Double, String> a, Tuple2<Double, String> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1);
        }
    }
    
    public static class CpuTrendAggregate implements AggregateFunction<MetricEvent, Tuple2<Double, String>, Tuple2<String, Double>> {
        @Override
        public Tuple2<Double, String> createAccumulator() {
            return Tuple2.of(0.0, "");
        }
        
        @Override
        public Tuple2<Double, String> add(MetricEvent event, Tuple2<Double, String> accumulator) {
            return Tuple2.of(accumulator.f0 + event.getValue(), event.getHostId());
        }
        
        @Override
        public Tuple2<String, Double> getResult(Tuple2<Double, String> accumulator) {
            return Tuple2.of(accumulator.f1, accumulator.f0);
        }
        
        @Override
        public Tuple2<Double, String> merge(Tuple2<Double, String> a, Tuple2<Double, String> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1);
        }
    }
}
