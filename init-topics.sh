#!/bin/sh

# Create topics
kafka-topics --create --topic dc_metrics --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3 --if-not-exists
kafka-topics --create --topic dc_alerts --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3 --if-not-exists
kafka-topics --create --topic dc_host_status --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3 --if-not-exists
kafka-topics --create --topic dc_sla_compliance --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3 --if-not-exists
kafka-topics --create --topic dc_zone_load --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3 --if-not-exists
kafka-topics --create --topic dc_cpu_load --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3 --if-not-exists

echo "Topics created successfully"
