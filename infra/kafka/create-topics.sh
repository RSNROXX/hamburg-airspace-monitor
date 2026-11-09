#!/usr/bin/env bash
# Creates the project's Kafka topics. Safe to run again: existing topics are left alone.
set -euo pipefail

BOOTSTRAP="kafka-1:19092,kafka-2:19092,kafka-3:19092"
TOPICS="/opt/kafka/bin/kafka-topics.sh"
SEVEN_DAYS_MS=604800000

# Main stream: one message per aircraft position, keyed by aircraft ID.
"$TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic adsb.positions --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms="$SEVEN_DAYS_MS"

# Dead-letter topic: records that failed validation, kept for inspection.
"$TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic adsb.dlq --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms="$SEVEN_DAYS_MS"

echo "Topics ready:"
"$TOPICS" --bootstrap-server "$BOOTSTRAP" --describe --topic adsb.positions
"$TOPICS" --bootstrap-server "$BOOTSTRAP" --describe --topic adsb.dlq
