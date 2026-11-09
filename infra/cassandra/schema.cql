-- Hamburg Airspace Monitor: Cassandra schema.
-- Query-first design: one table per question the processor or dashboard asks.
-- Safe to run again: IF NOT EXISTS leaves existing objects alone.

-- Replication factor 3 in data centre dc1: every row is stored on all 3 nodes.
CREATE KEYSPACE IF NOT EXISTS airspace
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

-- Q: Where was aircraft X during the last 10 minutes? (circling detection, movements)
-- Partition (hex, day): one aircraft's positions for one day, kept together on the same nodes.
-- Clustering pos_time DESC: newest position first.
-- Same (hex, day, pos_time) written twice = one row (upsert), so duplicates disappear.
CREATE TABLE IF NOT EXISTS airspace.positions_by_aircraft (
  hex              text,
  day              date,
  pos_time         timestamp,
  flight           text,
  registration     text,
  aircraft_type    text,
  category         text,
  lat              double,
  lon              double,
  alt_baro_ft      int,
  on_ground        boolean,
  gs_kt            double,
  track_deg        double,
  true_heading_deg double,
  squawk           text,
  emergency        text,
  seen_pos_s       double,
  is_stale         boolean,
  source           text,
  schema_version   int,
  ingested_at      timestamp,
  processed_at     timestamp,
  kafka_partition  int,
  kafka_offset     bigint,
  PRIMARY KEY ((hex, day), pos_time)
) WITH CLUSTERING ORDER BY (pos_time DESC)
  AND default_time_to_live = 604800;  -- rows expire after 7 days

-- Q: How many Hamburg arrivals and departures per 15-minute window?
-- One row per aircraft movement; counting rows gives the pulse (no counter columns).
CREATE TABLE IF NOT EXISTS airspace.movements_by_day (
  airport      text,
  day          date,
  window_start timestamp,
  hex          text,
  movement     text,       -- 'arrival' or 'departure'
  event_time   timestamp,
  flight       text,
  PRIMARY KEY ((airport, day), window_start, hex, movement)
) WITH CLUSTERING ORDER BY (window_start DESC, hex ASC, movement ASC)
  AND default_time_to_live = 604800;

-- Q: Which circling and emergency alerts happened today, newest first?
CREATE TABLE IF NOT EXISTS airspace.alerts_by_day (
  day          date,
  alert_time   timestamp,
  hex          text,
  alert_type   text,       -- 'circling' or 'emergency'
  flight       text,
  lat          double,
  lon          double,
  alt_baro_ft  int,
  detail       text,
  PRIMARY KEY ((day), alert_time, hex, alert_type)
) WITH CLUSTERING ORDER BY (alert_time DESC, hex ASC, alert_type ASC)
  AND default_time_to_live = 604800;

-- Q: Throughput, latency and data quality per processed batch.
-- Keyed by the Kafka partition and first offset of the batch, so re-processing
-- the same batch overwrites its own row.
CREATE TABLE IF NOT EXISTS airspace.batch_stats_by_day (
  day              date,
  batch_time       timestamp,
  kafka_partition  int,
  first_offset     bigint,
  records_in       int,
  records_valid    int,
  records_dlq      int,
  records_excluded int,
  records_stale    int,
  duplicates       int,
  missing_track    int,
  missing_speed    int,
  missing_altitude int,
  implausible      int,
  latency_p50_ms   double,
  latency_p95_ms   double,
  latency_max_ms   double,
  PRIMARY KEY ((day), batch_time, kafka_partition, first_offset)
) WITH CLUSTERING ORDER BY (batch_time DESC, kafka_partition ASC, first_offset ASC)
  AND default_time_to_live = 604800;
