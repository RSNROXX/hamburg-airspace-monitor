# Design decisions

Each decision states what was chosen, why, and what was rejected. Decisions 11 to 13 were changed after the first runs on live data.

## 1. Poll the source every 7 seconds

adsb.lol offers no push feed, so the producer polls. Polling is legitimate here because aircraft positions change faster than the interval, so every poll returns new events. The first interval was 5 seconds. The logs then showed HTTP 429 about once a minute: the source allows about 10 requests per minute. 7 seconds stays under that limit and gives evenly spaced samples. A turning airliner changes direction by about 21 degrees between samples, which is precise enough to follow a circle.

Rejected: 1 to 3 seconds (rate limited, mostly repeated positions) and 30 seconds (about 90 degrees per sample, so the turn direction becomes ambiguous).

## 2. Message key is the aircraft ID

Kafka assigns a partition with `hash(key) % partitions`. Keying by aircraft ID puts all positions of one aircraft in one partition, in order, and one partition is read by exactly one processor. Circling detection needs that order.

Rejected: no key or a random key, which spreads one aircraft across partitions and processors.

## 3. Three Kafka brokers with replication factor 3 and `min.insync.replicas=2`

The reason is survival, not capacity. One broker can fail and writes continue, because two replicas remain. Three KRaft controllers keep a majority of two when one fails. The producer uses `acks=all` and idempotence, and unclean leader election is disabled, so a confirmed message is never lost and never duplicated by a retry.

Rejected: one broker (no failover) and two brokers (the controller vote would need both).

## 4. Cassandra with three nodes, replication factor 3 and LOCAL_QUORUM

Cassandra is masterless: every node is equal and any node can coordinate a request, so no single node's failure stops the system. It is built for heavy time-stamped writes. With R = 2, W = 2 and N = 3, R + W > N, so every read sees the latest write and one node can be down. Each node is in its own rack so the three copies of a row are spread out.

Rejected: PostgreSQL (one primary to fail over) and MongoDB (a primary election pauses writes).

## 5. One table per question, no counters, time buckets

Cassandra cannot join or filter freely, so each table answers one question: positions by aircraft, movements by day, alerts by day, batch statistics by day, live aircraft by minute. Partition keys include the day or the minute so partitions stay small.

Counter columns are not used. A counter is not idempotent: a message read twice would count twice. Counts are made by counting rows instead.

## 6. Commit after the write, and make every write an upsert

The processor commits Kafka offsets only after all Cassandra writes of a batch succeeded. A failure therefore causes a re-read, never a loss. Every table has a natural primary key, so writing the same row again overwrites it. At-least-once delivery plus idempotent writes gives an exactly-once result.

After three failed attempts the processor exits without committing instead of skipping the batch. Docker restarts it.

## 7. Raw data in Kafka, cleaning in the processor

The producer passes the source fields on unchanged and only adds metadata (source, schema version, poll ID, ingestion time, event time). If a cleaning rule has a bug, the raw messages are still in Kafka for 7 days and can be processed again.

## 8. Invalid records go to a dead-letter topic

Records that fail the Pandera schema are written to `adsb.dlq` together with the reasons and their original position in the topic. One bad record never stops the pipeline, and nothing is thrown away unseen.

## 9. Event time, rounded to the second

Event time is the moment the aircraft was at the position: the server time of the response minus the age of the position. It is used for windows, for latency and as part of the primary key. It is rounded to the whole second, because the same position seen in two polls can differ by a millisecond after the calculation, and rounding lets the primary key merge the two.

## 10. Circling is a signed turn of 360 degrees within 10 minutes

The processor adds up the change in ground track between consecutive positions, each taken between -180 and +180 degrees. Left and right turns cancel, so S-turns on approach do not count. Turns across gaps longer than 30 seconds are ignored. Each episode alerts once.

## 11. Only airliners raise circling alerts (changed after live data)

The first 10 circling alerts were helicopters and light aircraft. They circle for police work, training and sightseeing, which says nothing about airport congestion. Circling alerts are now limited to airliners. The dashboard watchlist shows airliners only for the same reason.

## 12. Airliners are identified by aircraft type first (changed after live data)

The ADS-B emitter category is set by hand in each transponder and is unreliable: an Airbus A321 broadcast category A0 ("no information"). The aircraft type comes from the source's aircraft database and is used first. The category is the fallback when the type is missing.

## 13. Emergencies need confirmation (changed after live data)

Single messages reported "unlawful interference" and "downed" for normal flights: corrupted radio messages that decoded into valid values. An emergency alert now needs the same signal in at least 3 positions within 2 minutes. The status "lifeguard" marks a medical priority flight and is not treated as an emergency.

## 14. Streamlit for the dashboard

The dashboard is written in Python like the rest of the system and reads Cassandra through the official driver. It reads only from Cassandra, so it can fail or restart without touching the pipeline.

Rejected: Grafana, which needs a community plugin for Cassandra.

## 15. Containers and safe defaults

Every service runs in its own container with pinned versions, a restart policy and, for Kafka, Cassandra and the dashboard, a health check. Topics and tables are created by one-shot jobs that are safe to run again. The Python services run as an unprivileged user. Published ports are bound to localhost. Local settings live in `.env`, which is not committed.
