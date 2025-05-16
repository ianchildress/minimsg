

---

## 1. Core Messaging Primitives

* **Topics / Streams**
  Logical channels that producers write to and consumers subscribe from.
* **Partitions**
  Scale-out mechanism: each topic split into ordered, independently consumable shards.
* **Consumer Groups**
  Coordinate multiple consumers for load-balanced, fault-tolerant processing.
* **Offset Management**
  Track “where you left off” per consumer (auto-commit vs. manual commit).

---

## 2. Delivery Semantics & Ordering

* **At-least-once vs. At-most-once vs. Exactly-once**
  Configurable guarantees around message duplication and loss.
* **Per-partition Ordering**
  Ensure messages with the same key always arrive in order.
* **Idempotence & Transactions**
  Producer-side dedupe and multi-message atomic writes.

---

## 3. Performance & Scalability

* **Batching & Compression**
  Group multiple records into a single frame; gzip/Snappy/LZ4 for wire savings.
* **Backpressure & Flow Control**
  Throttling producers or slowing consumers to avoid overload.
* **Retention & Quotas**
  Time- or size-based log retention, per-topic or per-client quotas.

---

## 4. Reliability & Durability

* **Replication**
  Keep multiple copies of data across broker nodes for fault tolerance.
* **Leader Election**
  Automated failover when a partition leader dies.
* **Durable Write-Ahead Logs**
  Append-only segment files on disk; safe recovery after crashes.

---

## 5. Security & Operations

* **TLS + SASL**
  Encrypted transport and pluggable authentication (e.g. SCRAM, OAuth).
* **ACLs / Authorization**
  Control which clients can read or write each topic.
* **Metrics & Monitoring**
  Expose per-broker and per-topic stats (throughput, lag, queue length).
* **Management API / Admin Tooling**
  Create/delete topics, adjust retention, observe consumer-group offsets.

---

## 6. Developer Ergonomics

* **Multiple Language Clients**
  Idiomatic libraries for Go, Java, Python, etc., with consistent behavior.
* **REST / HTTP Proxy**
  Allow lightweight HTTP/JSON access alongside binary clients.
* **Schema Registry / Compatibility**
  Centralize message schemas (Avro/Protobuf/JSON) and enforce compatibility.

---

## Common Use Cases

1. **Metrics & Telemetry Ingestion**
   Collect real-time metrics from apps, edge devices, or IoT sensors.
2. **Log Aggregation & Processing**
   Centralize application logs and feed them into analytics pipelines.
3. **Event Sourcing**
   Persist every state change as an immutable event stream (e.g. CQRS).
4. **Stream Processing**
   Real-time transformations, filtering, enrichments via frameworks (e.g. Kafka Streams).
5. **Microservice Communication**
   Decouple services with pub/sub or durable queues for asynchronous jobs.
6. **Change Data Capture (CDC)**
   Capture DB change-events and propagate them to downstream consumers.
7. **Job Scheduling / Work Queues**
   Dispatch tasks (e.g. image processing) with worker pools and retry semantics.
8. **User Activity Tracking**
   Clickstreams or user interactions for personalization and A/B testing.

---


# Partitions

## 1. **Message Offset**

* **What it is:** A simple, ever-increasing integer (per-partition) that uniquely identifies each record’s position.
* **Why you need it:**

    * Consumers use it to know “where to start” or “where I left off.”
    * Replicas compare offsets to determine how far behind they are.

---

## 2. **Log Segment Boundaries**

* **What it is:** You typically break the on-disk log into numbered segment files (e.g. `00000000000000000000.log`, `00000000000000000001.log`, …).
* **Why you need it:**

    * Fast startup (you only map in the active segment).
    * Efficient purging (drop entire old segments once their offsets fall below retention).

---

## 3. **High Watermark (HW) & Low Watermark (LW)**

* **HW (committed offset):** The highest offset such that all in-sync replicas have that message.  Only messages ≤ HW are safe for consumers to read.
* **LW (log start offset):** The smallest offset still retained on disk (everything below gets purged).

---

## 4. **Per-Consumer-Group Committed Offsets**

* **What it is:** For each `(groupID, topic, partition)` tuple, you remember the last offset the consumer group has acknowledged.
* **Why you need it:**

    * Enables fault-tolerant “replay” if a consumer dies or you rebalance.
    * Allows you to compute consumer lag (`HW – committedOffset`).

---

## 5. **Timestamps & Metadata**

* **Message Timestamps:** You may store both a “create time” (producer-attached) and “append time” (broker’s reception time).  Useful for age-based retention, SLA metrics, watermarking in stream processing.
* **Message Key / Headers:** If you support keys or headers, store them alongside so consumers can partition-route or apply filters.

---

## 6. **In-Memory Indexes**

* **Offset Index:** A sparse on-disk index that maps offsets → byte-positions in the segment file, so you can do a binary search to find “offset = 12345” without scanning the whole file.
* **Time Index (optional):** If you need to look up by timestamp, a similar index maps time → offset.

---

## 7. **Broker Metrics per Partition**

Track operational metrics so you can monitor health and performance:

* **Bytes in/out** (per second)
* **Messages in/out**
* **Under-replicated replicas** count
* **Consumer lag** per group
* **Disk usage** (segment count, size)

---

### Putting it all together

1. **Append** a new record → assign `offset = lastOffset+1`, write into the active segment file, update offset index.
2. **Flush/replicate** → once replicas acknowledge up to N, advance the high-watermark.
3. **Consume** → clients read from any offset ≤ HW, then commit their own offset back to the broker’s offset store.
4. **Purge** → when `LW + retentionSize < currentOffset` or `messageAge > retentionTime`, drop old segments and bump `LW`.


# Partitioning Strategies

---

## 1. Round-Robin (or Random)

* **How it works:** Each incoming message is sent to the next partition in sequence (or chosen uniformly at random).
* **Purpose:**

    * **Even load distribution** across all partitions/brokers
    * **Maximize parallelism** when you don’t care about ordering or grouping

---

## 2. Key-Based (Hash)

* **How it works:** You pick a “key” (e.g. user ID, device ID) and hash it to decide the partition:

  ```go
  partition = hash(key) % numPartitions
  ```
* **Purpose:**

    * **Per-key ordering**: all messages for the same key always go to the same partition
    * **Affinity**: group related events (e.g. all orders for one customer) together

---

## 3. Range-Based

* **How it works:** You predefine numeric (or lexicographic) ranges and map them to partitions. E.g.:

    * Partition 0: IDs 0–999
    * Partition 1: IDs 1000–1999
    * …
* **Purpose:**

    * **Predictable grouping** when your keyspace is naturally ordered (timestamps, numeric IDs)
    * **Archival or compaction** strategies per range

---

## 4. Sticky (or “Batch”) Partitioning

* **How it works:** For each batch of messages, pick one partition and send the entire batch there. When the batch flushes, pick a new partition.
* **Purpose:**

    * **Batch efficiency**: keep related messages in the same TCP batch/frame
    * **Still fairly balanced** over time

---

## 5. Custom / Domain-Specific

* **How it works:** You write your own partitioner based on business logic—e.g.,

    * Round-robin across a subset of “hot” partitions
    * Geo-aware routing: EU traffic → EU partitions
* **Purpose:**

    * **Optimize for specific patterns** (hot keys, regional locality, SLA requirements)

---

### Why Partition at All?

1. **Scalability & Throughput**
   Spread load across multiple brokers and CPU cores.
2. **Parallelism**
   Multiple consumers in a group can each read from different partitions.
3. **Ordering Guarantees**
   You get “strict ordering” only within a single partition.
4. **Fault Isolation**
   If one partition/broker fails, others remain available.



---