# Backend Interview Assignment

**Kafka Consumer in Go (`kafka-go`)**

## Objective

Design and implement a Kafka consumer in Go that demonstrates production-grade consumption patterns, with a focus on correctness, concurrency, and graceful shutdown.

The assignment centers on improving a Kafka consumer to:

- Guarantee **at-least-once** message processing
- Process messages **concurrently** to improve throughput
- Handle failures with retries
- Shut down gracefully (finish in-flight work and commit only processed offsets)

---

## Requirements

### Consumption & Concurrency

- Messages must be processed **concurrently across partitions**
- Messages must also be processed **concurrently within the same partition**
- Ordering **does not need to be preserved**

### Delivery Semantics

- **At-least-once** delivery is required
- **Duplicates are acceptable**
- **Message loss is not acceptable**
- Exactly-once processing is **not required**

### Failure Handling

- Failed message processing must be handled explicitly (e.g., retry, backoff, dead-letter, etc.)
- Offsets must be committed **only after successful processing**

### Implementation Freedom

- You may use:
  - A **consumer group**, or
  - **Direct partition consumption**
- You may modify the existing code, replace components, introduce additional libraries, or build the solution from scratch.
- Whichever approach you choose, briefly explain the trade-offs and rationale

---

## Deliverables

1. Runnable Go source code
2. Short design write-up covering:
   - At-least-once strategy
   - Concurrency model
   - Offset commit approach

---

## Discussion Topics

Be prepared to discuss:

- Rebalancing and offset persistence
- Horizontal scaling
- Handling duplicates vs. failures
- Exactly-once processing trade-offs
- Message ordering requirements

---

## Current Implementation Snapshot

The runnable implementation in this repository currently chooses the **consumer-group** path.

- It uses one `kafka-go` reader configured with `GroupID` and consumes messages via `FetchMessage`.
- Fetched messages are dispatched into a shared worker pool, so concurrency happens both across partitions and within the same partition.
- The code keeps **per-partition offset state** and only commits when the contiguous completed frontier advances, which preserves at-least-once semantics under out-of-order completion.
- Failed processing is retried with bounded backoff. If retries are exhausted, the offset is intentionally left uncommitted so the message can be replayed after restart or rebalance.
- Shutdown is phased: the runtime waits for producer completion, gives the consumer a bounded catch-up window, stops intake, and drains in-flight work before exit.

## About This Repository

This repository provides a self-contained local Kafka environment.

### Environment

- Docker
- Required free ports:
  - `29092` — Kafka broker
  - `29093` — KRaft controller
- Go **1.25+**

### Run

```bash
go run ./cmd/app
```

When you run the code, it

- Pulls and starts a single-broker Kafka container (KRaft) via Docker SDK.
- Creates test-topic with 3 partitions.
- Starts a consumer group reading all partitions concurrently.
- Produces 10 messages with hash partitioning for key-based spread.
- Shuts down gracefully, then stops/kills the container on exit.
