# Bank Liquidity Management System — Build Spec & Tutoring Guide

> **How to use this document with Claude Code.**
> This is a learning build, not a delivery sprint. I want to be *tutored* through it, one microservice at a time, in the order given. For each step:
> 1. Explain the concepts involved before writing code, and check I understand them.
> 2. Have me implement as much as I can myself; fill gaps and correct rather than dumping a finished service.
> 3. After each service works end-to-end, pause and let me drive a test scenario before moving on.
> 4. Do **not** skip ahead to the money-movement services (5–8) until the read pipeline (1–4) is visibly working.
> I already know Kafka, Redis, and Spring Boot pipelines reasonably well; anchor new ideas to those where you can. Bias toward making me reason through failure modes rather than handing me the happy path.

---

## 1. What we're building and why

A bank's treasury desk must see **real-time cash positions** across ~40 currencies, ~200 correspondent banks, and ~15 internal entities, and must **trigger automated funding sweeps** (moving idle funds to where they're needed) before it runs short of intraday liquidity.

The hard part is not moving money — it's **trusting the number**. Positions are derived from thousands of noisy, sometimes-late, sometimes-silent feeds. The desk needs freshness measured in seconds, but a sweep is a real, irreversible wire, so the system must never act on a number it isn't sure about.

That single asymmetry shapes everything:

> **The read side tolerates staleness. The money side tolerates nothing ambiguous.**

The system is two halves joined by a gate:

- A **read/aggregation pipeline** whose only job is to answer, continuously and honestly, *"what is our position right now, and how much do we trust it?"*
- A **money-movement path** that acts on that position — but only when it's safe (data not stale, FX assumptions still valid, and execution idempotent).

### The four failure scenarios the design must survive

These are the acceptance criteria for the whole project. Every one should be reproducible on demand.

1. **Late / out-of-order message** — a transaction arrives after later-timestamped ones. It must *correct* the position, not corrupt it.
2. **Silent feed** — a correspondent stops sending. The system must know whether the position is *wrong* or merely *stale*, and react differently. Stale ≠ zero, stale ≠ error.
3. **Double-fire on restart** — a sweep is sent, the service crashes before confirmation, and restarts. It must recognise the wire is already in flight instead of re-sending.
4. **EOD reconciliation break** — the reported position is off from the nostro statement. Reconciliation must explain *why* (categorized break), not just flag the gap.
5. **Peg break** — a currency peg breaks overnight, invalidating FX models. Automated sweeps must halt themselves *before* acting on stale assumptions, and fail closed if the health check itself is unreachable.

---

## 2. Terms to read up on

Grouped in study order. I don't need to master all of this before coding — the domain terms and the first architecture patterns are enough to start; the rest I'll absorb as each service forces me to. Claude Code: when a term first becomes load-bearing for a step, check I actually understand it before we build on it.

### Domain / banking
- Cash position; intraday liquidity; funding sweep; treasury desk
- Nostro / vostro accounts; correspondent bank; internal entity
- SWIFT messages: MT940 & MT950 (statements), MT103 (credit transfer); MX / ISO 20022 (XML successor)
- SWIFT gpi and UETR (unique end-to-end transaction reference)
- FX peg, currency band, intraday liquidity buffer
- EOD reconciliation; break categorization (timing difference, missing item, FX mismatch, booking error)

### Distributed systems / architecture patterns
- Event sourcing; append-only log
- CQRS; read models / materialized views
- Event-time vs processing-time; watermarks; late & out-of-order events
- Idempotency & idempotency keys (deterministic vs random)
- Outbox pattern (transactional outbox); the dual-write problem
- Exactly-once vs at-least-once delivery; consumer offsets
- Circuit breaker; fail-closed vs fail-open
- Heartbeat / liveness detection; health monitoring
- Saga pattern (relevant later for multi-step sweeps)

### Stack-specific (Java / Spring Boot + Kafka + Redis + Postgres)
- Spring Boot, Spring Web (REST), Spring Data JPA
- Spring for Apache Kafka: `@KafkaListener`, `KafkaTemplate`, consumer groups, manual ack, partition keys
- Kafka core: topics, partitions, partition keys, offsets, consumer-group rebalancing
- Spring Data Redis (read model / cache)
- Kafka Streams *or* Apache Flink for event-time aggregation with watermarks (Kafka Streams is lower-friction inside a Spring app; Flink is closer to how banks actually do it)
- Resilience4j (circuit breaker, rate limiter) for the risk-health gate
- Postgres (event store + outbox table; JSONB for event payloads)
- Testcontainers (real Kafka/Postgres/Redis in integration tests — worth it for this domain)

---

## 3. Target architecture

Numbered boxes are the build order. Arrows are data flow.

```
                    +-----------------------------+
                    | 1. Feed simulator           |
                    |    fakes SWIFT feeds +       |
                    |    heartbeats                |
                    +--------------+--------------+
                                   | publishes
                                   v
                    +-----------------------------+
                    | Kafka                        |
                    |   topics partitioned by      |
                    |   currency                   |
                    +--------------+--------------+
                                   |
                                   v
                    +-----------------------------+
                    | 2. Aggregation service       |
                    |    event-time, watermarks    |
                    +--------------+--------------+
                                   | writes
                                   v
                    +-----------------------------+        +--------------------+
                    | 3. Ledger store              |------->| 6. Reconciliation   |
                    |    append-only, Postgres     | reads  |    EOD nostro match  |
                    +--------------+--------------+        +--------------------+
                                   | changes
                                   v
                    +-----------------------------+        +--------------------+
                    | 4. Position service          |------->| Dashboard           |
                    |    Redis read model,          | reads  |    live positions    |
                    |    stale flag                 |        +--------------------+
                    +--------------+--------------+
                                   | position updates
                                   v
  +------------------+   gate    +-----------------------------+
  | 7. Risk-health   |---------->| 5. Sweep engine              |
  |    FX peg gate    |          |    rules + idempotent outbox  |
  +------------------+          +--------------+--------------+
                                                | instruction
                                                v
                                 +-----------------------------+
                                 | 8. Wire gateway sim          |
                                 |    SWIFT gpi mock, UETR       |
                                 +-----------------------------+

  Legend:  [1–4] read/aggregation pipeline   [5,8] money-movement path
           [7] safety gate   [6] side consumer of the ledger
```

**Key structural decisions**

- **The ledger is a first-class component (box 3), not bundled inside the stream processor.** Both the position service and reconciliation read from it. It is the shared source of truth and what makes replay possible.
- **Reconciliation is a side consumer**, off the live path.
- **The dashboard reads the Redis model**, never the money path.
- **Risk-health feeds the sweep engine as a gate** that sits *before* any wire is sent.

**Repo layout suggestion** — a multi-module Gradle/Maven project: a shared `events` module (the event POJOs/records, shared enums like `Confidence`, `SweepState`), then one Spring Boot module per service. Start services as separate apps sharing the `events` module; a modular monolith is an acceptable simplification if the multi-app orchestration becomes a distraction — call it out if we take that path.

---

## 4. Step-by-step implementation pathway

The governing principle: **get a thin end-to-end slice running first** (a fake feed → a number that moves on a screen), *then* layer the hard correctness mechanisms on top. Don't perfect aggregation before a single position is visible.

### Step 0 — Infra scaffold
- `docker-compose` with Kafka, Postgres, Redis.
- Parent multi-module build; shared `events` module with event types as Java records: `TransactionEvent` (entity, currency, amount, eventTimestamp, correspondent, uetr), `HeartbeatEvent` (feedId, timestamp), and shared enums.
- Plain POJOs/records for now; graduate to Avro + Schema Registry only if we want that lesson later.
- **Checkpoint:** `docker-compose up` gives healthy Kafka/Postgres/Redis; the shared module compiles and is importable.

### Step 1 — Feed simulator service
- Spring Boot app; `@Scheduled` job publishing `TransactionEvent`s to a currency-keyed Kafka topic.
- Separate periodic `HeartbeatEvent` per feed.
- REST endpoints to inject specific scenarios on demand: a late event, a silent feed (stop heartbeats for feed X), a large inbound wire. **These endpoints are the test harness for the whole project — invest in them.**
- **Concepts to nail first:** partition keys (why currency), why event-timestamp is carried in the payload and distinct from publish time.
- **Checkpoint:** messages visible on the topic (console consumer); scenario-injection endpoints work.

### Step 2 — Aggregation service
- Consume the transaction topic. **Start trivially:** sum amounts per entity+currency and log. Prove the consumer works.
- **Then the hard part:** move to Kafka Streams with an event-time extractor and a windowed aggregation with a grace period for late arrivals.
- Emit `PositionUpdated` (or write straight to the ledger in Step 3).
- **Concepts to nail first:** event-time vs processing-time, watermarks, the grace-period trade-off (latency vs completeness), what happens to an event that arrives after the window closes.
- **Checkpoint:** inject a late event via Step 1's endpoint; show it lands in the correct window and adjusts the aggregate rather than being dropped or double-counted.

### Step 3 — Ledger store
- Postgres table, strictly **append-only**: one immutable row per position change — `id, entity, currency, delta, event_time, ingest_time, source, confidence (CONFIRMED|PROVISIONAL), source_event_id`.
- **Never UPDATE or DELETE.** Corrections are new rows. Enforce at the DB level (revoke UPDATE/DELETE, or a trigger that rejects them).
- Query to reconstruct a position *as of* any timestamp (needed by reconciliation and for replay).
- **Concepts to nail first:** why append-only, why event sourcing makes "what did we believe at time T" answerable, how corrections-as-appends differ from mutation.
- **Checkpoint:** write a sequence including a correction; reconstruct the position at two different timestamps and show the difference.

### Step 4 — Position service + dashboard
- Consume ledger changes; maintain a Redis read model: current position per entity+currency, plus `stale` flag and `lastUpdated`.
- REST: `GET /positions`, `GET /positions/{entity}`.
- Heartbeat consumer: flip positions to `stale=true` when a feed's heartbeat lapses beyond its SLA (a scheduled sweep over `lastHeartbeat` in Redis is the simplest implementation). SLA can be tighter for high-exposure feeds.
- Dashboard: trivial React or Thymeleaf page polling the REST endpoints — this is where the system becomes visible.
- **Concepts to nail first:** CQRS (why a separate read model), why staleness is a first-class attribute of a position, stale vs wrong vs zero.
- **Checkpoint:** trigger a silent feed via Step 1; watch the affected position flip to stale on the dashboard while others stay live.

### Step 5 — Sweep engine (the correctness centrepiece — go slow)
- Consume position updates; evaluate threshold rules (entity X, currency Y below buffer Z).
- On fire:
  1. Write a `SweepIntent` row to an **outbox table** in the **same DB transaction** as recording the decision — state `PENDING`, with a **deterministic idempotency key** from business fields (`entity + counterparty + currency + date + triggering_event_id`), never a random UUID.
  2. A separate outbox poller/relay reads `PENDING` intents and calls the wire gateway; transitions `PENDING → SENT → CONFIRMED|FAILED`.
  3. On restart, the poller re-reads `PENDING`/`SENT` intents and checks status **by UETR** rather than re-sending.
- **Exclude `stale` positions from triggering sweeps here.**
- **Concepts to nail first:** the dual-write problem and why the outbox solves it, why the idempotency key must be deterministic and business-derived, at-least-once delivery meeting idempotent effect.
- **Checkpoint:** kill the service between `SENT` and `CONFIRMED`; on restart prove it reconciles by UETR and does **not** double-fire.

### Step 6 — Reconciliation service
- Independent consumer of the ledger; **not** on the live path.
- On schedule or REST trigger: load a mock nostro statement (parse a canned MT940 file), reconstruct the ledger's position for that account/day, and match:
  1. exact (reference + amount + date),
  2. fuzzy (amount + date window) → timing differences,
  3. categorize the unmatched remainder (timing, missing, FX mismatch, booking error).
- Output a report listing each break and its category.
- **Concepts to nail first:** why reconciliation is only tractable because the ledger is append-only; hierarchical matching; break taxonomy.
- **Checkpoint:** use Step 1's late-event scenario to produce a realistic timing-difference break; show recon finds and categorizes it, then walk entity → currency → correspondent → transaction to the root.

### Step 7 — Risk-health service (the gate)
- Continuously compare a mock live FX rate against a configured peg/band per currency pair.
- On deviation past threshold: flip that pair's health to `UNHEALTHY`.
- Expose as a signal the sweep engine consults before every decision — a shared Redis flag, or a call wrapped in a **Resilience4j circuit breaker** so that if risk-health is unreachable, the breaker opens and the sweep engine **fails closed** (treats it as unhealthy).
- Wire into the sweep engine's decision gate from Step 5.
- **Concepts to nail first:** circuit breaker mechanics, fail-closed as the safe default for money movement, checking the gate on *every* decision (not just at startup).
- **Checkpoint:** trip a peg via config; show the next sweep for that pair halts and routes to manual, and that killing the risk-health service also halts sweeps (fail closed).

### Step 8 — Wire gateway sim
- Mock external SWIFT gpi: accept a sweep instruction, return a UETR immediately, then asynchronously transition `ACCEPTED → IN_PROGRESS → CONFIRMED`, with configurable delay/failure for testing.
- Status-by-UETR endpoint — exactly what the outbox poller calls on restart.
- **Concepts to nail first:** why the immediate UETR + async status model mirrors real gpi, and how it enables the recovery path in Step 5.
- **Checkpoint:** run a full sweep through to `CONFIRMED`; then force a failure and show the sweep engine handles it.

### Step 9 — Drive all four scenarios end-to-end
Use the Step 1 harness to reproduce each acceptance scenario and watch it resolve:
- silent feed → stale flag → sweep exclusion
- crash mid-sweep → no double fire
- peg break (and risk-health down) → sweep halts, fails closed
- late event → EOD reconciliation finds & categorizes the timing break
- **Checkpoint:** all four reproducible on demand from a clean start. This is the finish line — and the material for a writeup.

---

## 5. Sequencing rules & honest simplifications

- **Do 1–4 before 5–8.** The money path is the interesting part, but the read pipeline generates the data everything else reacts to. Build the money services before positions flow and you'll debug blind.
- **Kafka Streams before Flink.** Streams stays inside the Spring app with no separate cluster; it's the lower-friction way to learn event-time and watermarks. Flink is the more bank-realistic choice to graduate to later.
- **Plain append-only table before an event-store framework.** Start with a Postgres table and DB-enforced immutability; reach for a dedicated event-store only if we want that lesson.
- **Modular monolith is an acceptable fallback** to separate Spring Boot apps if multi-app orchestration becomes the bottleneck to learning — but keep the module boundaries clean so it can be split later.

Each of these is a deliberate simplification, not a shortcut past a concept — flag it as such in any writeup.

---

## 6. Definition of done

- All five failure scenarios in §1 are reproducible on demand from a clean `docker-compose up`.
- The ledger is append-only and enforces it at the DB level.
- The sweep engine cannot double-fire across a crash, proven by test.
- The sweep engine fails closed when risk-health is unhealthy *or* unreachable.
- Reconciliation categorizes a break rather than only reporting a delta.
- A newcomer can read this document and the code and understand *why* each mechanism exists, not just what it does.
