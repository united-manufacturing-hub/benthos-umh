# UNS Beta (Input)

> **Experimental preview.** `uns_beta` is the reliable successor to the `uns`
> input and will replace it once it has proven out. Until then, `uns` stays the
> default.
>
> **Works exclusively with UMH Core.** It consumes from the embedded Redpanda.

`uns_beta` is an at-least-once consumer of the Unified Namespace. Each message
stays in-flight until your pipeline acknowledges it. If a processor or output
rejects a message, `uns_beta` redelivers it until it succeeds and only then
advances the committed offset, so a transient downstream failure never costs you
a message.

There is no retry limit and no dead-letter queue yet. A message that your
pipeline rejects every time, for example one that a processor always fails on,
is redelivered forever and holds up every later message on its partition.

## 1 Quick-start

Two fields are required: `consumer_group` names the consumer, and `umh_topics`
declares which topics it wants.

```yaml
input:
  uns_beta:
    consumer_group: "analytics_reader"   # unique to this consumer
    umh_topics:                          # the topics this consumer subscribes to
      - "umh\\.v1\\.acme\\.berlin\\..+"
      - "umh\\.v1\\.acme\\.munich\\..+"
pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.demo.plant1.line1.plc1";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = "value";
          return msg;
output:
  uns: {}
```

To consume the whole namespace, set `umh_topics: [".*"]`. There is no implicit
"everything" default, so a consumer always states what it wants. Inside UMH Core,
`broker_address` defaults to the embedded Redpanda (`localhost:9092`); set it only
to consume from an external broker.

| What | Default |
|------|---------|
| **Consumer group** | none (required; must be unique) |
| **Topic filter** (`umh_topics`) | none (required); use `[".*"]` for the whole namespace |
| **Broker address** | the embedded Redpanda (`localhost:9092`) |
| **Kafka topic** | `umh.messages` |

## 2 Configuration

| Field | Purpose & Default |
|-------|-------------------|
| `consumer_group` | Consumer-group ID used for offset tracking. **Required, and must be unique to this consumer.** Inside UMH Core the UNS topic has one partition, so two flows sharing a group do not share the work: one receives everything and the other receives nothing. |
| `broker_address` | Comma-separated Kafka bootstrap list. Default `localhost:9092`, the embedded Redpanda inside UMH Core. Override only to consume from an external broker. |
| `kafka_topic` | Physical Kafka topic to consume. Default `umh.messages`. |
| `umh_topics` | List of RE2 regex patterns matched against the Kafka key (the UMH topic). **Required.** A record is delivered only if its key matches at least one pattern. Patterns are unanchored; use `^...$` to match the full key. To consume the whole namespace, set `[".*"]`. |

`uns_beta` exposes **only** `umh_topics`. The single-pattern `umh_topic` and the
older `topic` field from `uns` are not available here. Use `umh_topics` with one
or more patterns instead.

## 3 What the plugin does behind the scenes

1. **Consume** `umh.messages` through the official Redpanda Connect engine,
   starting from the earliest offset on first connect.

2. **Filter** records by `umh_topics`, matched against the Kafka key (the UMH
   topic). A record whose key matches none of the patterns is dropped before a
   message is built for it. A record with an empty or absent key never matches
   any pattern.

3. **Set metadata** on each delivered message. The fields you will typically
   use:
   - `umh_topic` - the UMH topic structure from the Kafka key (e.g. `umh.v1.enterprise.plant1._historian.temperature`)
   - `kafka_msg_key` - the original Kafka key (same value as `umh_topic`)
   - `kafka_timestamp_ms` - when the record was written to Kafka
   - `kafka_topic` - the physical Kafka topic (e.g. `umh.messages`)
   - each Kafka header as `meta(...)` for downstream processors

   Because it delegates to the Redpanda Connect engine, `uns_beta` also carries
   that engine's standard Kafka metadata, such as `kafka_partition` and
   `kafka_offset`.

4. **Commit through the engine.** Offsets are committed by the Redpanda Connect
   engine only after the whole batch is acknowledged. This is what gives the
   in-flight-until-accepted behavior described at the top.

**Performance notes**
- `uns_beta` is built for selective consumers: when you subscribe to a small
  part of a busy namespace, the records you don't want are dropped before a
  message is built for them, so they cost little memory.
- A complex regex can still cost CPU. Prefer anchored, specific patterns such as
  `umh\\.v1\\.acme\\.(berlin|munich)\\..+` over `.*temperature.*` when throughput
  matters.

## 4 Migrating from `uns`

- Replace `umh_topic: "<pattern>"` (or `topic:`) with `umh_topics: ["<pattern>"]`.
  `umh_topics` is required: `uns` defaulted to `.*` (the whole namespace),
  so to keep that behavior set `umh_topics: [".*"]` explicitly.
- Set a `consumer_group`. Unlike `uns`, `uns_beta` does not default it.
- Records with an empty or absent Kafka key are no longer delivered. `uns`
  passed them through under its default `.*` pattern; `uns_beta` drops them,
  because an empty key produces an empty `umh_topic` that the `uns` output would
  reject forever.

## 5 Delivery guarantees

**Delivery and acknowledgement.** At-least-once. A message stays in flight until your pipeline
acknowledges it; a rejected message is redelivered until it is accepted, and the committed
offset never moves past it. Offsets are committed by the Redpanda Connect engine every 5
seconds, and only after the whole batch has been acknowledged.

Because the unit of acknowledgement is a whole Kafka fetch, two things follow that your pipeline
must tolerate:

- **Duplicates can be a whole fetch.** If UMH Core stops between your output accepting a message
  and the next commit, every message in that fetch is delivered again. In testing this was up to
  200 messages at once. Your output must be idempotent: for time-series data the topic and the
  timestamp are the key, so rewriting the same value is harmless.
- **One rejected message replays its batch.** Messages the output already accepted are sent
  again on every retry, about once a second, for as long as the failure lasts. One rejected
  message among 500 produced roughly 36 delivery attempts per message over an 80-second outage.
  Nothing is lost; the cost is duplicate writes at the system you forward to.

**Order.** Within one delivery pass, messages for a given topic arrive in the order they were
written. A redelivery restarts at the beginning of its batch, so a consumer that tracks the
stream sees it go backwards and repeat. The last delivery of each topic is in order, so the
value that ends up in the UNS is the right one — but a processor that accumulates state, such as
a counter or a projection, must tolerate the rewind as well as the duplicates.

**State.** The plugin keeps none. The read position lives in the Kafka consumer group and
survives a restart. A new `consumer_group` always starts at the oldest available message, and
there is no way to start from "now": a new flow on a busy topic reads the whole history first.
On a compacted topic, compaction removes older values in the background, so the history holds
the latest value per topic and possibly some older ones. Do not treat it as a snapshot of the
current state.

**Time.** `kafka_timestamp_ms` is the Kafka record timestamp, not when the measurement was
taken. Under the topic's default `CreateTime` the producer sets it when it sends the record;
under `LogAppendTime` it is when the broker stored the record. Event time, if you need it, is
whatever your payload carries. The plugin does not read, set or reorder by event time.

**Errors.** There is no retry limit and no dead-letter queue. A message your pipeline rejects
every time is redelivered forever and holds up every later message on its partition. That
includes a message whose Kafka key is not a valid UMH topic: this input accepts any key matching
your `umh_topics` patterns, while the `uns` output accepts only the UMH topic alphabet
(`a-z A-Z 0-9 . _ -`), so a key outside it from a third-party producer will stall the flow until
someone intervenes. Watch consumer-group lag to detect this.

**Limits.** Inside UMH Core the UNS topic has exactly one partition, so two flows sharing a
`consumer_group` do not share the work — one receives everything and the other receives nothing.
A producer that sets a Kafka header literally named `kafka_key` overrides the record key, so a
message can be delivered, and written back, under a different topic; keep `umh_topics` as narrow
as you can, since a spoofed topic still has to match your patterns. Suited to forwarding UNS data
to systems that tolerate duplicates. Not suited, yet, to a flow that must give up on a message it
cannot process.

## 6 Metrics

`uns_beta` adds three counters to the standard Redpanda Connect input metrics.
Each counts records it drops. A counter that has not counted anything yet does
not appear on the metrics endpoint at all, so an absent counter means zero:

- `key_omitted_records` - a record whose key matched none of your `umh_topics`
  patterns, dropped before a message is built for it. If it counts every
  record and nothing is delivered, check your patterns.
- `dropped_keyless` - a record with an empty or absent Kafka key that your
  patterns would otherwise accept (for example under `.*`). Under narrower
  patterns, keyless records are counted in `key_omitted_records` instead. A
  non-zero value means a producer is writing keyless records to the topic.
- `filtered_records` - a record whose Kafka key matched your patterns, but whose
  `kafka_key` metadata did not. This happens when a producer sets a Kafka header
  named `kafka_key`, which overrides the record key in the metadata. Remove that
  header from the producer.

The counters appear under these bare names, carrying the `path` and `label` labels that
identify the emitting component, in whatever metrics exporter you configure. Delivered records
increment none of them.

If you are moving a flow from `uns`, note that none of the `input_uns_*` metrics are emitted any
more: the names above replace them, and they are not the same measurements. In particular
`uns_beta` reports no `input_received` for a record it filters out, because it never builds a
message for one — so while a flow works through a backlog that matches none of your patterns,
`key_omitted_records` is the only number that moves.

For the standard consumer metrics (throughput, latency, connection state, and
consumer-group lag), see the Redpanda Connect
[redpanda input](https://docs.redpanda.com/redpanda-connect/components/inputs/redpanda)
and [metrics](https://docs.redpanda.com/redpanda-connect/components/metrics/about)
documentation.

## 7 FAQs / Troubleshooting

- **"No messages appear":** your `umh_topics` patterns filtered everything out.
  Start with `[".*"]`, confirm the flow, then tighten. The drop counters under
  Metrics tell you which records are being dropped and why.

- **"Consumer-group lag grows forever":** your pipeline rejects a message every
  time it is delivered, or never acknowledges it. `uns_beta` commits only on a
  successful ack and redelivers on a nack, so check downstream processors and
  outputs for errors.
