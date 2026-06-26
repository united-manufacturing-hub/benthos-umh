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
a message. This is the same retry-until-accepted behavior you rely on in write
flows.

#### 1 Quick-start

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

#### 2 Configuration

| Field | Purpose & Default |
|-------|-------------------|
| `consumer_group` | Consumer-group ID used for offset tracking. **Required, and must be unique to this consumer.** Two consumers sharing a group split the partitions between them. |
| `broker_address` | Comma-separated Kafka bootstrap list. Default `localhost:9092`, the embedded Redpanda inside UMH Core. Override only to consume from an external broker. |
| `kafka_topic` | Physical Kafka topic to consume. Default `umh.messages`. |
| `umh_topics` | List of RE2 regex patterns matched against the Kafka key (the UMH topic). **Required.** A record is delivered only if its key matches at least one pattern. Patterns are unanchored; use `^...$` to match the full key. To consume the whole namespace, set `[".*"]`. |

`uns_beta` exposes **only** `umh_topics`. The single-pattern `umh_topic` and the
older `topic` field from `uns` are not available here. Use `umh_topics` with one
or more patterns instead.

#### 3 What the plugin does behind the scenes

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
- `uns_beta` is built for selective consumers: when you subscribe to a subset of
  a high-volume namespace, non-matching records are discarded before
  construction, so memory churn stays low regardless of firehose volume.
- A complex regex can still cost CPU. Prefer anchored, specific patterns such as
  `umh\\.v1\\.acme\\.(berlin|munich)\\..+` over `.*temperature.*` when throughput
  matters.

#### 4 Migrating from `uns`

- Replace `umh_topic: "<pattern>"` (or `topic:`) with `umh_topics: ["<pattern>"]`.
  `umh_topics` is required: legacy `uns` defaulted to `.*` (the whole namespace),
  so to keep that behavior set `umh_topics: [".*"]` explicitly.
- Set a `consumer_group`. Unlike `uns`, `uns_beta` does not default it.
- Records with an empty or absent Kafka key are no longer delivered. Legacy `uns`
  passed them through under its default `.*` pattern; `uns_beta` drops them,
  because an empty key produces an empty `umh_topic` that the `uns` output would
  reject forever.

#### 5 Metrics

`uns_beta` adds three always-on counters to the standard Redpanda Connect input
metrics. Each counts a class of record it drops, so you can tell which one is
firing:

- `filtered_records` - a keyed record whose key matched none of your `umh_topics`
  patterns. Climbing while `input_received` stays flat means your patterns are
  too narrow.
- `dropped_keyless` - a record with an empty or absent Kafka key. A non-zero
  value means a producer is writing keyless records to the topic.
- `dropped_spoofed_key` - a record carrying a Kafka header literally named
  `kafka_key`, which shadows the real record key. `uns_beta` cannot recover the
  real key, so it drops the record. A non-zero value points to a misconfigured
  producer; remove the `kafka_key` header from it.

The counters appear under these bare names (carrying `path` / `label` / `stream`
labels that identify the emitting component) in whatever metrics exporter you
configure. Delivered records increment none of them.

For the standard consumer metrics (throughput, latency, connection state, and
consumer-group lag), see the Redpanda Connect
[redpanda input](https://docs.redpanda.com/redpanda-connect/components/inputs/redpanda)
and [metrics](https://docs.redpanda.com/redpanda-connect/components/metrics/about)
documentation.

#### 6 FAQs / Troubleshooting

- **"No messages appear":** your `umh_topics` patterns filtered everything out.
  Start with `[".*"]`, confirm the flow, then tighten. The drop counters under
  Metrics tell you which records are being dropped and why.

- **"Consumer-group lag grows forever":** your pipeline never acknowledges.
  `uns_beta` commits only on a successful ack and redelivers on a nack, so check
  downstream processors and outputs for errors.
