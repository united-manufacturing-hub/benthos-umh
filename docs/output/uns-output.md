# UNS (Output)

> **Works exclusively with UMH Core**

## 1 Quick-start (99 % of users)

```yaml
pipeline:
  processors:
    - tag_processor:            # or Bloblang / Node-RED JS
        defaults: |
          // Minimal example
          msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = "value";
          // tag_processor now auto-creates msg.meta.umh_topic
          return msg;

output:
  uns: {}                       # nothing else needed on UMH Core. Will automatically use msg.meta.umh_topic from the tag_processor and write to the internal redpanda.
```

*Open the **Topic Browser** (Management Console → Unified Namespace) to watch the live values.*

## 2 - Optional overrides

```yaml
output:
  uns:
    umh_topic:      "${! meta(\"umh_topic\") }"   # Must follow `umh.v1.<…>` naming. If not specified, will take the `umh_topic` from the metadata fields (e.g., from msg.meta.umh_topic)
    bridged_by:     "umh-core"                    # Traceability header. Default `umh-core`; overridden automatically by protocol-converters inside UMH Core.
```

## 3 - What the plugin does behind the scenes

1. **Batching** 100 messages *or* 100 ms – whichever comes first.
2. **Sanitising** Illegal chars in the key become "\_".
3. **Kafka/Redpanda** Each message will be stored in the Kafka topic `umh.messages` with the Kafka key of the umh_topic
4. **Headers** All Benthos metadata (except `kafka_*`) plus `bridged_by` are forwarded as Kafka headers.
5. **Topic check** If `umh.messages` is missing the plugin creates it (1 partition, `compact,delete`).

## Troubleshooting / FAQs

* **"topic is not set or is empty"** – your pipeline never wrote `msg.meta.umh_topic`.
  Add a `tag_processor` (auto) or a Bloblang line:
  `meta umh_topic = "umh.v1.demo.plant1.line1._historian.temperature"`
* **I am not using umh-core, but I still want to use the uns output plugin. How can I do that?** - there is a configuration variable called `broker_address` which you can point to any redpanda broker. 