## Using it with UMH

Use the `mqtt` input in a [bridge](https://docs.umh.app/usage/data-flows/bridges) to read from an existing MQTT broker, such as one that IoT sensors publish to, and write the values into the Unified Namespace. The input sets the `mqtt_topic` metadata field on each message, so the [Tag Processor](../../processing/tag-processor.md) can name each tag from the topic it arrived on.

This bridge reads temperature and humidity readings and writes each one as a `_historian` tag:

```yaml
input:
  mqtt:
    urls:
      - tcp://10.0.0.20:1883
    topics:
      - ia/raw/development/ioTSensors/Temperature
      - ia/raw/development/ioTSensors/Humidity
    client_id: umh-bridge-sensors
pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "{{ .location_path }}";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name = msg.meta.mqtt_topic.split("/").pop().toLowerCase();
          return msg;
```

Give every bridge its own `client_id`. Two clients with the same ID disconnect each other, and the bridge then reconnects in a loop.
