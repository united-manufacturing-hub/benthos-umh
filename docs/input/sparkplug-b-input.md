# Sparkplug B Input Plugin (Primary Host Role)

## Overview

The **Sparkplug B Input plugin** allows the United Manufacturing Hub (UMH) to ingest data from MQTT brokers using the Sparkplug B specification, acting in the **Primary Host** role. In practice, this plugin subscribes to Sparkplug B MQTT topics (e.g., device birth/data/death messages) and converts the incoming Protobuf payloads into UMH-compatible messages. It maintains the stateful context required by Sparkplug B – tracking device birth certificates, metric alias mapping, and sequence numbers – so that incoming data is interpreted correctly.

This input plugin is designed to seamlessly integrate Sparkplug-enabled edge devices into the UMH **Unified Namespace**. It automatically decodes Sparkplug messages and enriches them with metadata (such as metric names, types, and timestamps) to fit the UMH-Core data model. By default, each Sparkplug metric is emitted as an individual message into the pipeline, complete with a unified `umh_topic` and additional meta fields. This enables immediate use of downstream processors (like the Tag Processor) and outputs (like the `uns` output to Redpanda) without extra parsing logic.

## Use Cases

* **Integrating Legacy Sparkplug Devices:** Easily bring data from existing Sparkplug B clients (e.g., Ignition MQTT Engine devices, Sparkplug-enabled sensors or PLCs) into the UMH platform. The plugin bridges Sparkplug MQTT payloads into UMH's unified format, allowing legacy devices to feed into modern analytics or cloud systems.
* **Central SCADA/MES Data Ingestion:** Act as a Sparkplug **Primary Application** that listens for all device Birth (NBIRTH/DBIRTH), Data (NDATA/DDATA), and Death (NDEATH/DDEATH) messages on the shop floor. This is useful for SCADA, MES, or IIoT platforms built on UMH-Core to maintain a live data cache of all Sparkplug-enabled equipment.
* **Unified Namespace Population:** Sparkplug messages often carry hierarchical information (Group ID, Edge Node ID, Device ID, metric name). This plugin translates those into UMH **virtual paths** (`umh_topic`) so that Sparkplug data slots into the single source of truth (the Unified Namespace) alongside data from OPC UA, Modbus, etc. For example, a Sparkplug metric from *EdgeNode1* in group *FactoryA* can be mapped to a topic like `umh.v1.FactoryA.EdgeNode1._sparkplug.metricName` in the UMH namespace.
* **Stateful Device Monitoring:** Because the plugin internally tracks sequence numbers and birth certificates, it can be used to monitor device connectivity and session health. In a Sparkplug system, if a device goes offline or resets (causing unexpected sequence behavior or a new birth event), the plugin can detect this and handle it (e.g., by resetting its alias table and expecting a new NBIRTH), providing robust, state-aware data ingestion.

## Configuration Reference

The Sparkplug B Input plugin has several configuration options. Many are common MQTT connection settings; these are described here once and apply similarly to the Sparkplug B Output plugin as well:

* **`broker_address`** (string, required): The MQTT broker URI to connect to (e.g., `"tcp://localhost:1883"` for unencrypted, or `"ssl://broker.example.com:8883"` for TLS). This plugin must connect to the same broker where Sparkplug edge nodes are publishing.
* **`client_id`** (string, optional): MQTT Client identifier for this primary host. Must be unique on the broker. If not provided, a default like `"umh-sparkplug-primary"` is used. Typically, you can leave this default; use a custom ID if you need to identify the connection.
* **`username`** / **`password`** (string, optional): Credentials for broker authentication (if the MQTT broker requires login). Omit if not needed. Both or neither should be provided.
* **`tls` / certificate settings** (optional): Use these if connecting to an SSL/TLS-secured broker. You can specify:
  * **`ca_cert`**: Path to a CA certificate file if the broker uses a self-signed or private CA.
  * **`client_cert`** and **`client_key`**: Paths to the client's certificate and private key files, if client-side TLS auth is required.
* **`group_id`** (string, optional): If set, the plugin will subscribe **only** to that Sparkplug **Group ID**. By default, this plugin subscribes to all Sparkplug traffic (`spBv1.0/#`). Providing a group here (e.g., `"FactoryA"`) narrows the subscription to `spBv1.0/FactoryA/#`, filtering messages to just that group of devices. Use this in multi-tenant or multi-site scenarios to isolate data.
* **`data_only`** (boolean, optional): Default `false`. Controls whether initial Birth data should be emitted or suppressed. If `false`, when a device sends a Birth message (NBIRTH/DBIRTH), the plugin will output messages for each metric in that Birth payload (giving you the initial state of all metrics). If `true`, the plugin will **skip publishing metrics from birth messages**, and only output metrics from subsequent Data messages (NDATA/DDATA). Use `data_only: true` if you prefer to ignore the startup state and only react to live changing data (often useful to avoid a flood of initial values when many devices reconnect).
* **`client_group`** (string, optional, advanced): The MQTT consumer group name or client group used for subscription state (if applicable – not typically used in raw MQTT, mostly for Kafka-based connectors; Sparkplug B input uses standard MQTT subscribe semantics, so this may not be applicable).
* **`qos`** (integer, optional): Quality of Service level for MQTT subscription (0, 1, or 2). Default is 1 (at-least-once delivery) for reliability. QoS 0 (at-most-once) can be used if occasional data loss is acceptable, and QoS 2 typically isn't needed for telemetry.
* **`keep_alive`** (integer, optional): Keep-alive interval in seconds for MQTT. Default often 60. Usually you can use the broker's default; adjust if you need faster heartbeat or to detect dead connections quicker.

**Sparkplug-Specific Behavior:**

The input plugin does not require you to configure specific metric names or device IDs – it discovers and handles those from the incoming messages. However, it's important to understand how it interprets Sparkplug topics and payloads:

* **Topic Parsing to `umh_topic`:** The plugin automatically parses each incoming Sparkplug message's topic and constructs a corresponding `umh_topic` for the output message metadata. Sparkplug topics follow the format:

  ```
  spBv1.0/<GroupID>/<EdgeNodeID>/<MessageType>[/<DeviceID>]
  ```

  Using this information, the plugin will form a unified namespace topic. By convention, the Sparkplug `GroupID` and `EdgeNodeID` are mapped into the `location_path` within `umh_topic`. For example, if a message arrives on topic `spBv1.0/FactoryA/EdgeNode1/NDATA`, and it contains a metric named `Pressure`, the plugin might emit a message with metadata:

  * `msg.meta.location_path = "FactoryA.EdgeNode1"` (or a more expanded path if configured)
  * `msg.meta.data_contract = "_sparkplug"` (default contract for Sparkplug data unless overridden)
  * `msg.meta.tag_name = "Pressure"` (the metric's name)
    From these, it builds `msg.meta.umh_topic = "umh.v1.FactoryA.EdgeNode1._sparkplug.Pressure"`. This ensures the metric is placed correctly in the UMH namespace hierarchy. (You can adjust the naming convention by post-processing with a Tag Processor if needed, but by default the above scheme is used.)

* **Metric Names and Alias Decoding:** Sparkplug B uses **metric aliases** to reduce payload size – after a device's birth, subsequent data messages may omit metric names and use numeric aliases. The input plugin manages an **alias table** for each device (Edge Node and/or Device ID) internally. On receiving a Birth message, it maps each metric name to the provided alias. Later, when a Data message arrives, if a metric has no name but only an alias, the plugin looks up the alias to find the original name. The output Benthos message will always include the human-readable metric name (e.g., in the `umh_topic` and as `msg.meta.tag_name`) so you don't have to deal with alias numbers in your pipelines. This alias table is dynamically updated: if an unknown alias is encountered (or if a device sends a new Birth with a refreshed alias set), the plugin will log a warning or debug message and wait for a new Birth to properly map it.

  **Important Note on Alias Resolution:** The plugin only resolves aliases when metrics have an alias but **no name**. Many Sparkplug B devices send both the alias and the resolved name in DATA messages (which is perfectly valid per the specification). In such cases, you will see output like `{"alias": 50000, "name": "System Info/MAC Address", "value": "..."}` - this is **correct behavior**, not a bug. The alias field is preserved for debugging and traceability, while the name field provides the human-readable metric identifier. Only when a metric arrives with an alias but no name (e.g., `{"alias": 50000, "value": "..."}`) will the plugin attempt to resolve the alias from its internal cache.

* **Sequence Number Tracking:** Each Sparkplug message includes a sequence number (`seq`) and each Birth has a birth-death sequence (`bdSeq`). The input plugin performs **dynamic sequence validation** to ensure data consistency. It tracks the last sequence number seen from each Edge Node. If it detects a gap or a reset in the sequence (for example, if `seq` jumps backwards or skips unexpectedly), it interprets that as a potential session restart or message loss. In such cases, the plugin will internally mark the session as inconsistent – it may flush the old alias table for that device and expect a fresh NBIRTH or DBIRTH. This mechanism helps maintain a correct state: e.g., if a device went offline and came back (causing a new NBIRTH with `bdSeq` incremented), the plugin ensures old aliases/sequence are not misapplied. Sequence mismatches and resets are typically logged for visibility.

* **Metadata Enrichment:** In addition to `umh_topic`, the plugin attaches several Sparkplug-specific metadata fields to each output message:
  * `spb_group`: the Sparkplug Group ID of the source message.
  * `spb_edge_node`: the Edge Node ID (equipment or gateway name).
  * `spb_device` (if applicable): the Device ID (for metrics coming from a Device under an edge node; if the message was NDATA/NDIRTH at the node level, this may be empty or `null`).
  * `spb_seq`: the sequence number of the Sparkplug message that carried this metric.
  * `spb_bdseq`: the birth-death sequence number of the session (incremented each time the device rebirths).
  * `spb_timestamp`: the timestamp (in epoch ms) provided with the metric, if any.
  * `spb_datatype`: the Sparkplug data type of the metric (as a human-readable string, e.g. `"Int32"`, `"Double"`, `"Boolean"`, etc., derived from the metric's data type ID).
  * `spb_alias`: the alias number of the metric (as used in the Sparkplug payload). This is primarily for debugging or advanced use; as noted, the plugin already resolves alias to name for normal operations.
  * `spb_is_historical`: set to `true` if the metric was flagged as historical in the Sparkplug payload (Sparkplug metrics carry an `is_historical` flag to indicate back-filled data vs real-time).
    These metadata fields ensure that all relevant Sparkplug context is preserved. You can use them in processors or for routing if needed. For example, one could route messages differently if `spb_is_historical = true` to handle backfilled data separately.

## Example Configs

**Basic Example (Single Group):** Subscribe to all metrics from a known Sparkplug group and output them to the unified namespace.

```yaml
input:
  sparkplug_b:
    broker_address: "tcp://iot-broker.local:1883"
    group_id: "FactoryA"            # listen only to this Sparkplug Group
    client_id: "UMH_PrimaryApp_1"   # optional client ID
    username: "mqtt_user"           # if broker requires auth
    password: "mqtt_pass"
    data_only: false                # include initial birth metrics
pipeline:
  processors:
    - tag_processor: {}             # (Optional) further enrich or remap tags if needed
output:
  uns: {}                           # send to Unified Namespace (Redpanda)
```

In this example, the Sparkplug Input will connect to the MQTT broker at `iot-broker.local` and subscribe to `spBv1.0/FactoryA/#`. It will receive all Sparkplug messages for group "FactoryA". If a device EdgeNode1 in FactoryA publishes an NBIRTH with metrics `Pressure` and `Temperature`, the plugin will output each metric as a separate message. For instance, `Pressure` might come out as a message with `umh_topic = umh.v1.FactoryA.EdgeNode1._sparkplug.Pressure` and its current value in the payload. The next `Temperature` metric appears similarly. Subsequent NDATA messages containing changes in `Pressure` or `Temperature` will result in output messages with those tags and updated values. The initial birth metrics are emitted (`data_only: false`), so even if no change occurs after birth, you get the starting values. Each message carries metadata like `spb_group="FactoryA"`, `spb_edge_node="EdgeNode1"`, etc., which the Tag Processor (if used) can further transform or simply pass along. Finally, the `uns` output plugin will write these into the internal Kafka-based unified namespace.

**Filtering & Advanced Options:** If you have a setup with many Sparkplug groups or devices and only want to consume a subset, you can adjust the plugin accordingly:

* To subscribe to **multiple groups**, you currently need multiple input instances (one per group) or use a wildcard in `group_id` (Sparkplug doesn't typically use wildcards in the middle of topic; better to list specific groups or all).
* If you set `data_only: true`, the behavior changes – using the above example, the NBIRTH of EdgeNode1 will **not** immediately produce output messages for `Pressure` and `Temperature`. Only when the device sends new NDATA or DDATA messages will the plugin emit those metrics. The alias table is still built on NBIRTH in the background, but the initial values are suppressed. This is useful if you only care about changes or live data.

**TLS Connection Example:** Connecting securely with TLS (e.g., MQTT over SSL):

```yaml
input:
  sparkplug_b:
    broker_address: "ssl://broker.company.com:8883"
    client_id: "UMH_Primary_TLS"
    username: "company_user"
    password: "secret"
    tls:
      ca_cert: "/certs/ca.crt"
      client_cert: "/certs/client.crt"
      client_key: "/certs/client.key"
    group_id: "Plant01"
    data_only: false
```

This configuration will connect to the broker using the provided CA and client certificates, subscribe to `Plant01` group topics, and ingest all Sparkplug data securely.

**Complete UMH Integration Example with Tag Processor:** For full UMH integration with proper asset hierarchy mapping:

```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://broker.hivemq.com:1883"]
      client_id: "benthos-umh-primary-host"
      qos: 1
      keep_alive: "60s"
      connect_timeout: "30s"
      clean_session: true
    
    identity:
      group_id: "UMH-Group"
      edge_node_id: "PrimaryHost"
    
    role: "primary_host"
    
    subscription:
      groups: []  # Listen to all groups
    
    behaviour:
      auto_split_metrics: true        # Each metric becomes separate message
      data_messages_only: false       # Process BIRTH, DATA, and DEATH messages
      drop_birth_messages: false      # Keep BIRTH messages for alias resolution
      auto_extract_values: true       # Extract values from protobuf
      include_node_metrics: true      # Include node-level metrics
      include_device_metrics: true    # Include device-level metrics

pipeline:
  processors:
    # Add timestamp and basic metadata
    - mapping: |
        root = this
        root.received_at = now()
        
    # Process through tag_processor for UMH format conversion
    - tag_processor:
        # UMH Asset Hierarchy Configuration
        asset_hierarchy:
          # Map Sparkplug group_id to UMH enterprise
          enterprise: this.group_id | "sparkplug"
          
          # Map edge_node_id to UMH site  
          site: this.edge_node_id | "unknown_site"
          
          # Map device_key components to UMH area/line
          area: |
            # Extract area from device_key (group/node/device -> use device as area)
            if this.device_key != null {
              this.device_key.split("/").index(2) | "production"
            } else {
              "production" 
            }
          
          # Use tag_name as work_cell
          work_cell: this.tag_name | "default"
        
        # Tag Name Processing
        tag_name_processing:
          # Use the resolved metric name or alias as tag name
          tag_name_source: |
            if this.name != null && this.name != "" {
              this.name
            } else if this.alias != null {
              "alias_" + string(this.alias)
            } else {
              "unknown_metric"
            }
          
          # Clean tag names for UMH compatibility
          tag_name_transformations:
            - type: "replace"
              pattern: "[^a-zA-Z0-9_/-]"
              replacement: "_"
            - type: "lowercase"
        
        # Value Processing
        value_processing:
          # Handle different Sparkplug data types
          value_transformations:
            - condition: 'this.value == null'
              action: 'set_null'
            - condition: 'type(this.value) == "bool"'
              action: 'convert_bool_to_int'
            - condition: 'type(this.value) == "string"'
              action: 'keep_string'
        
        # Metadata Enrichment
        metadata_enrichment:
          add_sparkplug_metadata: true
          add_timestamp_metadata: true
          custom_metadata:
            data_source: "sparkplug_b"
            message_type: this.spb_message_type | "unknown"
            device_online: this.spb_device_online | false

output:
  # Output to UMH Unified Namespace
  uns: {}
```

This example shows a complete Sparkplug B to UMH integration pipeline. The `tag_processor` transforms the Sparkplug data into UMH's asset hierarchy format, mapping Sparkplug Group IDs to enterprises, Edge Node IDs to sites, and properly handling metric names whether they come from alias resolution or direct names. The final output goes to the UMH Unified Namespace where it can be consumed by other UMH components.

## Notes

* **Sparkplug Version & Compatibility:** This plugin targets **Sparkplug B** (v3.0) compliance. It uses the official Sparkplug Protobuf definitions for decoding. Ensure your edge devices are publishing Sparkplug B formatted payloads (not Sparkplug A or custom formats). The plugin currently focuses on metrics (Data messages). Sparkplug command messages (NCMD/DCMD) are not generated by this input plugin – since it's an input, it only listens; if you need to respond with NCMD/DCMD, you would use the Sparkplug Output plugin or another method to publish commands.
* **UMH-Core Integration:** In a UMH-Core deployment, minimal configuration is needed – often just `sparkplug_b: { broker_address: "...", group_id: "..." }` is enough, as other defaults are optimized for UMH. The **Tag Processor** can be used in-line to reshape any metadata. For example, if you want to map Sparkplug's `EdgeNode1` to a more descriptive location in UMH, you could use a Tag Processor to prepend a higher-level path or change the data contract. The plugin's output `umh_topic` is meant to be readily consumable by the `uns` output (as shown in examples) so that Sparkplug data flows into the unified namespace with no extra fuss.
* **Performance:** The Sparkplug Input processes messages in a streaming fashion. Under the hood, it uses asynchronous MQTT subscriptions. Incoming Sparkplug payloads are decoded and then each metric is emitted as a separate Benthos message. This ensures backpressure is handled correctly (the plugin won't ACK the MQTT message until all output Benthos messages derived from it are processed). The plugin batches metrics from a single Sparkplug payload efficiently. If an NBIRTH carries 100 metrics, the plugin will emit 100 messages in a batch, which Benthos can process concurrently. This design prevents bottlenecks when devices publish large births.
* **Sequence & Session Handling:** The plugin's dynamic sequence validation is there to help identify missed data or device restarts. **Note:** It does not currently re-order out-of-order messages (that is generally the broker's job with QoS). However, if a gap is detected (say sequence jumped from 10 to 13), it will log the event. If a device reconnects and issues a new Birth (`bdSeq` increments), the plugin resets that device's context (alias table, last seq) and treats subsequent messages under the new session. All of this happens internally; from the user perspective, you'll just see a new stream of birth metrics followed by data metrics. If you see duplicate initial values, that could be because a device re-birthed and `data_only` was false (thus you got a second set of NBIRTH metrics).
* **Limitations & Future Improvements:** This initial version assumes that all metrics in Sparkplug payloads can be mapped to simple tag values in UMH. Complex Sparkplug features like **Templates** and **DataSets** (which bundle multiple metrics in a single metric data structure) are not fully expanded by the plugin yet – they will appear as a single payload blob if encountered. Support for these complex types may be added later. Additionally, if a Sparkplug device includes properties or metadata in metrics, those are not separately exposed except as raw JSON in the description (if provided). The focus is on core telemetry metrics.
* **Common MQTT Issues:** If the plugin doesn't seem to be receiving data:
  * Double-check the `broker_address` and credentials.
  * Ensure the `group_id` is correct (it's case-sensitive and must match exactly what the devices use).
  * Verify that the MQTT broker is reachable from the UMH instance and that any firewall or networking is set up to allow the connection.
  * If using TLS, make sure certificates are valid and the time is synchronized (certificate expiration or not yet valid issues can silently block connections).
  * Use the UMH logging output to see debug logs; the Sparkplug input will log when it connects, subscribes, and whenever it receives a Birth or Death (at info level), as well as any sequence warnings (at warn level).
* **Shutdown Behavior:** When Benthos (or UMH-Core) is shut down gracefully, this input will unsubscribe from the broker. Sparkplug Primary Host clients typically do not send any "death" announcement – that concept applies to Edge Nodes. Therefore, there is no special NDEATH sent by this plugin on exit (that's only for the output plugin acting as an Edge Node). However, the MQTT broker will simply register the subscription gone. If you restart the UMH primary application, be aware that Sparkplug edge nodes will **not** automatically resend their NBIRTHs unless they detect a disconnect of their own. You may need to design your system such that edge devices periodically do a rebirth or at least keep publishing data so the primary can rebuild state after a restart. Alternatively, consider configuring the Sparkplug B devices to use MQTT **Retained** messages for NBIRTH – this plugin does support reading retained NBIRTH on connect (the broker will send it as if it just occurred), allowing it to populate initial metric state even if it starts after the devices. Retained messages are optional in Sparkplug, so use according to your system's approach. 