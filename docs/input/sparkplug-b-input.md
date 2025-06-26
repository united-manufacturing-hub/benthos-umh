# Sparkplug B Input Plugin

## Overview

The **Sparkplug B Input plugin** allows the United Manufacturing Hub (UMH) to ingest data from MQTT brokers using the Sparkplug B specification. It subscribes to Sparkplug B MQTT topics (e.g., device birth/data/death messages) and converts the incoming Protobuf payloads into UMH-compatible messages. It maintains the stateful context required by Sparkplug B – tracking device birth certificates, metric alias mapping, and sequence numbers – so that incoming data is interpreted correctly.

This input plugin is designed to seamlessly integrate Sparkplug-enabled edge devices into the UMH **Unified Namespace**. It automatically decodes Sparkplug messages and enriches them with metadata (such as metric names, types, and timestamps) to fit the UMH-Core data model. By default, each Sparkplug metric is emitted as an individual message into the pipeline, complete with a unified `umh_topic` and additional meta fields.

## Quick Start

Most users should use this simple configuration to read Sparkplug B data:

```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
    identity:
      group_id: "DeviceLevelTest"
    # role: "host" is default - no configuration needed
    subscription:
      groups: []  # Empty = subscribe to all groups

processing:
  processors:
    - tag_processor:
        defaults: |
          # msg.meta.location_path = "..."; # automatic from the device_id (see also output plugin)
          # msg.meta.virtual_path = "..."; # automatic from the metric name (see also output plugin)  
          # msg.meta.tag_name = "..."; # automatic from the metric name (see also output plugin)

          msg.meta.data_contract = "_sparkplug";  # the target data contract

output:
  uns: {}
```

This configuration safely reads all Sparkplug B messages and converts them to UMH-Core format. Multiple instances can run simultaneously without conflicts.

### Sparkplug B to UMH-Core Mapping

Here's how a Sparkplug B message maps to UMH-Core:

**Input Sparkplug B Message:**
- **Topic**: `spBv1.0/FactoryA/DDATA/EdgeNode1/enterprise:factory:line1:station1`
- **Metric Name**: `sensors:ambient:temperature` 
- **Payload**: Protobuf with metric alias, value 23.5, timestamp

**↓ Results in UMH-Core Message:**

**Payload:**
```json
{
  "value": 23.5,
  "timestamp_ms": 1672531200000
}
```

**Metadata:**
```json
{
  "location_path": "FactoryA.EdgeNode1.enterprise:factory:line1:station1",
  "virtual_path": "sensors.ambient",
  "tag_name": "temperature",
  "data_contract": "_sparkplug"
}
```

**Key Transformations:**
1. **Topic Structure**: `spBv1.0/FactoryA/DDATA/EdgeNode1/enterprise:factory:line1:station1` → `location_path: "FactoryA.EdgeNode1.enterprise:factory:line1:station1"` (colons → dots)
2. **Metric Name**: `sensors:ambient:temperature` → `virtual_path: "sensors.ambient"` + `tag_name: "temperature"` (colons → dots)
3. **Sparkplug Protobuf**: Metric value and timestamp → UMH-Core format `{"value": X, "timestamp_ms": Y}`
4. **Data Contract**: Automatically set by tag_processor to `"_sparkplug"`

## Configuration Reference

### MQTT Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mqtt.urls` | `[]string` | **required** | List of MQTT broker URLs |
| `mqtt.client_id` | `string` | `"benthos-sparkplug"` | MQTT client identifier |
| `mqtt.credentials.username` | `string` | `""` | MQTT username |
| `mqtt.credentials.password` | `string` | `""` | MQTT password |
| `mqtt.qos` | `int` | `1` | MQTT QoS level |
| `mqtt.keep_alive` | `duration` | `"30s"` | MQTT keep alive interval |
| `mqtt.connect_timeout` | `duration` | `"10s"` | Connection timeout |
| `mqtt.clean_session` | `bool` | `true` | MQTT clean session flag |

### Identity Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `identity.group_id` | `string` | **required** | Sparkplug B Group ID |
| `identity.edge_node_id` | `string` | `""` | Optional: For advanced Primary Host configuration only |

### Subscription Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `subscription.groups` | `[]string` | `[]` | Groups to subscribe to (empty = all groups) |

### Behavior Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `behaviour.data_only` | `bool` | `false` | Skip Birth messages, only process Data messages |

---

## Technical Details

### Host Roles and STATE Messages

For advanced users who need to understand the different host roles:

#### Secondary Host (Default - Recommended)
- **Role**: `"host"` (default, no configuration needed)
- **Behavior**: Read-only, no STATE message publishing
- **Use Case**: Safe for brownfield deployments, multiple instances can run simultaneously
- **Requirements**: None (edge_node_id is optional)

#### Primary Host (Advanced Use Only)
- **Role**: `"primary"` (explicit configuration required)
- **Behavior**: Publishes STATE Birth/Death messages for host arbitration
- **Use Case**: SCADA/HMI applications that need to coordinate with Edge Nodes
- **Requirements**: `edge_node_id` is required (used as `host_id`)

**Advanced Primary Host Configuration:**
```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
    identity:
      group_id: "DeviceLevelTest"
      edge_node_id: "PrimaryHost"  # Required: used as host_id for STATE topic
    role: "primary"  # Primary Host: publishes STATE, tracks sequences
    subscription:
      groups: ["FactoryA", "TestGroup"]  # Specific groups or empty for all
```

**Primary Host STATE Topic (Sparkplug v3.0):**
```
spBv1.0/STATE/<host_id>
```

The Primary Host uses the `edge_node_id` configuration field as the `host_id` for STATE messages. For example:
- Configuration: `edge_node_id: "PrimaryHost"`  
- STATE topic: `spBv1.0/STATE/PrimaryHost`

**Important**: Primary Host STATE topics do NOT include the `group_id` (per Sparkplug B v3.0 specification). This allows Edge Nodes across all groups to detect the Primary Host for proper session management.

### Metadata Enrichment

In addition to `umh_topic`, the plugin attaches several Sparkplug-specific metadata fields to each output message:

* `spb_group`: the Sparkplug Group ID of the source message
* `spb_edge_node`: the Edge Node ID (equipment or gateway name)
* `spb_device` (if applicable): the Device ID (for metrics coming from a Device under an edge node)
* `spb_seq`: the sequence number of the Sparkplug message that carried this metric
* `spb_bdseq`: the birth-death sequence number of the session
* `spb_timestamp`: the timestamp (in epoch ms) provided with the metric, if any
* `spb_datatype`: the Sparkplug data type of the metric (e.g. `"Int32"`, `"Double"`, `"Boolean"`)
* `spb_alias`: the alias number of the metric (primarily for debugging)
* `spb_is_historical`: set to `true` if the metric was flagged as historical in the Sparkplug payload

These metadata fields ensure that all relevant Sparkplug context is preserved. You can use them in processors or for routing if needed.