# Sparkplug B (Output)

## Overview

The **Sparkplug B Output plugin** allows the United Manufacturing Hub (UMH) to publish industrial IoT data to MQTT brokers using the Sparkplug B specification. It acts as an **Edge Node** in the Sparkplug B ecosystem, converting UMH-Core messages into standardized MQTT-based Sparkplug B protocol with protobuf encoding and alias management.

Sparkplug B is an open standard for MQTT-based industrial IoT communication that minimizes bandwidth usage through metric aliases and efficient protobuf encoding. This output plugin always operates as an Edge Node - the role is implicit based on the plugin type (output plugins publish data, input plugins consume data).

**UMH-Core Format Requirement**: This output plugin only accepts data in the UMH-Core format (`{"value": X, "timestamp_ms": Y}`). When using the `uns` input plugin, data is already in the correct format. For other input sources, use the `tag_processor` to convert data to UMH-Core format before this output plugin.

## Quick Start

```yaml
input:
  uns: {}

output:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
    identity:
      group_id: "FactoryA"
      edge_node_id: "EdgeNode1"
      # device_id is auto-generated from message metadata "location_path"
```

This configuration reads UMH-Core data and publishes it as Sparkplug B messages. The output plugin always acts as an Edge Node in the Sparkplug B ecosystem.

### UMH-Core to Sparkplug B Mapping

Here's how a UMH-Core message maps to Sparkplug B:

**Configuration:**
```yaml
identity:
  group_id: "FactoryA"
  edge_node_id: "EdgeNode1"
```

**Input UMH-Core Message:**

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
  "location_path": "enterprise.factory.line1.station1",
  "virtual_path": "sensors.ambient", 
  "tag_name": "temperature",
  "data_contract": "_sparkplug"
}
```

**↓ Results in Sparkplug B Message:**
- **Topic**: `spBv1.0/FactoryA/DDATA/EdgeNode1/enterprise:factory:line1:station1`
- **Metric Name**: `sensors:ambient:temperature` (virtual_path + tag_name with colons)
- **Payload**: Protobuf with metric alias, value 23.5, timestamp

**Key Transformations:**
1. **Location Path**: `enterprise.factory.line1.station1` → Device ID `enterprise:factory:line1:station1` (dots → colons)
2. **Virtual Path + Tag Name**: `sensors.ambient` + `temperature` → Metric Name `sensors:ambient:temperature` (dots → colons)
3. **UMH-Core Format**: `{"value": 23.5, "timestamp_ms": 1672531200000}` → Sparkplug protobuf metric
4. **Topic Structure**: Uses configured `group_id` and `edge_node_id` from output plugin configuration


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
| `identity.edge_node_id` | `string` | **required** | Static Edge Node ID for Sparkplug B compliance (must be consistent throughout session) |
| `identity.device_id` | `string` | `""` | Device ID (empty for node-level messages, auto-generated from message metadata if not specified) |

### Advanced Configuration (Optional)

For advanced users who want to define static metric aliases:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `metrics` | `[]object` | optional | List of static metric definitions (for advanced alias management) |

### Metric Definition

Each metric in the `metrics` array supports:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| **name** | `string` | **yes** | Human-readable metric name |
| **alias** | `uint64` | **yes** | Unique numeric alias (1-65535) |
| **type** | `string` | **yes** | Sparkplug B data type |
| **value_from** | `string` | **yes** | JSON field name containing the value |
| **units** | `string` | no | Engineering units (e.g., "°C", "bar") |
| **is_historical** | `bool` | no | Whether this is historical data |
| **metadata** | `object` | no | Additional key-value metadata |



## Data Format Requirements

This output plugin requires input data to be in **UMH-Core format**:

```json
{
  "value": 25.4,
  "timestamp_ms": 1672531200000
}
```

**Compatible Input Sources:**
- ✅ `uns` input plugin (already in UMH-Core format)
- ✅ Any input + `tag_processor` (converts to UMH-Core format)

**Required Message Metadata:**
- `location_path`: Hierarchical location (e.g., "enterprise.factory.line1.station1")
- `tag_name`: Metric name (e.g., "temperature", "pressure")
- `data_contract`: Data contract identifier (e.g., "_sparkplug")
- `virtual_path`: Optional sub-path within device (e.g., "sensors.ambient")