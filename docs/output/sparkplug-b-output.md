# Sparkplug B (Output)

## Overview

The **Sparkplug B Output plugin** allows the United Manufacturing Hub (UMH) to publish industrial IoT data to MQTT brokers using the Sparkplug B specification. It acts as an **Edge Node** in the Sparkplug B ecosystem, converting UMH-Core messages into standardized MQTT-based Sparkplug B protocol with protobuf encoding and alias management.

Sparkplug B is an open standard for MQTT-based industrial IoT communication that minimizes bandwidth usage through metric aliases and efficient protobuf encoding. 

### Why Edge Node Only?

This output plugin **always operates as an Edge Node** because:

1. **Role Clarity**: In Sparkplug B architecture, data sources (PLCs, sensors, gateways) are Edge Nodes, while data consumers (SCADA, historians) are Hosts
2. **UMH Philosophy**: UMH acts as a data source when publishing to external systems, naturally fitting the Edge Node role
3. **No Conflicts**: Edge Nodes don't publish STATE messages, avoiding conflicts with existing Primary Hosts in your infrastructure
4. **Responds to Hosts**: Edge Nodes listen for rebirth commands from Host applications, enabling proper Sparkplug B session management

The complementary [Sparkplug B Input plugin](../input/sparkplug-b-input.md) handles the Host role for consuming Sparkplug B data.

**UMH-Core Format Requirement**: This output plugin only accepts data in the UMH-Core format (`{"value": X, "timestamp_ms": Y}`). When using the `uns` input plugin, data is already in the correct format. For other input sources, use the `tag_processor` to convert data to UMH-Core format before this output plugin.

**For Sparkplug B Architecture Overview**: See the [Sparkplug B Input plugin documentation](../input/sparkplug-b-input.md) for a comprehensive explanation of:
- UMH's Modified Parris Method and how it differs from industry standards
- Integration with the UMH Unified Namespace architecture
- Host vs Edge Node roles and their relationship

This output plugin implements the **Edge Node** role that complements the **Host** role of the input plugin.

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
      # device_id is optional - if not specified, generated from location_path metadata
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
| `mqtt.client_id` | `string` | `"benthos-sparkplug-output"` | MQTT client identifier |
| `mqtt.credentials.username` | `string` | `""` | MQTT username |
| `mqtt.credentials.password` | `string` | `""` | MQTT password |
| `mqtt.qos` | `int` | `1` | MQTT QoS level |
| `mqtt.keep_alive` | `duration` | `"60s"` | MQTT keep alive interval |
| `mqtt.connect_timeout` | `duration` | `"30s"` | Connection timeout |
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

### Behaviour Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `behaviour.auto_extract_tag_name` | `bool` | `true` | Whether to automatically extract tag_name from message metadata |
| `behaviour.retain_last_values` | `bool` | `true` | Whether to retain last known values for BIRTH messages after reconnection |



## Edge Node Behavior

### Automatic Session Management

As a Sparkplug B Edge Node, this plugin handles the complete session lifecycle:

1. **Connection**: Publishes NBIRTH with all configured metrics and bdSeq
2. **Device Discovery**: Publishes DBIRTH when new devices (location_paths) appear
3. **Data Flow**: Publishes DDATA with efficient alias-based encoding
4. **Disconnection**: NDEATH published automatically via MQTT Last Will Testament

### Rebirth Command Handling

The Edge Node listens for rebirth requests from Host applications on the NCMD topic:

```
spBv1.0/<group_id>/NCMD/<edge_node_id>
```

**When a rebirth is requested:**
1. bdSeq increments by +1
2. Republishes NBIRTH with all node-level metrics
3. Republishes DBIRTH for all known devices
4. Resumes normal DDATA publishing

**Why This Matters:**
- Hosts can request fresh BIRTH certificates after restart
- Ensures alias mappings stay synchronized
- Maintains Sparkplug B session integrity

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

## Stateless Architecture Limitations

### bdSeq (Birth-Death Sequence) Behavior

The Sparkplug B output plugin implements **bdSeq** (Birth-Death Sequence) according to the Sparkplug B v3.0 specification:

**Within Component Lifetime** (✅ Specification Compliant):
- bdSeq starts at 0 for the first MQTT session
- bdSeq increments by +1 for each subsequent MQTT reconnection session
- Example: Session 1: bdSeq=0 → Session 2: bdSeq=1 → Session 3: bdSeq=2

**Across Component Restarts** (⚠️ Stateless Limitation):
- bdSeq resets to 0 when the Benthos component is restarted
- This is a fundamental limitation of Benthos's stateless architecture
- No persistence mechanism is available (no database/disk storage)

### What This Means for Users

**Expected Behavior:**
```
Component Start 1: bdSeq=0 → reconnect → bdSeq=1 → reconnect → bdSeq=2
Component Restart: bdSeq=0 (resets)
Component Start 2: bdSeq=0 → reconnect → bdSeq=1 → reconnect → bdSeq=2
```

**Impact:**
- **Acceptable** for most Sparkplug deployments where Edge Nodes naturally reset bdSeq on restart
- **Compatible** with brownfield deployments and development environments  
- **Limitation** for deployments requiring persistent bdSeq across component restarts

### Recommendation

This stateless behavior is acceptable for the majority of Sparkplug B use cases. Many industrial Edge Node implementations also reset bdSeq on restart. If your specific use case requires persistent bdSeq across component restarts, consider using a dedicated Sparkplug B implementation with persistent storage capabilities.