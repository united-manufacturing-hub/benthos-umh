# Sparkplug B Input Plugin

## Overview

The **Sparkplug B Input plugin** allows the United Manufacturing Hub (UMH) to ingest data from MQTT brokers using the Sparkplug B specification. It subscribes to Sparkplug B MQTT topics (e.g., device birth/data/death messages) and converts the incoming Protobuf payloads into UMH-compatible messages. It maintains the stateful context required by Sparkplug B – tracking device birth certificates, metric alias mapping, and sequence numbers – so that incoming data is interpreted correctly.

This input plugin is designed to seamlessly integrate Sparkplug-enabled edge devices into the UMH **Unified Namespace**. It automatically decodes Sparkplug messages and enriches them with metadata (such as metric names, types, and timestamps) to fit the UMH-Core data model.

## Sparkplug B in UMH Architecture

### UMH's Modified Parris Method

UMH implements a **Modified Parris Method** that distributes hierarchy across both `device_id` and `metric_name` fields instead of cramming everything into `GroupID`. This approach provides significant advantages for multi-site deployments (e.g., a single UMH instance can ingest data from multiple sites):

**Key Innovation**: 
- **Location Hierarchy** → `device_id`: `"enterprise.site.area.line"` → `"enterprise:site:area:line"`
- **Virtual Path Hierarchy** → `metric_name`: `"motor.diagnostics" + "temperature"` → `"motor:diagnostics:temperature"`

### Architecture Roles

**Sparkplug B Input Plugin** (this plugin):
- **Role**: Host (Secondary Host by default, Primary Host optional)
- **Function**: Consumes Sparkplug B messages from external systems
- **Output**: Converts to UMH-Core format for UNS integration

**Sparkplug B Output Plugin** ([see output documentation](../output/sparkplug-b-output.md)):
- **Role**: Edge Node
- **Function**: Publishes UMH-Core data as Sparkplug B messages
- **Input**: Receives UMH-Core format from UNS

### Integration with UMH Unified Namespace

The Sparkplug B plugins integrate seamlessly with the UMH UNS architecture:

**Data Ingestion Flow**:

External Sparkplug B Systems → Sparkplug B Input Plugin (Host) → [tag_processor](../processing/tag-processor.md) → [UNS Output](../output/uns-output.md) → UNS

**Data Publication Flow**:
UNS → [UNS Input](uns-input.md) → UMH-Core Format → [Sparkplug B Output Plugin](../output/sparkplug-b-output.md) (Edge Node) → External Systems

### Why Modified Parris Method Matters

Unlike the original Parris Method which creates separate state management per GroupID, UMH's approach enables unified state management across all organizational levels by preserving hierarchy in `device_id` and `metric_name` fields, allowing scalable multi-enterprise/multi-site data ingestion without state explosion.

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

          # For Sparkplug B input data, use _raw data contract
          msg.meta.data_contract = "_raw";
          
          # Note: UMH conversion will use this data contract
          # Common options: "_raw", "_historian", "_sparkplug"

output:
  uns: {}
```

This configuration safely reads all Sparkplug B messages and converts them to UMH-Core format. Multiple Secondary Host instances (default role) can run simultaneously without conflicts for load balancing and redundancy.

**To publish data as Sparkplug B**: After processing in the UNS, use the [Sparkplug B Output plugin](../output/sparkplug-b-output.md) to convert UMH-Core data back to Sparkplug B format for external systems.

### Sparkplug B to UMH-Core Mapping

Here's how a Sparkplug B message maps to UMH-Core using the Modified Parris Method:

**Input Sparkplug B Message:**
- **Topic**: `spBv1.0/FactoryA/DDATA/EdgeNode1/enterprise:factory:line1:station1`
- **Metric Name**: `sensors:ambient:temperature` 
- **Payload**: Protobuf with metric alias, value 23.5, timestamp

**↓ Results in Structured JSON Message:**

**Payload:**
```json
{
  "name": "sensors:ambient:temperature",
  "alias": 42,
  "value": 23.5
}
```

**Metadata:**
```json
{
  "spb_group_id": "FactoryA",
  "spb_edge_node_id": "EdgeNode1", 
  "spb_device_id": "enterprise:factory:line1:station1",
  "spb_metric_name": "sensors:ambient:temperature",
  "spb_message_type": "DDATA",
  "umh_location_path": "enterprise.factory.line1.station1",
  "umh_virtual_path": "sensors.ambient",
  "umh_tag_name": "temperature"
}
```

**Key Transformations:**
1. **Device ID to Location Path**: `enterprise:factory:line1:station1` → `location_path: "enterprise.factory.line1.station1"` (colons → dots)
2. **Metric Name Parsing**: `sensors:ambient:temperature` → `virtual_path: "sensors.ambient"` + `tag_name: "temperature"` (colons → dots)
3. **Sparkplug Protobuf**: Metric value and alias → Structured JSON format `{"name": "...", "alias": X, "value": Y}`
4. **Topic Components**: Group/EdgeNode from MQTT topic used for `spb_group_id` and `spb_edge_node_id` metadata

**Reverse Transformation**: The [Sparkplug B Output plugin](../output/sparkplug-b-output.md) performs the inverse transformation to convert UMH-Core messages back to Sparkplug B format.

## Configuration Reference

### MQTT Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mqtt.urls` | `[]string` | **required** | List of MQTT broker URLs |
| `mqtt.client_id` | `string` | `"benthos-sparkplug-input"` | MQTT client identifier |
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
| `identity.edge_node_id` | `string` | `""` | Optional: For advanced Primary Host configuration only |

### Role Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `role` | `string` | `"host"` | Sparkplug Host role: `"host"` (Secondary Host) or `"primary"` (Primary Host) |

### Subscription Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `subscription.groups` | `[]string` | `[]` | Groups to subscribe to (empty = all groups) |

---

## Technical Details

### Host Roles and STATE Messages

For advanced users who need to understand the different host roles:

#### Secondary Host (Default - Recommended)
- **Role**: `"host"` (default, no configuration needed)
- **Behavior**: Read-only, no STATE message publishing
- **Use Case**: Safe for brownfield deployments, multiple instances can run simultaneously for load balancing
- **Requirements**: None (edge_node_id is optional)

#### Primary Host (Advanced Use Only)
- **Role**: `"primary"` (explicit configuration required)
- **Behavior**: Publishes STATE Birth/Death messages for host arbitration
- **Use Case**: SCADA/HMI applications that need to coordinate with Edge Nodes
- **Requirements**: `edge_node_id` is required (used as `host_id`)
- **Deployment**: Single instance only (multiple Primary Hosts create STATE conflicts)

**Edge Node Role**: The input plugin cannot act as an Edge Node (that's the role of the [Sparkplug B Output plugin](../output/sparkplug-b-output.md)). Edge Nodes publish NBIRTH/NDATA messages, while Host applications (like this input plugin) consume and process Sparkplug B data.

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

### Deployment Considerations

#### Multiple Instance Support

**Secondary Host (Default - `role: "host"`)**: 
✅ **Safe for multiple instances** - Read-only operation, no conflicts

**Primary Host (`role: "primary"`)**: 
⚠️ **Single instance only** - Publishes STATE messages for host arbitration

### Metadata Enrichment

The plugin attaches comprehensive Sparkplug-specific metadata fields to each output message. These are organized into **primary fields** (commonly used) and **secondary fields** (for advanced use cases):

#### Primary Metadata Fields

These are the main metadata fields that most users will need for processing Sparkplug messages:

* `spb_message_type`: The Sparkplug message type (e.g., "NBIRTH", "NDATA", "NDEATH", "DBIRTH", "DDATA", "DDEATH")
* `spb_group_id`: The Sparkplug Group ID of the source message
* `spb_edge_node_id`: The Edge Node ID (equipment or gateway name)
* `spb_device_id`: The Device ID (for metrics from devices under an edge node, empty for node-level messages)
* `spb_device_key`: Combined device identifier in format "group_id/edge_node_id" or "group_id/edge_node_id/device_id"
* `spb_topic`: The original MQTT topic the message was received from
* `spb_metric_name`: The Sparkplug metric name (either from name field or alias_X format)

#### Secondary Metadata Fields (Advanced)

These fields provide additional Sparkplug context and are primarily for debugging or advanced processing:

* `spb_group`: Same as `spb_group_id` (for backward compatibility)
* `spb_edge_node`: Same as `spb_edge_node_id` (for backward compatibility)
* `spb_device`: Same as `spb_device_id` (for backward compatibility)
* `spb_sequence`: The sequence number of the Sparkplug message
* `spb_bdseq`: The birth-death sequence number of the session
* `spb_timestamp`: The timestamp (in epoch ms) provided with the metric
* `spb_datatype`: The Sparkplug data type of the metric (e.g. "Int32", "Double", "Boolean")
* `spb_alias`: The alias number of the metric (for debugging alias resolution)
* `spb_is_historical`: Set to "true" if the metric was flagged as historical

#### Special Message Types

For STATE messages, the plugin sets:
* `event_type`: "state_change" 
* `node_state`: The state value ("ONLINE" or "OFFLINE")

For NDEATH/DDEATH messages, the plugin sets:
* `event_type`: "device_offline"

#### UMH Conversion Metadata (Optional)

When UMH conversion is successful, additional metadata is added:

* `umh_conversion_status`: "success", "failed", "skipped_insufficient_data", or "failed_no_value"
* `umh_location_path`: Converted UMH location path (dots format)
* `umh_tag_name`: UMH tag name extracted from metric name
* `umh_data_contract`: UMH data contract (e.g., "_raw", "_historian")
* `umh_virtual_path`: UMH virtual path if present in metric name
* `umh_topic`: Complete UMH topic string
* `umh_conversion_error`: Error message if conversion failed

**Usage Recommendation**: Use the **primary metadata fields** for most processing logic. The `spb_` prefixed fields are provided for backward compatibility and advanced debugging scenarios.

### Message Processing

The Sparkplug B input plugin **always splits metrics** into individual messages to ensure UMH-Core format compatibility. Each Sparkplug metric becomes a separate Benthos message for downstream processing.

*Note: This behavior is required for UMH-Core format and cannot be disabled.*

## Stateless Architecture Considerations

### Understanding bdSeq (Birth-Death Sequence) in Sparkplug B

The Sparkplug B input plugin processes **bdSeq** values from incoming Edge Node messages. Understanding bdSeq behavior is important for monitoring Edge Node session lifecycle:

**Specification-Compliant Edge Nodes:**
- bdSeq should increment by +1 for each new MQTT session
- Example: Session 1: bdSeq=0 → Session 2: bdSeq=1 → Session 3: bdSeq=2

**Stateless Edge Nodes (like Benthos Sparkplug B Output):**
- bdSeq may reset to 0 on Edge Node component restart
- This is common in container-based or stateless Edge Node implementations
- Still compliant within individual component lifecycles

### Impact on Input Processing

**What to Expect:**
- Edge Nodes may send bdSeq=0 after restarts (not necessarily the first session)
- bdSeq jumps or resets indicate Edge Node restarts or different implementations
- This is normal behavior for stateless architectures

**Recommendation:**
The input plugin handles both persistent and stateless Edge Node bdSeq patterns correctly. No special configuration is needed - the plugin automatically adapts to different Edge Node implementations and their bdSeq behaviors.


