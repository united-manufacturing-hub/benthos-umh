# Sparkplug B Input Plugin

## Overview

The **Sparkplug B Input plugin** allows the United Manufacturing Hub (UMH) to ingest data from MQTT brokers using the Sparkplug B specification. It subscribes to Sparkplug B MQTT topics (e.g., device birth/data/death messages) and converts the incoming Protobuf payloads into UMH-compatible messages. It maintains the stateful context required by Sparkplug B – tracking device birth certificates, metric alias mapping, and sequence numbers – so that incoming data is interpreted correctly.

This input plugin is designed to seamlessly integrate Sparkplug-enabled edge devices into the UMH **Unified Namespace**. It automatically decodes Sparkplug messages and enriches them with metadata (such as metric names, types, and timestamps) to fit the UMH-Core data model.

## Sparkplug B in UMH Architecture

### UMH's Modified Parris Method

UMH implements a **Modified Parris Method** that distributes hierarchy across both `device_id` and `metric_name` fields instead of cramming everything into `GroupID`. This approach provides significant advantages for multi-site deployments (e.g., a single UMH instance can ingest data from multiple sites):

**Key Innovation**:
- **Location Hierarchy** → `device_id`: `"enterprise.site.area.line"` → `"enterprise:site:area:line"`
- **Virtual Path Hierarchy** → `metric_name`: Supports multiple separators:
  - Colons: `"motor:diagnostics:temperature"` → virtual_path=`"motor.diagnostics"` + tag_name=`"temperature"`
  - Slashes: `"motor/diagnostics/temperature"` → virtual_path=`"motor.diagnostics"` + tag_name=`"temperature"`
  - Dots: `"motor.diagnostics.temperature"` → virtual_path=`"motor.diagnostics"` + tag_name=`"temperature"`

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
    # mode: "secondary_passive" is default - safest for brownfield deployments

pipeline:
  processors:
    - tag_processor:
        defaults: |
          // msg.meta.location_path = "..."; // automatic from the device_id (see also output plugin)
          // msg.meta.virtual_path = "..."; // automatic from the metric name (see also output plugin)
          // msg.meta.tag_name = "..."; // automatic from the metric name (see also output plugin)

          // For Sparkplug B input data, use _raw data contract
          msg.meta.data_contract = "_raw";

          // Note: UMH conversion will use this data contract
          // Common options: "_raw", "_historian", "_sparkplug"

output:
  uns: {}
```

This configuration safely reads all Sparkplug B messages and converts them to UMH-Core format. The default `secondary_passive` mode is read-only and won't interfere with existing Sparkplug infrastructure, making it safe to run multiple instances for load balancing and redundancy.

**To publish data as Sparkplug B**: After processing in the UNS, use the [Sparkplug B Output plugin](../output/sparkplug-b-output.md) to convert UMH-Core data back to Sparkplug B format for external systems.

### Sparkplug B to UMH-Core Mapping

Here's how a Sparkplug B message maps to UMH-Core using the Modified Parris Method:

**Input Sparkplug B Message:**
- **Topic**: `spBv1.0/FactoryA/DDATA/EdgeNode1/enterprise:factory:line1:station1`
- **Metric Name**: `sensors:ambient:temperature` (or `sensors/ambient/temperature` or `sensors.ambient.temperature`)
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
2. **Metric Name Parsing**: Splits on last separator (priority: colon > slash > dot)
   - `sensors:ambient:temperature` → `virtual_path: "sensors.ambient"` + `tag_name: "temperature"`
   - `sensors/ambient/temperature` → `virtual_path: "sensors.ambient"` + `tag_name: "temperature"`
   - `sensors.ambient.temperature` → `virtual_path: "sensors.ambient"` + `tag_name: "temperature"`
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
| `identity.edge_node_id` | `string` | `""` | Required only for `primary` mode (used as host_id for STATE topic) |

### Mode Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | `string` | `"secondary_passive"` | Operating mode for the Sparkplug B input plugin |

**Available Modes:**
- `"secondary_passive"` (default): Read-only consumer, no rebirth commands sent
- `"secondary_active"`: Consumer that can request rebirths when needed
- `"primary"`: Full Primary Host with STATE publishing and session management

---

## Technical Details

### Operating Modes Explained

The Sparkplug B input plugin offers three operating modes that automatically configure all necessary settings. These modes provide clear choices for different deployment scenarios while eliminating the complexity of understanding Sparkplug B roles.

#### Mode Overview

| Mode | Role | Description | Safe for Brownfield |
|------|------|-------------|---------------------|
| `secondary_passive` | Secondary Host (Muted) | Read-only consumer, no rebirth commands | ✅ Yes (Default) |
| `secondary_active` | Secondary Host (Unmuted) | Consumer that can request rebirths | ✅ Yes |
| `primary` | Primary Host | Full host with STATE publishing | ⚠️ May conflict |

#### Mode: `secondary_passive` (Default)

**What it does:**
- Operates as a read-only Secondary Host
- **Does NOT send rebirth commands** (fully passive)
- Does NOT publish STATE messages
- Safe to run multiple instances for scalability
- Consumes all Sparkplug B messages without interfering

**When to use:**
- ✅ **Default choice** - Safest option for any deployment
- ✅ **Brownfield deployments** - Existing infrastructure remains undisturbed
- ✅ **Multi-consumer environments** - Prevents rebirth storms
- ✅ **Uncertain scenarios** - When you're not sure about the infrastructure

**Configuration:**
```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
    identity:
      group_id: "FactoryA"
    # mode: "secondary_passive" is the default
```

**Technical behavior:** Pure consumer mode with no command publishing capabilities.

#### Mode: `secondary_active`

**What it does:**
- Operates as an active Secondary Host
- **Can send rebirth commands** when needed
- Does NOT publish STATE messages
- Can run multiple instances (though rebirth storms possible)
- Actively manages alias resolution through rebirth requests

**When to use:**
- ✅ **Single consumer deployments** - You're the only Sparkplug B consumer
- ✅ **Controlled environments** - You understand the rebirth implications
- ✅ **Active data management needed** - You need to request fresh BIRTH certificates

**Configuration:**
```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
    identity:
      group_id: "FactoryA"
    mode: "secondary_active"
```

**Technical behavior:** Secondary Host with NCMD/DCMD publishing for rebirth requests.

#### Mode: `primary`

**What it does:**
- Operates as the Primary Host per Sparkplug B specification
- Publishes STATE messages for Edge Node coordination
- Monitors sequence numbers and manages sessions
- Single instance only (multiple Primary Hosts conflict)
- Full control over Edge Node behavior

**When to use:**
- ✅ **Greenfield deployments** - UMH is your only Sparkplug B application
- ✅ **Full control needed** - You want Edge Nodes to buffer data when offline
- ✅ **Spec compliance required** - Strict Sparkplug B v3.0 compliance

**Configuration:**
```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
    identity:
      group_id: "FactoryA"
      edge_node_id: "UMH_Primary"  # Required for STATE topic
    mode: "primary"
```

**Technical behavior:** Full Primary Host with STATE publishing and session management.

#### Understanding Rebirth Storms

**What is a rebirth storm?**
A cascading effect that occurs when multiple Secondary Hosts simultaneously request rebirths from Edge Nodes, potentially overwhelming the MQTT infrastructure with redundant BIRTH messages.

**Scenario:**
1. Edge Node publishes DDATA with aliases
2. Multiple consumers don't have the alias mappings
3. All consumers simultaneously request rebirth
4. Edge Node publishes NBIRTH/DBIRTH for each request
5. Network and broker become saturated

**Prevention:**
- Use `secondary_passive` mode (default) in multi-consumer environments
- Only use `secondary_active` when you're the sole consumer
- Coordinate rebirth requests if multiple active consumers are necessary

#### Choosing the Right Mode

**Start with `secondary_passive` if:**
- You have any existing Sparkplug B infrastructure
- Multiple applications consume the same data
- You're unsure about the deployment environment
- You want the safest, most compatible option

**Consider `secondary_active` if:**
- You're the only Sparkplug B consumer
- You need to actively request BIRTH certificates
- You understand and can manage rebirth timing
- Network bandwidth isn't a concern

**Use `primary` only if:**
- You're building a greenfield Sparkplug B system
- No other Primary Hosts exist in your infrastructure
- You need Edge Nodes to respect your online/offline state
- You require full Sparkplug B specification compliance


### Advanced Configuration Options

For users who need fine-grained control beyond the simplified modes, additional configuration options are available:

```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
    identity:
      group_id: "FactoryA"
      edge_node_id: "CustomHost"  # Optional for secondary modes
    mode: "secondary_passive"

    # Advanced options (usually not needed)
    subscription:
      groups: ["GroupA", "GroupB"]  # Specific groups instead of all

    # Future options (planned):
    # include_data_contract_in_device_id: true  # Add data contract to device ID
```

### STATE Topic Behavior (primary mode only)

When using `primary` mode, the plugin publishes STATE messages according to Sparkplug B v3.0:

```
spBv1.0/STATE/<host_id>
```

- The `edge_node_id` is used as the `host_id`
- STATE topics do NOT include `group_id` (per specification)
- This allows Edge Nodes across all groups to detect the Primary Host

### Deployment Considerations

#### Multiple Instance Support

**`secondary_passive` mode (default)**:
✅ **Safe for multiple instances** - No STATE conflicts, no rebirth storms, load balancing friendly

**`secondary_active` mode**:
✅ **Can run multiple instances** - But be aware of potential rebirth storms

**`primary` mode**:
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

* `spb_group`: Same as `spb_group_id` (alternative field name)
* `spb_edge_node`: Same as `spb_edge_node_id` (alternative field name)
* `spb_device`: Same as `spb_device_id` (alternative field name)
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
* `umh_location_path`: Converted UMH location path (dots format, sanitized)
* `umh_tag_name`: UMH tag name extracted from metric name (sanitized)
* `umh_data_contract`: UMH data contract (e.g., "_raw", "_historian")
* `umh_virtual_path`: UMH virtual path if present in metric name (sanitized)
* `umh_conversion_error`: Error message if conversion failed

**Automatic Sanitization**: The plugin handles Sparkplug metric names to ensure UMH compatibility through a clear architectural boundary at the format conversion layer:

1. **Input Processing**: Sparkplug messages are received with their original metric names intact (no preprocessing)
2. **Format Conversion**: The format converter parses and sanitizes during the conversion to UMH format:
   - Splits metric names to extract virtual paths (priority: colon > slash > dot)
   - Example: `vpath:segment:metric` → virtual_path=`vpath.segment`, tag_name=`metric`
   - Trims leading/trailing separators before parsing
3. **Sanitization**: Applied only at the conversion boundary to preserve data integrity

Sanitization rules (applied during conversion):
- Hierarchy separators (`/`, `:`) are converted to dots (`.`) to preserve structure
- Invalid characters are replaced with underscores (`_`)
- Valid characters are: `a-z`, `A-Z`, `0-9`, `.`, `_`, `-`
- Multiple consecutive dots are collapsed into a single dot
- Leading and trailing dots are removed

Examples:
- `Refrigeration/Tower1/Pumps/chemHOA` → `Refrigeration.Tower1.Pumps.chemHOA`
- `Device@Name#123` → `Device_Name_123`
- `Area/Zone@1/Device#2` → `Area.Zone_1.Device_2`
- `Path//with///slashes` → `Path.with.slashes` (double dots prevented)
- `/hello123/test/` → virtual_path=`hello123`, tag_name=`test` (slashes trimmed, then split)
- `vpath:segment:metric` → virtual_path=`vpath.segment`, tag_name=`metric` (colons to dots)

When sanitization occurs, the plugin preserves the original values in metadata:
- `spb_original_metric_name`: The original metric name before sanitization (if different from spb_metric_name)
- `spb_original_device_id`: The original device ID before sanitization (if different from spb_device_id)

The sanitized values are available in the UMH metadata fields:
- `umh_location_path`: The sanitized location path (from device ID)
- `umh_virtual_path`: The sanitized virtual path (from metric name)
- `umh_tag_name`: The sanitized tag name (from metric name)

This ensures that messages with non-compliant Sparkplug names are automatically converted to valid UMH topics while preserving the original values for reference.

**Usage Recommendation**: Use the **primary metadata fields** for most processing logic. The alternative `spb_` prefixed fields are provided for consistency and advanced debugging scenarios.

### Message Processing

The Sparkplug B input plugin **always splits metrics** into individual messages to ensure UMH-Core format compliance. Each Sparkplug metric becomes a separate Benthos message for downstream processing.

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

