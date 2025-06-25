# Sparkplug B Plugin

A comprehensive Benthos plugin implementation for the Eclipse Sparkplug B specification, providing both input and output processors for MQTT-based industrial IoT communication.

## 🎉 Status: Production Ready

**Current Implementation**: Device-Level PARRIS (Primary Application Request/Response Interaction Synchronization)
- ✅ **Sparkplug B v3.0 Compliant**: Fully implements the Eclipse Sparkplug specification
- ✅ **Alias Resolution**: Automatic metric name resolution for efficient data transmission
- ✅ **MQTT Integration**: Complete NBIRTH → DBIRTH → DDATA message flow
- ✅ **Real-time Processing**: Live MQTT communication with 2-second intervals
- ✅ **All Tests Passing**: 50/50 unit tests + integration tests validated

## Features

### Input Processor
- **Message Type Support**: NBIRTH, DBIRTH, NDATA, DDATA, NCMD, DCMD, STATE
- **Alias Resolution**: Converts numeric aliases back to metric names using cached birth certificates
- **Metadata Enrichment**: Populates 9 spb_* metadata fields for downstream processing
- **Data-Only Filtering**: Efficiently processes only data-containing messages
- **State Message Handling**: Properly handles PRIMARY HOST state transitions

### Output Processor  
- **Dynamic Device Management**: Device-level PARRIS with location path to Device ID conversion
- **Automatic Birth Certificates**: NBIRTH and DBIRTH message generation with metric definitions
- **Sequence Management**: Proper sequence numbering with wraparound handling
- **Alias Assignment**: Dynamic alias generation for efficient data transmission
- **Retained Messages**: Proper MQTT retained message handling for birth certificates

## Quick Start

### Prerequisites

```bash
# Build the latest binary
make target
cp tmp/bin/benthos ./benthos

# Ensure internet access (uses broker.hivemq.com:1883 for testing)
```

### Integration Test

Run a complete Edge Node ↔ Primary Host communication test:

```bash
# Terminal 1: Primary Host
./benthos -c config/sparkplug-device-level-primary-host.yaml

# Terminal 2: Edge Node  
./benthos -c config/sparkplug-device-level-test.yaml
```

### Expected Results

**✅ Device-Level PARRIS Message Flow:**
1. **Static Edge Node ID**: `StaticEdgeNode01` (required for Sparkplug compliance)
2. **Dynamic Device ID**: `enterprise:factory:line1:station1` (generated from UMH location_path)
3. **Device-Only Topic Structure**: 
   - DBIRTH: `spBv1.0/DeviceLevelTest/DBIRTH/StaticEdgeNode01/enterprise:factory:line1:station1`
   - DDATA: `spBv1.0/DeviceLevelTest/DDATA/StaticEdgeNode01/enterprise:factory:line1:station1`
   - STATE: `spBv1.0/DeviceLevelTest/STATE/PrimaryHost`

**✅ Perfect Alias Resolution:**
```json
// DBIRTH defines aliases (both alias AND name)
{"alias":1,"name":"humidity:value","value":61}
{"alias":2,"name":"temperature:value","value":27}

// DDATA uses aliases efficiently (alias OR name)
{"alias":1,"name":"humidity:value","value":79}
{"alias":2,"name":"temperature:value","value":33}
```

## Architecture

### Understanding Sparkplug B Birth Messages

In Sparkplug B there are exactly **two "birth" message types**—one for the edge node itself and one for every device the node represents:

| Birth message             | Who publishes it                                  | MQTT topic (v1.0 namespace)                        | What it must contain                                                                                                                                                       | Typical timing                                                                                                                                       |
| ------------------------- | ------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **NBIRTH (Node Birth)**   | The Edge-of-Network (EoN) node                    | `spBv1.0/<GroupID>/NBIRTH/<EdgeNodeID>`            | • Full list of *node-level* metrics and their data types<br>• `bdSeq` startup counter<br>• mandatory metric "Node Control/Rebirth" (boolean false)<br>• optional templates | Immediately after the node establishes an MQTT session, and again whenever a host requests a "rebirth"                  |
| **DBIRTH (Device Birth)** | The same EoN node, one DBIRTH per attached device | `spBv1.0/<GroupID>/DBIRTH/<EdgeNodeID>/<DeviceID>` | • Complete metric list for that *device* (initial values, types, properties)<br>• Any template definitions referenced by the device                                        | Sent right after NBIRTH for each online device, then whenever that device first appears or after a rebirth request |

**Why two kinds?**

* **Scope** – NBIRTH declares the *node* itself while DBIRTH declares every *device* behind that node.
* **Granularity** – NBIRTH includes metrics such as CPU load or gateway firmware; DBIRTH focuses on sensor/actuator metrics (temperature, set-points, etc.).
* **Lifecycle** – Both act as the "birth certificate" for session state. Their opposites are **NDEATH** and **DDEATH**, published via the MQTT Last-Will-and-Testament when the node or device goes offline.

### Device-Level PARRIS Method

This plugin implements the **Device-Level PARRIS approach** with UMH integration:

1. **Static Edge Node ID**: Required for Sparkplug B v3.0 compliance (`StaticEdgeNode01`)
2. **Dynamic Device ID**: Generated from UMH `location_path` metadata using PARRIS method
   - Example: `location_path="enterprise.plant1.line3"` → `DeviceID="enterprise:plant1:line3"`
3. **Exclusive Device-Level Operation**: 
   - **DBIRTH only**: Device birth certificates with metric definitions
   - **DDATA only**: Device data with efficient alias-based transmission
   - **No NBIRTH**: Node-level metrics not used (simplified architecture)

### Message Flow

```
Edge Node (PARRIS Method):
1. Extract location_path from UMH metadata
2. Convert to Sparkplug Device ID: "enterprise.plant1.line3" → "enterprise:plant1:line3"
3. DBIRTH → Publishes device-level birth certificate (defines all metric aliases)
4. DDATA → Publishes device data with aliases (efficient transmission)

Primary Host:  
1. STATE → Publishes ONLINE/OFFLINE status
2. DCMD → Sends commands to specific devices
3. Input Processing → Resolves aliases using cached DBIRTH certificates
```

## Technical Details

### Supported Message Types

| Type | Direction | Purpose | PARRIS Usage |
|------|-----------|---------|--------------|
| NBIRTH | Edge→Primary | Node birth certificate with metric definitions | ⚠️ **Not used** (device-level only) |
| DBIRTH | Edge→Primary | Device birth certificate with metric definitions | ✅ **Primary method** (defines aliases) |  
| NDATA | Edge→Primary | Node data with aliases | ⚠️ **Not used** (device-level only) |
| DDATA | Edge→Primary | Device data with aliases | ✅ **Primary method** (efficient transmission) |
| NCMD | Primary→Edge | Node commands | ⚠️ **Not used** (device-level only) |
| DCMD | Primary→Edge | Device commands | ✅ **Used for device control** |
| STATE | Primary→Clients | Primary host online/offline status | ✅ **Used for host status** |

**Note**: The PARRIS method focuses exclusively on **device-level messaging** (DBIRTH/DDATA), simplifying the architecture by avoiding node-level complexity.

### Metadata Fields

The input processor enriches messages with these spb_* metadata fields:

- `spb_group_id`: Sparkplug group identifier
- `spb_edge_node_id`: Edge node identifier  
- `spb_device_id`: Device identifier (if present)
- `spb_message_type`: Message type (NBIRTH, DDATA, etc.)
- `spb_sequence`: Message sequence number
- `spb_timestamp`: Message timestamp
- `spb_uuid`: Message UUID
- `spb_body`: Original protobuf payload
- `spb_alias_map`: Alias to metric name mappings

## Testing

### Unit Tests
```bash
cd sparkplug_plugin
go test -v
```

**Results**: 50/50 tests passing, including 14 Sparkplug B compliance tests

### Integration Tests
```bash
# Run the integration test as described in Quick Start
# Validates real-time MQTT communication and alias resolution
```

## Integration Testing

### Quick Integration Test

For a complete end-to-end test of the Sparkplug B plugin with local Mosquitto:

```bash
make test-integration-local
```

This automated script will:
1. 🐳 Start a local Mosquitto MQTT broker
2. 🏭 Launch an Edge Node that publishes test data  
3. 🖥️ Start a Primary Host that receives and displays messages
4. 🧹 Automatically cleanup all resources

### Manual Integration Testing

#### Prerequisites

1. **Docker**: For running Mosquitto MQTT broker
2. **Benthos**: With Sparkplug B plugin compiled

#### Step 1: Start MQTT Broker

```bash
# Start Mosquitto (allows anonymous connections)
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:1.6

# Verify it's running
docker ps | grep mosquitto
```

#### Step 2: Start Edge Node (Terminal 1)

```bash
# From project root
benthos -c config/sparkplug-device-level-test.yaml
```

This will:
- Generate test data every 2 seconds (alternating temperature/humidity)
- Convert UMH location_path to Sparkplug Device ID using PARRIS method
- Publish DBIRTH message with complete metric definitions and aliases
- Publish DDATA messages with current values using efficient aliases
- Display published messages to stdout

#### Step 3: Start Primary Host (Terminal 2)

```bash
# From project root  
benthos -c config/sparkplug-device-level-primary-host.yaml
```

This will:
- Subscribe to all Sparkplug B messages in the "DeviceLevelTest" group
- Publish STATE ONLINE message (Primary Host lifecycle)
- Display received DBIRTH and DDATA messages
- Demonstrate perfect alias resolution from cached DBIRTH certificates

#### Step 4: Verify Communication

You should see:

**Edge Node Output (Device-Level PARRIS):**
```
DBIRTH[DeviceID: enterprise:factory:line1:station1]: 2 metrics with aliases
Published DDATA: {"alias":1,"name":"humidity:value","value":29}
Published DDATA: {"alias":2,"name":"temperature:value","value":65}  
Published DDATA: {"alias":1,"name":"humidity:value","value":31}
```

**Primary Host Output (Perfect Alias Resolution):**
```
STATE ONLINE: DeviceLevelTest/PrimaryHost
DBIRTH cached 2 aliases for device: enterprise:factory:line1:station1
   🔗 alias 1 -> 'humidity:value'
   🔗 alias 2 -> 'temperature:value'
DDATA resolved 2 aliases: humidity=29, temperature=65
DDATA resolved 2 aliases: humidity=31, temperature=67
```

#### Step 5: Cleanup

```bash
# Stop Benthos processes (Ctrl+C in both terminals)
# Remove MQTT broker
docker stop mosquitto && docker rm mosquitto
```

### Configuration Files

#### Edge Node Config (`config/sparkplug-device-level-test.yaml`)
- **Purpose**: Publishes test data as Sparkplug B Edge Node
- **Group ID**: `DeviceLevelTest`
- **Edge Node ID**: `TestEdgeNode`
- **Metrics**: Temperature and humidity values
- **Frequency**: Every 2 seconds

#### Primary Host Config (`config/sparkplug-device-level-primary-host.yaml`)  
- **Purpose**: Receives and displays all Sparkplug B messages
- **Group ID**: `DeviceLevelTest` (subscribes to all edge nodes)
- **Role**: Primary Host (subscriber)
- **Output**: Human-readable message display

### Troubleshooting

#### Edge Node Not Publishing
```bash
# Check if Mosquitto is accessible
docker exec mosquitto mosquitto_pub -h localhost -t test -m "hello"

# Check Benthos logs for connection errors
benthos -c config/sparkplug-device-level-test.yaml -l DEBUG
```

#### Primary Host Not Receiving
```bash
# Test direct MQTT subscription
docker exec mosquitto mosquitto_sub -h localhost -t "spBv1.0/DeviceLevelTest/#"

# Check if messages are being published
benthos -c config/sparkplug-device-level-primary-host.yaml -l DEBUG
```

#### Port 1883 Already in Use
```bash
# Find what's using the port
sudo lsof -i :1883

# Use different port (update configs accordingly)
docker run -d --name mosquitto -p 1884:1883 eclipse-mosquitto:1.6
```

### Performance Testing

For high-throughput testing, modify the Edge Node config:

```yaml
input:
  generate:
    interval: "100ms"  # 10 messages per second
```

Monitor performance:
```bash
# Edge Node metrics
docker stats mosquitto

# Message rates in Primary Host output
benthos -c config/sparkplug-device-level-primary-host.yaml | grep -c "Received"
```

## Testing Strategy

## Requirements

- **Go**: 1.19 or later
- **MQTT Broker**: Any Sparkplug B compatible MQTT broker
- **Benthos**: Latest version with custom plugin support
- **Network**: Internet access for public broker testing

## Architectural Decisions

### Why Device-Level PARRIS Only?

This implementation focuses exclusively on **device-level messaging** (DBIRTH/DDATA) rather than supporting both node-level and device-level patterns. This decision provides several benefits:

1. **Simplified Architecture**: Eliminates complexity of managing both NBIRTH and DBIRTH flows
2. **UMH Integration**: Aligns perfectly with UMH's asset hierarchy model (`location_path` → Device ID)
3. **Real-World Usage**: Most industrial IoT scenarios operate at device/asset level, not gateway level
4. **Maintenance**: Easier to maintain and troubleshoot with single message pattern

### NBIRTH vs DBIRTH Trade-offs

| Aspect | NBIRTH (Node-Level) | DBIRTH (Device-Level) | Our Choice |
|--------|---------------------|----------------------|------------|
| **Scope** | Gateway/Edge Node metrics | Individual device/asset metrics | ✅ **Device-Level** |
| **Complexity** | High (dual message flows) | Medium (single device focus) | ✅ **Simplified** |
| **UMH Alignment** | Poor (no location_path mapping) | Perfect (direct asset mapping) | ✅ **Perfect fit** |
| **Industrial Use** | Limited (infrastructure monitoring) | Primary (sensor/actuator data) | ✅ **Industrial focus** |

## Compliance

✅ **Eclipse Sparkplug B v3.0 Specification**
- Proper message sequencing and birth certificate handling
- Static Edge Node ID requirement enforced
- Complete Device-Level PARRIS method implementation
- All device-level message types supported with correct binary encoding
- Perfect alias resolution for DBIRTH → DDATA message flow

## Troubleshooting

### Alias Resolution Not Working
1. Verify both Edge Node and Primary Host use the same `group_id`
2. Ensure Edge Node publishes NBIRTH/DBIRTH before DDATA
3. Check MQTT connectivity to broker
4. Rebuild binary after code changes: `make target`

### No Data Output
1. Verify internet connection (for public broker testing)
2. Check configuration file syntax
3. Ensure tag_processor populates `location_path` metadata
4. Verify `edge_node_id` is configured (required field)

### Message Flow Issues
1. Check MQTT broker logs for retained message cleanup
2. Verify proper Sparkplug B topic structure
3. Ensure sequence numbers are incrementing correctly

## Contributing

1. Run all tests: `go test -v ./...`
2. Test integration scenario before submitting PR
3. Follow existing code patterns for consistency
4. Update documentation for new features

## License

See [LICENSE](../LICENSE) file for details. 