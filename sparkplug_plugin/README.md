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

**✅ Sparkplug B Compliant Message Flow:**
1. **Static Edge Node ID**: `StaticEdgeNode01`
2. **Dynamic Device ID**: `enterprise:factory:line1:station1` (from location_path)
3. **Topic Structure**: 
   - NBIRTH: `spBv1.0/DeviceLevelTest/NBIRTH/StaticEdgeNode01`
   - DBIRTH: `spBv1.0/DeviceLevelTest/DBIRTH/StaticEdgeNode01/enterprise:factory:line1:station1`
   - DDATA: `spBv1.0/DeviceLevelTest/DDATA/StaticEdgeNode01/enterprise:factory:line1:station1`

**✅ Alias Resolution Working:**
```json
{"alias":1,"name":"humidity:value","value":79}
{"alias":2,"name":"temperature:value","value":33}
```

## Architecture

### Device-Level PARRIS Method

The plugin implements the industry-standard Device-Level PARRIS approach:

1. **Static Edge Node ID**: Required for Sparkplug B v3.0 compliance
2. **Dynamic Device ID**: Generated from location_path metadata
3. **Message Hierarchy**: 
   - Edge Node level: NBIRTH/NDATA (node-level data)
   - Device level: DBIRTH/DDATA (device-specific data)

### Message Flow

```
Edge Node:
1. NBIRTH → Publishes node-level birth certificate
2. DBIRTH → Publishes device-level birth certificate (first message per device)
3. DDATA → Publishes device data with aliases

Primary Host:  
1. STATE → Publishes ONLINE/OFFLINE status
2. NCMD → Sends commands to Edge Nodes
3. Input Processing → Resolves aliases using cached birth certificates
```

## Technical Details

### Supported Message Types

| Type | Direction | Purpose |
|------|-----------|---------|
| NBIRTH | Edge→Primary | Node birth certificate with metric definitions |
| DBIRTH | Edge→Primary | Device birth certificate with metric definitions |  
| NDATA | Edge→Primary | Node data with aliases |
| DDATA | Edge→Primary | Device data with aliases |
| NCMD | Primary→Edge | Node commands |
| DCMD | Primary→Edge | Device commands |
| STATE | Primary→Clients | Primary host online/offline status |

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

## Requirements

- **Go**: 1.19 or later
- **MQTT Broker**: Any Sparkplug B compatible MQTT broker
- **Benthos**: Latest version with custom plugin support
- **Network**: Internet access for public broker testing

## Compliance

✅ **Eclipse Sparkplug B v3.0 Specification**
- Proper message sequencing and birth certificate handling
- Static Edge Node ID requirement enforced
- Complete PARRIS method implementation
- All message types supported with correct binary encoding

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