# Sparkplug B Plugin - Developer Guide

Developer documentation for the Sparkplug B Benthos plugin. For user documentation, see [docs/input/sparkplug-b-input.md](../docs/input/sparkplug-b-input.md) and [docs/output/sparkplug-b-output.md](../docs/output/sparkplug-b-output.md).

## Development Status

- ✅ **90/90 Unit Tests Passing**
- ✅ **Integration Tests Validated** 
- ✅ **Sparkplug B v3.0 Compliant**
- ✅ **Production Ready**

## Quick Development Setup

```bash
# Build the plugin binary
make target
cp tmp/bin/benthos ./benthos

# Run unit tests
cd sparkplug_plugin
go test -v

# Run integration tests  
make test-integration-local
```

## Testing

### Unit Tests

```bash
cd sparkplug_plugin
go test -v
```

**Expected Results**: 90/90 tests passing, including:
- Message type parsing and validation
- Alias resolution logic
- Sparkplug B specification compliance
- Configuration validation
- Error handling scenarios

### Integration Testing

#### Automated Integration Test

```bash
make test-integration-local
```

This automated script:
1. 🐳 Starts local Mosquitto MQTT broker
2. 🏭 Launches Edge Node publishing test data  
3. 🖥️ Starts Primary Host receiving messages
4. ✅ Validates end-to-end message flow
5. 🧹 Automatically cleans up resources

#### Manual Integration Test

For debugging or custom testing scenarios:

```bash
# Terminal 1: Start MQTT Broker
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:1.6

# Terminal 2: Primary Host (subscriber)
./benthos -c config/sparkplug-device-level-primary-host.yaml

# Terminal 3: Edge Node (publisher)  
./benthos -c config/sparkplug-device-level-test.yaml
```

**Expected Message Flow:**
```
Edge Node → DBIRTH → Primary Host (alias definitions cached)
Edge Node → DDATA  → Primary Host (aliases resolved to metric names)
Primary Host → STATE → Edge Node (host lifecycle)
```

#### Verification Steps

1. **Edge Node Output** should show:
   ```
   DBIRTH[DeviceID: enterprise:factory:line1:station1]: 2 metrics with aliases
   Published DDATA: {"alias":1,"name":"humidity:value","value":29}
   Published DDATA: {"alias":2,"name":"temperature:value","value":65}
   ```

2. **Primary Host Output** should show:
   ```
   STATE ONLINE: DeviceLevelTest/PrimaryHost
   DBIRTH cached 2 aliases for device: enterprise:factory:line1:station1
   DDATA resolved 2 aliases: humidity=29, temperature=65
   ```

3. **Perfect Alias Resolution**: Aliases defined in DBIRTH are correctly resolved in DDATA messages

#### Cleanup

```bash
# Stop Benthos processes (Ctrl+C)
docker stop mosquitto && docker rm mosquitto
```

### Performance Testing

Test high-throughput scenarios:

```bash
# Modify config for high frequency
# input.generate.interval: "100ms"  # 10 messages/second

# Monitor performance
docker stats mosquitto
benthos -c config/sparkplug-device-level-primary-host.yaml | grep -c "Received"
```

## Development

### Code Structure

```
sparkplug_plugin/
├── config.go              # Configuration structures
├── sparkplug_b_input.go    # Input plugin implementation  
├── sparkplug_b_output.go   # Output plugin implementation
├── sparkplug_b_core.go     # Core Sparkplug B logic
├── *_test.go              # Unit tests
├── integration_test.go     # Integration test suite
└── test_vectors.go        # Test data generation
```

### Key Implementation Details

- **Device-Level PARRIS**: Exclusive focus on DBIRTH/DDATA (no NBIRTH/NDATA)
- **UMH Integration**: `location_path` metadata → Sparkplug Device ID conversion
- **Alias Management**: Automatic alias assignment and resolution
- **Specification Compliance**: Sparkplug B v3.0 compliant STATE topics

### Testing Configuration Files

- `config/sparkplug-device-level-test.yaml`: Edge Node (publisher)
- `config/sparkplug-device-level-primary-host.yaml`: Primary Host (subscriber)

## Troubleshooting Development Issues

### Unit Tests Failing

```bash
# Check Go version (requires 1.19+)
go version

# Clean and rebuild
go clean -cache
go mod tidy
go test -v
```

### Integration Test Issues

```bash
# Check if port 1883 is available
sudo lsof -i :1883

# Test MQTT broker connectivity
docker exec mosquitto mosquitto_pub -h localhost -t test -m "hello"

# Debug with verbose logging
benthos -c config/sparkplug-device-level-test.yaml -l DEBUG
```

### Alias Resolution Not Working

1. Verify DBIRTH is published before DDATA
2. Check that both nodes use same `group_id`
3. Ensure proper topic structure: `spBv1.0/{group}/{type}/{edge_node}/{device}`
4. Validate protobuf encoding/decoding

### Build Issues

```bash
# Ensure dependencies are current
go mod tidy

# Rebuild from scratch
make clean
make target

# Check plugin registration
./benthos list inputs | grep sparkplug
./benthos list outputs | grep sparkplug
```

## Contributing

### Before Submitting PRs

1. **Run all tests**: `go test -v ./...`
2. **Test integration**: `make test-integration-local`
3. **Update documentation**: If adding features, update user docs
4. **Follow patterns**: Match existing code style and structure
5. **Test edge cases**: Verify error handling and boundary conditions

### Code Review Checklist

- [ ] Unit tests cover new functionality
- [ ] Integration tests pass
- [ ] No breaking changes to existing APIs
- [ ] Documentation updated if needed
- [ ] Error handling implemented
- [ ] Logging follows existing patterns

### Testing New Features

```bash
# Run specific test suites
go test -v -run TestSparkplugInput
go test -v -run TestSparkplugOutput
go test -v -run TestAliasResolution

# Test with custom configs
benthos -c your-custom-config.yaml -l DEBUG
```

## Architecture Notes

### Why Device-Level Only?

This implementation focuses exclusively on device-level messaging (DBIRTH/DDATA) rather than node-level (NBIRTH/NDATA):

- **Simplified Architecture**: Single message pattern vs dual patterns
- **UMH Alignment**: Perfect mapping of `location_path` → Device ID
- **Industrial Focus**: Most IoT scenarios operate at device/asset level
- **Maintenance**: Easier to debug and maintain

### Sparkplug B Compliance

- ✅ **Static Edge Node ID**: Required for v3.0 compliance
- ✅ **STATE Topics**: `spBv1.0/STATE/<host_id>` (no group_id)
- ✅ **Sequence Management**: Proper wraparound handling
- ✅ **Birth Certificates**: Complete metric definitions with aliases
- ✅ **Retained Messages**: Proper MQTT retention for DBIRTH

## License

See [LICENSE](../LICENSE) file for details. 