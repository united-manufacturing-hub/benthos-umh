# Sparkplug B Configuration Examples

This directory contains example configurations for the Benthos Sparkplug B plugin in various scenarios.

## Configuration Files

### Primary Host Configurations (Input)

**`sparkplug-simple-test.yaml`** - Minimal test configuration
- Basic Sparkplug B Primary Host setup
- Connects to public broker (broker.hivemq.com) for testing
- Simple output to console
- Use this for initial testing and verification

**`sparkplug-primary-host-proper.yaml`** - Complete UMH integration
- Full-featured Primary Host configuration with tag_processor
- Includes UMH asset hierarchy mapping
- Converts Sparkplug B data to UMH format
- Ready for production use with UMH Unified Namespace

**`sparkplug-to-uns.yaml`** - Existing production configuration
- Production-ready configuration for UMH deployment
- Optimized for UMH-Core integration

### Edge Node Configurations (Output)

**`sparkplug-edge-node-test.yaml`** - Test Edge Node
- Generates sample data for testing
- Publishes as Sparkplug B Edge Node
- Use alongside Primary Host configs for complete testing

## Usage Examples

### Testing the Complete Flow

1. **Start a Primary Host** (in one terminal):
   ```bash
   ./tmp/bin/benthos -c config/sparkplug-simple-test.yaml
   ```

2. **Start an Edge Node** (in another terminal):
   ```bash
   ./tmp/bin/benthos -c config/sparkplug-edge-node-test.yaml
   ```

3. **Observe the data flow** - The Primary Host should receive and process the Edge Node's data

### Production Deployment

For production UMH deployment, use:
```bash
./tmp/bin/benthos -c config/sparkplug-primary-host-proper.yaml
```

This configuration includes:
- Proper alias resolution for Sparkplug B devices
- UMH asset hierarchy mapping via tag_processor
- Conversion to UMH Unified Namespace format

## Important Notes

### Alias Resolution Behavior

The Sparkplug B plugin correctly handles alias resolution according to the specification:

- **Both alias and name present**: Output shows `{"alias": 50000, "name": "Temperature", "value": 25.0}` - this is **correct behavior**
- **Only alias present**: Plugin resolves from cache to show `{"alias": 50000, "name": "Temperature", "value": 25.0}`
- **Unresolved aliases**: Show as `{"alias": 50000, "value": 25.0}` when BIRTH message hasn't been received yet

This is normal Sparkplug B behavior, not a bug.

### Broker Configuration

All examples use `broker.hivemq.com:1883` for testing. For production:

1. Update the `mqtt.urls` to your MQTT broker
2. Add authentication if required:
   ```yaml
   mqtt:
     urls: ["tcp://your-broker:1883"]
     username: "your-username"
     password: "your-password"
   ```
3. Add TLS configuration if needed:
   ```yaml
   mqtt:
     urls: ["ssl://your-broker:8883"]
     tls:
       ca_cert: "/path/to/ca.crt"
   ```

### Tag Processor Integration

The `sparkplug-primary-host-proper.yaml` shows how to integrate with UMH's tag_processor:

- Maps Sparkplug Group IDs to UMH enterprises
- Maps Edge Node IDs to UMH sites
- Handles both resolved names and aliases
- Cleans tag names for UMH compatibility
- Converts data types as needed

## Troubleshooting

### No Data Received
- Check broker connectivity and credentials
- Verify Group ID matches between Edge Nodes and Primary Host
- Ensure firewall allows MQTT traffic

### Alias Resolution Issues
- Verify BIRTH messages are being received (check logs)
- Ensure `drop_birth_messages: false` in Primary Host config
- Check that device keys match between BIRTH and DATA messages

### Performance Issues
- Use `auto_split_metrics: true` for better throughput
- Consider `data_messages_only: true` if you don't need initial values
- Monitor MQTT QoS settings and adjust as needed 