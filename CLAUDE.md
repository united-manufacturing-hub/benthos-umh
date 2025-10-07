# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the benthos-umh repository.

## Repository Purpose

benthos-umh is a specialized extension of Benthos (Redpanda Connect) tailored for the manufacturing industry. It serves as the stream processing engine for the United Manufacturing Hub, bridging industrial protocols with the UNS (Unified Namespace) through YAML-defined data pipelines.

## Documentation Structure

- `docs/input/` - Input connectors documentation
  - OPC UA, Modbus, S7, Sparkplug B, Ethernet/IP, sensorconnect
- `docs/output/` - Output connectors documentation
  - UNS, OPC UA, Sparkplug B
- `docs/processing/` - Processing pipelines documentation
  - Tag processor, downsampler, stream processor, topic browser
- `docs/libraries/` - Shared libraries documentation
  - UMH topic parser
- `docs/testing/` - Testing approach and examples
- Full documentation: [docs.umh.app/benthos-umh](https://docs.umh.app/benthos-umh)

## Plugin Architecture

benthos-umh extends Benthos with 13 specialized plugins:

### Input Plugins
- `opcua_plugin/` - OPC UA protocol input/output
- `modbus_plugin/` - Modbus TCP/RTU protocol
- `s7comm_plugin/` - Siemens S7 protocol
- `sparkplug_plugin/` - Sparkplug B MQTT protocol
- `eip_plugin/` - Ethernet/IP protocol
- `sensorconnect_plugin/` - ifm IO-Link Master

### Processing Plugins
- `tag_processor_plugin/` - Individual sensor data processing
- `stream_processor_plugin/` - Multi-source data aggregation
- `downsampler_plugin/` - Data reduction and aggregation
- `topic_browser_plugin/` - Topic discovery and metadata
- `classic_to_core_plugin/` - UMH Classic to Core migration
- `nodered_js_plugin/` - Node-RED JavaScript execution

### Output Plugin
- `uns_plugin/` - Unified Namespace output with validation

## Key Concepts

- **Data Flow Component (DFC)**: YAML-defined data pipeline configuration
- **Protocol Converter**: Bridge between industrial protocols and UNS
- **Stream Processor**: Aggregates multiple data sources into unified streams
- **Tag Processor**: Handles individual sensor/tag data points
- **UNS (Unified Namespace)**: Central data structure following ISA-95 standard
- **Topic Browser**: Discovers and catalogs available data topics
- **Downsampler**: Reduces data volume while preserving important changes

## Development Guidelines

### General Preferences
- Be terse and code-focused - provide answers immediately, explanations after
- Treat developer as expert - no basic explanations needed
- Give actual code, not high-level descriptions
- Respect existing code comments unless clearly irrelevant
- No unnecessary preambles, disclaimers, or AI identification
- Value arguments over authorities - source doesn't matter, correctness does
- Consider new technologies and contrarian approaches

### Golang Specific
- **Testing Framework**: Ginkgo v2 with Gomega matchers
- **Go Version Features**: Use newer Go methods when applicable
  - go1.21+: min/max/clear builtins, slices/maps packages
  - go1.22+: range over integers, math/rand/v2
  - go1.23+: iterator functions, unique package
- **Testing Rules**:
  - Do not remove focused tests (`FIt`, `FDescribe`)
  - Maintain existing documentation
  - Tests use `_test.go` and `_suite_test.go` pattern

## Build & Test Commands

### Core Commands
```bash
make all          # Clean and build
make target       # Build benthos binary
make clean        # Remove build artifacts
make test         # Run all tests with Ginkgo
```

### Plugin-Specific Tests
```bash
make test-opcua              # OPC UA plugin tests
make test-modbus             # Modbus plugin tests
make test-s7                 # S7 protocol tests
make test-sparkplug          # Sparkplug B tests
make test-eip                # Ethernet/IP tests
make test-sensorconnect      # Sensorconnect tests
make test-stream-processor   # Stream processor tests
make test-tag-processor      # Tag processor tests
make test-topic-browser      # Topic browser tests
make test-downsampler        # Downsampler tests
make test-classic-to-core    # Migration plugin tests
make test-uns                # UNS output tests
```

### Utility Commands
```bash
make license-check    # Verify Apache 2.0 headers
make license-fix      # Add missing license headers
make setup-test-deps  # Install test dependencies
make serve-pprof      # Run profiling server
```

## Testing Approach

- **Unit Tests**: Per-plugin using Ginkgo v2
  - Focus on business logic validation
  - Mock external dependencies
- **Integration Tests**: Using Docker containers
  - Test real protocol connections
  - Validate end-to-end data flow
- **YAML Validation**: Schema-based config testing
- **Benchmarks**: Performance testing for critical paths
  - `make bench-stream-processor`
  - `make bench-pkg-umh-topic`

### Test Patterns
```go
var _ = Describe("PluginName", func() {
    Context("when processing data", func() {
        It("should handle valid input", func() {
            // Test implementation
            Expect(result).To(Equal(expected))
        })
    })
})
```

## Common Tasks

### Adding a New Protocol
1. Create new plugin directory: `protocol_plugin/`
2. Implement input/output interfaces
3. Add documentation in `docs/input/` or `docs/output/`
4. Create tests with `_suite_test.go` and `_test.go`
5. Add Make target for testing
6. Update this CLAUDE.md

### Debugging Data Flows
1. Enable debug logging in configuration
2. Use `make serve-pprof` for performance profiling
3. Check plugin-specific logs for protocol details
4. Test with minimal config first
5. Use example configurations from `config/` directory

## Project Structure

```
benthos-umh/
├── cmd/benthos/          # Main application entry
├── config/               # Configuration examples
├── docs/                 # Documentation (GitBook format)
├── pkg/                  # Shared packages
│   └── umhtopic/         # UMH topic parsing
├── *_plugin/             # Protocol/processor plugins
├── Makefile              # Build automation
└── Makefile.Common       # Shared Make configuration
```

## Important Notes

- Default branch is `master` (not `main` or `staging`)
- Apache 2.0 license headers required on all source files
- Ginkgo runs tests in parallel with randomization
- Plugins are loaded dynamically at runtime
- Configuration uses standard Benthos YAML format
- Each plugin maintains its own documentation

## UX Standards

See `UX_STANDARDS.md` for UI/UX principles when building management interfaces or user-facing components.