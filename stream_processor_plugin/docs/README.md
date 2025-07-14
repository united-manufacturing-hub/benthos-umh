# Stream Processor Plugin

The Stream Processor Plugin transforms incoming UMH timeseries messages using configurable JavaScript expressions and variable mappings.

## Architecture Overview

The plugin follows a modular architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Stream Processor Plugin                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Processor     │  │  State Manager  │  │   JS Engine     │ │
│  │  (Main Logic)   │  │  (Variables)    │  │  (Expressions)  │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│           │                     │                     │         │
│           │                     │                     │         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Metrics       │  │   Object Pools  │  │   Security      │ │
│  │  (Monitoring)   │  │  (Performance)  │  │  (JS Sandbox)   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. **Processor** (`processor.go`)
- **Purpose**: Main processing logic and message orchestration
- **Key Features**:
  - Message validation and parsing
  - Variable resolution from topics
  - Static and dynamic mapping coordination
  - Output message generation
- **Entry Point**: `ProcessBatch()` method

### 2. **State Manager** (`state.go`)
- **Purpose**: Thread-safe variable storage and dependency tracking
- **Key Features**:
  - Lock-free variable reads with atomic operations
  - Copy-on-write updates for thread safety
  - Dependency-based mapping evaluation
  - Topic-to-variable resolution
- **Memory Note**: [[memory:2882218]] - Sequential processing was chosen over worker pools for better performance

### 3. **JavaScript Engine** (`js_engine.go`)
- **Purpose**: Secure JavaScript expression evaluation
- **Key Features**:
  - Goja runtime with security restrictions
  - Expression compilation and caching
  - Static vs dynamic expression handling
  - Timeout protection for infinite loops

### 4. **Object Pools** (`pools.go`)
- **Purpose**: Memory allocation optimization
- **Key Features**:
  - Reusable objects for high-frequency operations
  - Reduced garbage collection pressure
  - Thread-safe pool management
  - Topic caching for performance

### 5. **Configuration** (`config.go`)
- **Purpose**: Configuration parsing and validation
- **Key Features**:
  - YAML/JSON configuration support
  - Source mapping validation
  - Model specification handling

### 6. **Metrics** (`metrics.go`)
- **Purpose**: Observability and monitoring
- **Key Features**:
  - Processing counters and timers
  - JavaScript execution metrics
  - Resource utilization gauges
  - Error tracking

## Data Flow

The plugin processes messages through the following stages:

1. **Input Validation**: Checks for required `umh_topic` metadata
2. **Message Parsing**: Validates UMH timeseries format
3. **Variable Resolution**: Maps topics to configured variables
4. **State Update**: Updates variable values in thread-safe state
5. **Mapping Evaluation**: Processes static and dynamic mappings
6. **Output Generation**: Creates UMH-compliant output messages

## Testing Strategy

The plugin includes comprehensive testing:

- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end message processing
- **Benchmark Tests**: Performance validation
- **Fuzz Tests**: Input validation and edge case handling
- **Security Tests**: JavaScript sandbox validation

## Features

- **UNS Message Processing**: Validates and processes UMH timeseries format messages
- **Variable Resolution**: Maps incoming topics to configured source variables
- **JavaScript Expression Evaluation**: Supports dynamic calculations using Goja runtime
- **Static & Dynamic Mappings**: Efficiently handles both constant values and variable-dependent calculations
- **Metadata Preservation**: Automatically preserves message metadata from input to output messages
- **Error Handling**: Fail-open policy ensures processing continues despite individual mapping failures

## Metadata Preservation

The plugin automatically preserves all metadata from the original input message to the output messages, with the following behavior:

- **`umh_topic`**: Overridden with the new output topic format: `<output_topic>.<data_contract>.<virtual_path>`
- **All other metadata**: Preserved as-is from the original message

This ensures that important context such as correlation IDs, trace IDs, source system information, and routing metadata flows through the processing pipeline.

### Example

Input message metadata:
```
umh_topic: "umh.v1.corpA.plant-A.aawd._raw.press"
correlation_id: "test-correlation-123"
source_system: "test-system"
priority: "high"
```

Output message metadata:
```
umh_topic: "umh.v1.corpA.plant-A.aawd._pump_v1.pressure"
correlation_id: "test-correlation-123"
source_system: "test-system"
priority: "high"
```

## Configuration

The plugin supports timeseries mode with the following configuration structure:

```yaml
mode: "timeseries"
output_topic: "umh.v1.corpA.plant-A.aawd"
model:
  name: "pump"
  version: "v1"
sources:
  press: "umh.v1.corpA.plant-A.aawd._raw.press"
  tF: "umh.v1.corpA.plant-A.aawd._raw.tempF"
  run: "umh.v1.corpA.plant-A.aawd._raw.run"
mapping:
  pressure: "press + 4.00001"
  temperature: "tF * 9 / 5 + 32"
  motor:
    rpm: "press / 4"
  serialNumber: '"SN-P42-008"'
  status: 'run ? "active" : "inactive"'
```

## Performance Considerations

- **Memory Efficiency**: Object pooling reduces allocations
- **Thread Safety**: Lock-free reads with atomic operations
- **Expression Compilation**: Pre-compiled JavaScript for performance
- **Dependency Tracking**: Only re-evaluate mappings when dependencies change
- **Sequential Processing**: Simpler and more efficient than parallel worker pools

## Security Features

- **JavaScript Sandbox**: Restricted runtime environment
- **Expression Validation**: Static analysis for dangerous patterns
- **Timeout Protection**: Prevents infinite loops
- **Input Sanitization**: Validates and sanitizes all inputs

## Monitoring

The plugin exposes metrics for:
- Message processing rates and errors
- JavaScript execution time and failures
- Variable state and mapping counts
- Memory usage and performance

## Contributing

When making changes to the plugin:

1. **Run Tests**: Ensure all tests pass including fuzz tests
2. **Update Documentation**: Keep README and comments current
3. **Monitor Performance**: Use benchmark tests for performance-critical changes
4. **Follow Patterns**: Maintain consistent code organization and patterns 