# UMH Topic Parser

The UMH Topic Parser is a high-performance Go package that provides parsing, validation, and construction utilities for UMH (Unified Manufacturing Hub) topics. It ensures that all topics conform to the UMH specification and can be safely used as Kafka message keys.

## Overview

UMH topics follow a strict hierarchical structure that represents the physical and logical organization of manufacturing systems. This package centralizes topic handling across all UMH components, providing consistent validation and preventing bad data from entering the UNS.

### Topic Structure

UMH topics follow this format:

```
umh.v1.<location_path>.<data_contract>[.<virtual_path>].<name>
```

**Components:**
- **location_path**: 1-N hierarchical levels representing physical organization (enterprise.site.area.line...)
- **data_contract**: Service/schema identifier starting with underscore (_historian, _analytics, etc.)
- **virtual_path**: Optional logical grouping (axis.x.position, diagnostics.*, etc.)
- **name**: Final identifier for the specific data point

### Validation Rules

The package enforces these fixed rules:
- **Location path levels**: MUST NOT start with underscore, at least 1 level required
- **Data contract**: MUST start with underscore, cannot be just "_"
- **Virtual path**: CAN start with underscore, optional
- **Name**: CAN start with underscore, required
- **Kafka compatibility**: Only `[a-zA-Z0-9._-]` characters allowed
- **No consecutive dots, leading dots, or trailing dots**

## Examples

### Valid Topics

```
umh.v1.enterprise._historian.temperature
umh.v1.acme.berlin._historian.pressure
umh.v1.factory.line1.station2._raw.motor.diagnostics.vibration
umh.v1.plant._analytics.efficiency._kpi.oee
```

### Invalid Topics

```
umh.v1._enterprise._historian.temp     // location cannot start with _
umh.v1.factory.historian.temp          // data contract must start with _
umh.v1.factory._._historian.temp       // data contract cannot be just _
umh.v1.factory.._historian.temp        // empty location level
umh.v1.factory._historian.temp@ture    // invalid characters
```

## Usage

### Basic Parsing and Validation

```go
import "github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"

// Parse and validate a topic string
uns, err := topic.NewUnsTopic("umh.v1.enterprise.site._historian.temperature")
if err != nil {
    log.Fatal(err)
}

// Use the topic as a Kafka key
kafkaKey := uns.AsKafkaKey()

// Access parsed components
info := uns.Info()
fmt.Println("Location:", info.LocationPath())        // "enterprise.site"
fmt.Println("Data Contract:", info.DataContract)     // "_historian"
fmt.Println("Name:", info.Name)                      // "temperature"
```

### Programmatic Construction

```go
// Build a simple topic
topic, err := topic.NewBuilder().
    SetLevel0("enterprise").
    SetDataContract("_historian").
    SetName("temperature").
    Build()

// Build a complex topic with all components
complexTopic, err := topic.NewBuilder().
    SetLocationLevels("enterprise", "site", "area", "line").
    SetDataContract("_historian").
    SetVirtualPath("motor.axis.x").
    SetName("position").
    Build()
```

### High-Performance Scenarios

For high-throughput scenarios, reuse builder instances:

```go
builder := topic.NewBuilder()

for _, sensor := range sensors {
    topic, err := builder.Reset().
        SetLocationPath(sensor.Location).
        SetDataContract("_historian").
        SetName(sensor.Name).
        Build()
    
    if err != nil {
        log.Printf("Invalid topic for sensor %s: %v", sensor.Name, err)
        continue
    }
    
    // Process topic...
}
```

### Location Path Handling

```go
// Different ways to set location paths
builder := topic.NewBuilder()

// Method 1: Dot-separated string
builder.SetLocationPath("enterprise.site.area")

// Method 2: Individual levels
builder.SetLocationLevels("enterprise", "site", "area")

// Method 3: Incremental building
builder.SetLevel0("enterprise").
    AddLocationLevel("site").
    AddLocationLevel("area")
```

## Performance Characteristics

The package is optimized for high-throughput scenarios:

- **Topic parsing**: ~625ns per operation
- **Topic construction**: ~900ns per operation  
- **Memory allocations**: 3-5 allocations per topic creation
- **Read operations**: <1ns (String(), Info(), AsKafkaKey() are essentially free)

### Thread Safety

- **UnsTopic instances**: Immutable and safe for concurrent use
- **Builder instances**: NOT thread-safe (use separate builders per goroutine)
- **Package-level functions**: Thread-safe

## Integration Patterns

### Message Processing

```go
func processMessage(topicStr string, payload []byte) error {
    topic, err := topic.NewUnsTopic(topicStr)
    if err != nil {
        return fmt.Errorf("invalid topic: %w", err)
    }

    // Use topic for routing, validation, etc.
    return routeMessage(topic, payload)
}
```

### Location-Based Routing

```go
func routeByLocation(topic *topic.UnsTopic) string {
    info := topic.Info()
    switch info.TotalLocationLevels() {
    case 1:
        return "enterprise-router"
    case 2:
        return "site-router"
    default:
        return "local-router"
    }
}
```

### Bulk Topic Generation

```go
func createTopics(sensors []Sensor) ([]*topic.UnsTopic, error) {
    builder := topic.NewBuilder()
    topics := make([]*topic.UnsTopic, 0, len(sensors))

    for _, sensor := range sensors {
        topic, err := builder.Reset().
            SetLocationPath(sensor.LocationPath).
            SetDataContract("_historian").
            SetName(sensor.Name).
            Build()
        if err != nil {
            return nil, err
        }
        topics = append(topics, topic)
    }

    return topics, nil
}
```

## Error Handling

The package provides detailed error messages for debugging:

```go
topic, err := topic.NewUnsTopic("umh.v1._enterprise._historian.temp")
if err != nil {
    fmt.Println(err) // "level0 (enterprise) cannot start with underscore"
}
```

Error messages include:
- Specific validation rule that failed
- Position information for parsing errors
- Suggestions for common mistakes

## Migration Guide

### From Manual String Parsing

**Before:**
```go
parts := strings.Split(topicStr, ".")
if len(parts) < 5 || !strings.HasPrefix(parts[0], "umh") {
    return errors.New("invalid topic")
}
// ... manual validation logic
level0 := parts[2]
dataContract := parts[3] // This might be wrong!
```

**After:**
```go
topic, err := topic.NewUnsTopic(topicStr)
if err != nil {
    return err
}
info := topic.Info()
level0 := info.Level0
dataContract := info.DataContract // Correctly parsed
```

### From Existing Validation Code

The topic parser replaces manual validation logic throughout the codebase:

1. **topic_browser_plugin**: Replace `topicToUNSInfo()` and manual parsing
2. **uns_output**: Replace manual topic validation 
3. **tag_processor_plugin**: Replace `constructUMHTopic()` with Builder
4. **classic_to_core_plugin**: Replace manual topic parsing

## API Reference

### Core Types

```go
// UnsTopic represents a validated UMH topic
type UnsTopic struct { /* ... */ }

// TopicInfo contains parsed topic components
type TopicInfo struct {
    Level0            string   // Enterprise level
    LocationSublevels []string // Additional location levels
    DataContract      string   // Data contract (_historian, etc.)
    VirtualPath       *string  // Optional virtual path
    Name              string   // Final name segment
}

// Builder provides fluent topic construction
type Builder struct { /* ... */ }
```

### Key Methods

```go
// Parsing and validation
func NewUnsTopic(topic string) (*UnsTopic, error)

// Topic access
func (u *UnsTopic) String() string
func (u *UnsTopic) AsKafkaKey() string  
func (u *UnsTopic) Info() *TopicInfo

// Location methods
func (t *TopicInfo) LocationPath() string
func (t *TopicInfo) TotalLocationLevels() int

// Builder methods
func NewBuilder() *Builder
func (b *Builder) SetLevel0(level0 string) *Builder
func (b *Builder) SetLocationPath(path string) *Builder
func (b *Builder) SetLocationLevels(level0 string, additional ...string) *Builder
func (b *Builder) AddLocationLevel(level string) *Builder
func (b *Builder) SetDataContract(contract string) *Builder
func (b *Builder) SetVirtualPath(path string) *Builder
func (b *Builder) SetName(name string) *Builder
func (b *Builder) Build() (*UnsTopic, error)
func (b *Builder) BuildString() (string, error)
func (b *Builder) Reset() *Builder
```

## Testing

The package includes comprehensive test coverage:

- **100+ test cases** covering valid and invalid topics
- **Performance benchmarks** for all operations
- **Concurrency tests** for thread safety
- **Edge case validation** for boundary conditions

Run tests:
```bash
cd pkg/umh/topic
go test -v
go test -bench=. -benchmem
``` 