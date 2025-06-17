# E2E Test Coverage Analysis & Test Plan

## Current Test Coverage Analysis

### ✅ Currently Covered
1. **Basic Message Processing**: Single message with metadata extraction
2. **Batch Processing**: Multiple messages in one batch
3. **UNS Map Caching**: Basic caching across invocations
4. **Protobuf Serialization**: Round-trip encoding/decoding verification
5. **Message Structure Validation**: Topic hierarchy parsing

### ❌ Critical E2E Test Gaps

## 1. **Emit Rate Limiting / Timing Edge Cases**

### **Issue**: Current tests use 1ms emit interval (unrealistic)
- Tests immediately emit on every ProcessBatch call
- No verification of actual rate limiting behavior 
- Missing realistic timing scenarios

### **Required E2E Tests**:

#### Test: `"respects emit interval timing (realistic 1s interval)"`
```go
It("respects emit interval timing with realistic intervals", func() {
    // Use realistic 1s emit interval 
    processor := NewTopicBrowserProcessor(nil, nil, 0, time.Second, 10, 10000)
    
    // Process first message
    msg1 := createTestMessage("value", 1, 1647753600000)
    result1, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
    
    // Should buffer (no emission yet)
    Expect(err).To(BeNil())
    Expect(result1).To(BeNil()) // No emission, still buffering
    
    // Process second message immediately 
    msg2 := createTestMessage("value", 2, 1647753600500)
    result2, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
    
    // Still buffering (< 1s elapsed)
    Expect(err).To(BeNil())
    Expect(result2).To(BeNil())
    
    // Wait for emit interval to elapse
    time.Sleep(1100 * time.Millisecond)
    
    // Process third message - should trigger emission of all 3 messages
    msg3 := createTestMessage("value", 3, 1647753601000)
    result3, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg3})
    
    // Should emit (interval elapsed)
    Expect(err).To(BeNil())
    Expect(result3).To(HaveLen(2)) // [emission_batch, ack_batch]
    Expect(result3[0]).To(HaveLen(1)) // 1 emission message
    Expect(result3[1]).To(HaveLen(3)) // ACK all 3 buffered messages
    
    // Verify emission contains all 3 events
    bundle := decodeEmissionMessage(result3[0][0])
    Expect(bundle.Events.Entries).To(HaveLen(3))
})
```

#### Test: `"message-driven emission (no timer heartbeats)"`
```go
It("only emits when messages arrive (no timer heartbeats)", func() {
    processor := NewTopicBrowserProcessor(nil, nil, 0, 100*time.Millisecond, 10, 10000)
    
    // Process one message 
    msg1 := createTestMessage("value", 1, 1647753600000)
    result1, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
    Expect(result1).To(BeNil()) // Buffering
    
    // Wait longer than emit interval but don't send new messages
    time.Sleep(200 * time.Millisecond)
    
    // No automatic emission should occur (message-driven, not timer-driven)
    // This is the critical behavior: no background timers
    
    // Only when next message arrives should emission happen
    msg2 := createTestMessage("value", 2, 1647753600100)
    result2, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
    
    Expect(err).To(BeNil())
    Expect(result2).To(HaveLen(2)) // Now emission occurs
    Expect(result2[1]).To(HaveLen(2)) // Both messages ACKed
})
```

## 2. **Ring Buffer Overflow Edge Cases**

### **Issue**: No tests for ring buffer overflow behavior
- No verification of `eventsOverwritten` metric
- No validation of FIFO behavior when buffer is full
- Missing high-traffic scenarios

### **Required E2E Tests**:

#### Test: `"ring buffer overflow with FIFO behavior"`
```go
It("handles ring buffer overflow with FIFO behavior", func() {
    // Small ring buffer (3 events max per topic)
    processor := NewTopicBrowserProcessor(nil, nil, 0, time.Millisecond, 3, 10000)
    
    topic := "umh.v1.test-topic._historian.overflow_test" 
    
    // Send 5 messages (exceeds buffer capacity of 3)
    for i := 1; i <= 5; i++ {
        msg := service.NewMessage(nil)
        msg.MetaSet("umh_topic", topic)
        msg.SetStructured(map[string]interface{}{
            "timestamp_ms": int64(1647753600000 + i*1000),
            "value":        i,
        })
        
        _, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
        Expect(err).To(BeNil())
    }
    
    // Verify eventsOverwritten metric incremented 
    Expect(processor.eventsOverwritten.Get()).To(Equal(int64(2))) // 2 events overwritten
    
    // Verify only latest 3 events in emission (FIFO behavior)
    // Should contain events 3, 4, 5 (oldest 1, 2 discarded)
    bundle := decodeLastEmission(processor)
    Expect(bundle.Events.Entries).To(HaveLen(3))
    
    // Verify chronological order (oldest to newest of remaining events)
    Expect(bundle.Events.Entries[0].GetTs().GetNumericValue().GetValue()).To(Equal(float64(3)))
    Expect(bundle.Events.Entries[1].GetTs().GetNumericValue().GetValue()).To(Equal(float64(4)))
    Expect(bundle.Events.Entries[2].GetTs().GetNumericValue().GetValue()).To(Equal(float64(5)))
})
```

#### Test: `"multiple topics with independent ring buffers"`
```go
It("maintains independent ring buffers per topic", func() {
    processor := NewTopicBrowserProcessor(nil, nil, 0, time.Millisecond, 2, 10000)
    
    topic1 := "umh.v1.test-topic._historian.sensor1"
    topic2 := "umh.v1.test-topic._historian.sensor2"
    
    // Fill topic1 buffer (2 events max)
    for i := 1; i <= 3; i++ { // 3 > 2, so overflow
        msg := createTestMessageForTopic(topic1, i, 1647753600000+int64(i*1000))
        processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
    }
    
    // Fill topic2 buffer (independent of topic1)
    for i := 10; i <= 12; i++ { // 3 > 2, so overflow
        msg := createTestMessageForTopic(topic2, i, 1647753600000+int64(i*1000))
        processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
    }
    
    // Verify independent overflow counters
    Expect(processor.eventsOverwritten.Get()).To(Equal(int64(2))) // 1 per topic
    
    // Verify emission contains latest from both topics
    bundle := decodeLastEmission(processor) 
    Expect(bundle.Events.Entries).To(HaveLen(4)) // 2 from each topic
    
    // Verify topic1 has events [2,3] and topic2 has events [11,12]
    topic1Events := filterEventsByTopic(bundle.Events.Entries, topic1)
    topic2Events := filterEventsByTopic(bundle.Events.Entries, topic2) 
    
    Expect(topic1Events).To(HaveLen(2))
    Expect(topic2Events).To(HaveLen(2))
})
```

## 3. **Buffer Size Safety Limits**

### **Issue**: No tests for `maxBufferSize` safety limit 
- No verification of "buffer full - dropping message" error
- Missing backpressure scenarios

### **Required E2E Tests**:

#### Test: `"respects maxBufferSize safety limit"`
```go
It("enforces maxBufferSize safety limit", func() {
    // Very small buffer size for testing
    processor := NewTopicBrowserProcessor(nil, nil, 0, time.Hour, 10, 3) // maxBufferSize=3
    
    // Send 5 messages (exceeds maxBufferSize of 3)
    var lastErr error
    for i := 1; i <= 5; i++ {
        msg := createTestMessage("value", i, 1647753600000+int64(i*1000))
        _, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
        if err != nil {
            lastErr = err
        }
    }
    
    // Should get "buffer full" error for messages 4 and 5
    Expect(lastErr).To(MatchError(ContainSubstring("buffer full - dropping message")))
    
    // Verify failed message metrics
    Expect(processor.messagesFailed.Get()).To(BeGreaterThan(int64(0)))
})
```

## 4. **Delayed ACK Pattern Edge Cases**

### **Issue**: No verification of ACK timing behavior
- No tests ensuring messages aren't ACKed before emission
- Missing ACK failure scenarios

### **Required E2E Tests**:

#### Test: `"delayed ACK pattern - no early ACK"`
```go 
It("implements delayed ACK pattern correctly", func() {
    processor := NewTopicBrowserProcessor(nil, nil, 0, time.Second, 10, 10000)
    
    // Process message
    msg := createTestMessage("value", 1, 1647753600000)
    result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
    
    // No immediate ACK (delayed pattern)
    Expect(err).To(BeNil())
    Expect(result).To(BeNil()) // No ACK batch returned
    
    // Wait for emit interval + send trigger message
    time.Sleep(1100 * time.Millisecond)
    triggerMsg := createTestMessage("value", 2, 1647753601000)
    result2, err := processor.ProcessBatch(context.Background(), service.MessageBatch{triggerMsg})
    
    // Now ACK should occur 
    Expect(err).To(BeNil())
    Expect(result2).To(HaveLen(2)) // [emission, ack_batch]
    Expect(result2[1]).To(HaveLen(2)) // Both messages ACKed together
})
```

## 5. **Metadata Persistence Edge Cases**

### **Issue**: No tests for metadata accumulation across time
- No verification of cumulative header merging
- Missing LRU cache eviction scenarios

### **Required E2E Tests**:

#### Test: `"cumulative metadata persistence across emissions"`
```go
It("accumulates metadata across multiple emissions", func() {
    processor := NewTopicBrowserProcessor(nil, nil, 2, time.Millisecond, 10, 10000) // Small LRU cache
    
    topic := "umh.v1.test-topic._historian.metadata_test"
    
    // First message with header A 
    msg1 := service.NewMessage(nil)
    msg1.MetaSet("umh_topic", topic)
    msg1.MetaSet("header_a", "value_a") 
    msg1.SetStructured(map[string]interface{}{"timestamp_ms": 1647753600000, "value": 1})
    
    result1, _ := processor.ProcessBatch(context.Background(), service.MessageBatch{msg1})
    bundle1 := decodeEmissionMessage(result1[0][0])
    
    // Second emission with header B (header A should persist)
    time.Sleep(2 * time.Millisecond) // Ensure new emission interval
    
    msg2 := service.NewMessage(nil)
    msg2.MetaSet("umh_topic", topic)
    msg2.MetaSet("header_b", "value_b")
    msg2.SetStructured(map[string]interface{}{"timestamp_ms": 1647753600001, "value": 2})
    
    result2, _ := processor.ProcessBatch(context.Background(), service.MessageBatch{msg2})
    bundle2 := decodeEmissionMessage(result2[0][0])
    
    // Verify cumulative metadata (both headers present)
    topicInfo := getTopicInfoFromBundle(bundle2, topic)
    Expect(topicInfo.Metadata).To(HaveKeyWithValue("header_a", "value_a"))
    Expect(topicInfo.Metadata).To(HaveKeyWithValue("header_b", "value_b"))
})
```

## 6. **Graceful Shutdown Edge Cases**

### **Issue**: No tests for shutdown behavior
- No verification of flush during Close()
- Missing data loss prevention tests

### **Required E2E Tests**:

#### Test: `"graceful shutdown flushes buffered messages"`
```go
It("flushes buffered messages during graceful shutdown", func() {
    processor := NewTopicBrowserProcessor(nil, nil, 0, time.Hour, 10, 10000) // Long interval
    
    // Buffer messages without emission
    msg := createTestMessage("value", 1, 1647753600000)
    result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
    Expect(result).To(BeNil()) // Still buffering
    
    // Trigger graceful shutdown
    err = processor.Close(context.Background())
    Expect(err).To(BeNil())
    
    // Verify buffered data was flushed (logged, not returned)
    // This test would require capturing log output or exposing flush result
})
```

## 7. **PayloadFormat Field Usage**

### **Issue**: No tests for PayloadFormat field that was recently added

### **Required E2E Tests**:

#### Test: `"sets PayloadFormat correctly for timeseries vs relational data"`
```go
It("sets PayloadFormat field correctly", func() {
    processor := NewTopicBrowserProcessor(nil, nil, 0, time.Millisecond, 10, 10000)
    
    // Timeseries message
    tsMsg := service.NewMessage(nil)
    tsMsg.MetaSet("umh_topic", "umh.v1.test-topic._historian.sensor") 
    tsMsg.SetStructured(map[string]interface{}{
        "timestamp_ms": 1647753600000,
        "value": 42.5,
    })
    
    result, _ := processor.ProcessBatch(context.Background(), service.MessageBatch{tsMsg})
    bundle := decodeEmissionMessage(result[0][0])
    
    // Verify PayloadFormat for timeseries
    Expect(bundle.Events.Entries[0].PayloadFormat).To(Equal(PayloadFormat_TIMESERIES))
    
    // Relational message  
    time.Sleep(2 * time.Millisecond) // New emission
    relMsg := service.NewMessage(nil)
    relMsg.MetaSet("umh_topic", "umh.v1.test-topic._relation.users")
    relMsg.SetStructured([]interface{}{
        map[string]interface{}{"id": 1, "name": "Alice"},
    })
    
    result2, _ := processor.ProcessBatch(context.Background(), service.MessageBatch{relMsg})
    bundle2 := decodeEmissionMessage(result2[0][0])
    
    // Verify PayloadFormat for relational
    Expect(bundle2.Events.Entries[0].PayloadFormat).To(Equal(PayloadFormat_RELATIONAL))
})
```

## Test Implementation Priority

### **High Priority (Critical E2E Gaps)**
1. Realistic emit interval timing tests
2. Ring buffer overflow tests  
3. Buffer size safety limit tests
4. Delayed ACK pattern verification

### **Medium Priority (Important Edge Cases)**
5. Metadata persistence across emissions
6. PayloadFormat field validation
7. Multiple topic independence

### **Low Priority (Robustness)**
8. Graceful shutdown behavior
9. Error handling edge cases
10. Performance under load

## Helper Functions Needed

```go
func createTestMessage(field string, value interface{}, timestampMs int64) *service.Message {
    msg := service.NewMessage(nil)
    msg.MetaSet("umh_topic", "umh.v1.test-topic._historian."+field)
    msg.SetStructured(map[string]interface{}{
        "timestamp_ms": timestampMs,
        field: value,
    })
    return msg
}

func createTestMessageForTopic(topic string, value interface{}, timestampMs int64) *service.Message {
    msg := service.NewMessage(nil)
    msg.MetaSet("umh_topic", topic)
    msg.SetStructured(map[string]interface{}{
        "timestamp_ms": timestampMs,
        "value": value,
    })
    return msg
}

func decodeEmissionMessage(msg *service.Message) *UnsBundle {
    bytes, _ := msg.AsBytes()
    dataLine := strings.Split(string(bytes), "\n")[1]
    hexDecoded, _ := hex.DecodeString(dataLine)
    bundle, _ := ProtobufBytesToBundleWithCompression(hexDecoded)
    return bundle
}
```

## Summary

The current tests provide good basic coverage but miss **critical E2E scenarios** that are essential for production reliability. The biggest gaps are:

1. **No realistic timing tests** (current 1ms intervals are unrealistic)
2. **No ring buffer overflow validation** (core rate limiting feature untested)
3. **No buffer safety limit testing** (potential memory exhaustion risk)
4. **No delayed ACK verification** (core reliability pattern untested)

Adding these E2E tests would significantly improve confidence in production behavior, especially under high-traffic and edge case scenarios. 