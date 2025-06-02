# Node-RED JavaScript Processor

The Node-RED JavaScript processor allows you to write JavaScript code to process messages in a style similar to Node-RED function nodes. This makes it easy to port existing Node-RED functions to Benthos or write new processing logic using familiar JavaScript syntax.

Use the `nodered_js` processor instead of the `tag_processor` when you need full control over the payload and require custom processing logic that goes beyond standard tag or time series data handling. This processor allows you to write custom JavaScript code to manipulate both the payload and metadata, providing the flexibility to implement complex transformations, conditional logic, or integrate with other systems.

**Configuration**

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Your JavaScript code here
          return msg;
```

**Message Format**

Messages in Benthos and in the JavaScript processor are handled differently:

**In Benthos/Bloblang:**

```yaml
# Message content is the message itself
root = this   # accesses the message content

# Metadata is accessed via meta() function
meta("some_key")   # gets metadata value
meta some_key = "value"   # sets metadata
```

**In JavaScript (Node-RED style):**

```javascript
// Message content is in msg.payload
msg.payload   // accesses the message content

// Metadata is in msg.meta
msg.meta.some_key   // accesses metadata
```

The processor automatically converts between these formats.

**Examples**

1. **Pass Through Message**\
   Input message:

```json
{
  "temperature": 25.5,
  "humidity": 60
}
```

Metadata:

```yaml
sensor_id: "temp_1"
location: "room_a"
```

JavaScript code:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Message arrives as:
          // msg.payload = {"temperature": 25.5, "humidity": 60}
          // msg.meta = {"sensor_id": "temp_1", "location": "room_a"}

          // Simply pass through
          return msg;
```

Output: Identical to input

2. **Modify Message Payload**\
   Input message:

```json
["apple", "banana", "orange"]
```

JavaScript code:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // msg.payload = ["apple", "banana", "orange"]
          msg.payload = msg.payload.length;
          return msg;
```

Output message:

```json
3
```

3. **Create New Message**\
   Input message:

```json
{
  "raw_value": 1234
}
```

JavaScript code:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Create new message with transformed data
          var newMsg = {
            payload: {
              processed_value: msg.payload.raw_value * 2,
              timestamp: Date.now()
            }
          };
          return newMsg;
```

Output message:

```json
{
  "processed_value": 2468,
  "timestamp": 1710254879123
}
```

4. **Drop Messages (Filter)**\
   Input messages:

```json
{"status": "ok"}
{"status": "error"}
{"status": "ok"}
```

JavaScript code:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Only pass through messages with status "ok"
          if (msg.payload.status === "error") {
            return null;  // Message will be dropped
          }
          return msg;
```

Output: Only messages with status "ok" pass through

5. **Working with Metadata**\
   Input message:

```json
{"value": 42}
```

Metadata:

```yaml
source: "sensor_1"
```

JavaScript code:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Add processing information to metadata
          msg.meta.processed = "true";
          msg.meta.count = "1";

          // Modify existing metadata
          if (msg.meta.source) {
            msg.meta.source = "modified-" + msg.meta.source;
          }

          return msg;
```

Output message: Same as input

Output metadata:

```yaml
source: "modified-sensor_1"
processed: "true"
count: "1"
```

Equivalent Bloblang:

```coffee
meta processed = "true"
meta count = "1"
meta source = "modified-" + meta("source")
```

6. **String Manipulation**\
   Input message:

```json
"hello world"
```

JavaScript code:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Convert to uppercase
          msg.payload = msg.payload.toUpperCase();
          return msg;
```

Output message:

```json
"HELLO WORLD"
```

7. **Numeric Operations**\
   Input message:

```json
42
```

JavaScript code:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Double a number
          msg.payload = msg.payload * 2;
          return msg;
```

Output message:

```json
84
```

8. **Logging**\
   Input message:

```json
{
  "sensor": "temp_1",
  "value": 25.5
}
```

Metadata:

```yaml
timestamp: "2024-03-12T12:00:00Z"
```

JavaScript code:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Log various aspects of the message
          console.log("Processing temperature reading:" + msg.payload.value);
          console.log("From sensor:" + msg.payload.sensor);
          console.log("At time:" + msg.meta.timestamp);

          if (msg.payload.value > 30) {
            console.warn("High temperature detected!");
          }

          return msg;
```

Output: Same as input, with log messages in Benthos logs

**Performance Comparison**

When choosing between Node-RED JavaScript and Bloblang for message processing, consider the performance implications. Here's a benchmark comparison of both processors performing a simple operation (doubling a number) on 1000 messages:

**JavaScript Processing:**

* Median: 15.4ms
* Mean: 20.9ms
* Standard Deviation: 9.4ms
* Range: 13.8ms - 39ms

**Bloblang Processing:**

* Median: 3.7ms
* Mean: 4ms
* Standard Deviation: 800µs
* Range: 3.3ms - 5.6ms

**Key Observations:**

1. Bloblang is approximately 4-5x faster for simple operations
2. Bloblang shows more consistent performance (smaller standard deviation)
3. However, considering typical protocol converter workloads (around 1000 messages/second), the performance difference is negligible for most use cases. The JavaScript processor's ease of use and familiarity often outweigh the performance benefits of Bloblang, especially for smaller user-generated flows.

Note that these benchmarks represent a simple operation. The performance difference may vary with more complex transformations or when using advanced JavaScript features.
