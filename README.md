# benthos-umh

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![GitHub Actions](https://github.com/united-manufacturing-hub/benthos-umh/workflows/main/badge.svg)](https://github.com/united-manufacturing-hub/benthos-umh/actions)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Funited-manufacturing-hub%2Fbenthos-umh.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Funited-manufacturing-hub%2Fbenthos-umh?ref=badge_shield)

benthos-umh is a specialized extension of Benthos (now known as Redpanda Connect) developed by the [United Manufacturing Hub (UMH)](https://www.umh.app). Tailored for the manufacturing industry, benthos-umh integrates additionally manufacturing protocols such as OPC UA, Siemens S7, and Modbus.

Learn more by visiting our [Protocol Converter product page](https://www.umh.app/product/protocol-converter). For comprehensive technical documentation and configuration details, please continue reading below.

## Manufacturing Specific Plugins

### OPC UA

The plugin is designed to browse and subscribe to all child nodes within a folder for each configured NodeID, provided that the NodeID represents a folder. It features a recursion depth of up to 10 levels, enabling thorough exploration of nested folder structures. The browsing specifically targets nodes organized under the OPC UA 'Organizes' relationship type, intentionally excluding nodes under 'HasProperty' and 'HasComponent' relationships. Additionally, the plugin does not browse Objects represented by red, blue, or green cube icons in UAExpert.

Subscriptions are selectively managed, with tags having a DataType of null being excluded from subscription. Also, by default, the plugin does not subscribe to the properties of a tag, such as minimum and maximum values.

#### Datatypes
The plugin has been rigorously tested with an array of datatypes, both as single values and as arrays. The following datatypes have been verified for compatibility:

- `Boolean`
- `Byte`
- `DateTime`
- `Double`
- `Enumeration`
- `ExpandedNodeId`
- `Float`
- `Guid`
- `Int16`
- `Int32`
- `Int64`
- `Integer`
- `LocalizedText`
- `NodeId`
- `Number`
- `QualifiedName`
- `SByte`
- `StatusCode`
- `String`
- `UInt16`
- `UInt32`
- `UInt64`
- `UInteger`
- `ByteArray`
- `ByteString`
- `Duration`
- `LocaleId`
- `UtcTime`
- `Variant`
- `XmlElement`

There are specific datatypes which are currently not supported by the plugin and attempting to use them will result in errors. These include:

- Two-dimensional arrays
- UA Extension Objects
- Variant arrays (Arrays with multiple different datatypes)


#### Authentication and Security

In benthos-umh, we design security and authentication to be as robust as possible while maintaining flexibility. The software automates the process of selecting the highest level of security offered by an OPC-UA server for the selected Authentication Method, but the user can specify their own Security Policy / Security Mode if they want (see below at Configuration options)

##### Supported Authentication Methods

- **Anonymous**: No extra information is needed. The connection uses the highest security level available for anonymous connections.
- **Username and Password**: Specify the username and password in the configuration. The client opts for the highest security level that supports these credentials.
- **Certificate (Future Release)**: Certificate-based authentication is planned for future releases.

#### Metadata outputs

The plugin provides metadata for each message, that can be used to create a topic for the output, as shown in the example above. The metadata can also be used to create a unique identifier for each message, which is useful for deduplication.

| Metadata                 | Description                                                                                                                                          |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `opcua_tag_name`         | The sanitized ID of the Node that sent the message. This is always unique between nodes                                                              |
| `opcua_tag_group`        | A dot-separated path to the tag, created by joining the BrowseNames.                                                                                 |
| `opcua_tag_type`         | The data type of the node optimized for benthos, which can be either a number, string or bool. For the original one, check out `opcua_attr_datatype` |
| `opcua_source_timestamp` | The SourceTimestamp of the OPC UA node                                                                                                               |
| `opcua_server_timestamp` | The ServerTimestamp of the OPC UA node                                                                                                               |
| `opcua_attr_nodeid`      | The NodeID attribute of the Node as a string                                                                                                         |
| `opcua_attr_nodeclass`   | The NodeClass attribute of the Node as a string                                                                                                      |
| `opcua_attr_browsename`  | The BrowseName attribute of the Node as a string                                                                                                     |
| `opcua_attr_description` | The Description attribute of the Node as a string                                                                                                    |
| `opcua_attr_accesslevel` | The AccessLevel attribute of the Node as a string                                                                                                    |
| `opcua_attr_datatype`    | The DataType attribute of the Node as a string                                                                                                       |

Taking as example the following OPC-UA structure:

```text
Root
└── ns=2;s=FolderNode
    ├── ns=2;s=Tag1
    ├── ns=2;s=Tag2
    └── ns=2;s=SubFolder
        ├── ns=2;s=Tag3
        └── ns=2;s=Tag4
```

Subscribing to `ns=2;s=FolderNode` would result in the following metadata:

| `opcua_tag_name` | `opcua_tag_group`      |
|------------------|------------------------|
| `Tag1`           | `FolderNode`           |
| `Tag2`           | `FolderNode`           |
| `Tag3`           | `FolderNode.SubFolder` |
| `Tag4`           | `FolderNode.SubFolder` |

#### Configuration Options

The following options can be specified in the `benthos.yaml` configuration file:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    username: 'your-username'  # optional (default: unset)
    password: 'your-password'  # optional (default: unset)
    insecure: false | true # DEPRECATED, see below
    securityMode: None | Sign | SignAndEncrypt # optional (default: unset)
    securityPolicy: None | Basic256Sha256  # optional (default: unset)
    subscribeEnabled: false | true # optional (default: false)
    useHeartbeat: false | true # optional (default: false)
```

##### Endpoint

You can specify the endpoint in the configuration file. Node endpoints are automatically discovered and selected based on the authentication method.

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
```

##### Node IDs

You can specify the node IDs in the configuration file (currently only namespaced node IDs are supported):

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
```

##### Username and Password

If you want to use username and password authentication, you can specify them in the configuration file:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    username: 'your-username'
    password: 'your-password'
```

##### Security Mode and Security Policy

Security Mode: This defines the level of security applied to the messages. The options are:
- None: No security is applied; messages are neither signed nor encrypted.
- Sign: Messages are signed for integrity and authenticity but not encrypted.
- SignAndEncrypt: Provides the highest security level where messages are both signed and encrypted.

Security Policy: Specifies the set of cryptographic algorithms used for securing messages. This includes algorithms for encryption, decryption, and signing of messages. Currently only Basic256Sha256 is allowed.

While the security mode and policy are automatically selected based on the endpoint and authentication method, you have the option to override this by specifying them in the configuration file:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    securityMode: SignAndEncrypt
    securityPolicy: Basic256Sha256
```

##### Insecure Mode

This is now deprecated. By default, benthos-umh will now connect via SignAndEncrypt and Basic256Sha256 and if this fails it will fall back to insecure mode.

##### Pull and Subscribe Methods

Benthos-umh supports two modes of operation: pull and subscribe. In pull mode, it pulls all nodes every second, regardless of changes. In subscribe mode, it only sends data when there's a change in value, reducing unnecessary data transfer.

| Method    | Advantages                                                                                                                                                                                                                                    | Disadvantages                                                                                                        |
|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| Pull      | - Provides real-time data visibility, e.g., in MQTT Explorer. <br> - Clearly differentiates between 'no data received' and 'value did not change' scenarios, which can be crucial for documentation and proving the OPC-UA client's activity. | - Results in higher data throughput as it pulls all nodes every second, regardless of changes.                       |
| Subscribe | - Data is sent only when there's a change in value, reducing unnecessary data transfer.                                                                                                                                                       | - Less visibility into real-time data status, and it's harder to differentiate between no data and unchanged values. |

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    subscribeEnabled: true
```

##### UseHeartbeat

If you are unsure if the OPC UA server is actually sending new data, you can enable `useHeartbeat` by setting it to true. It will automatically subscribe to the OPC UA server time, and will re-connect automatically if it does not receive an update within 10 seconds.

```yaml
input:
  opcua:
    useHeartbeat: true
```

### S7comm

This input is tailored for the S7 communication protocol, facilitating a direct connection with S7-300, S7-400, S7-1200, and S7-1500 series PLCs.

For more modern PLCs like the S7-1200 and S7-1500 the following two changes need to be done to use them:
1. "Optimized block access" must be disabled for the DBs we want to access
2. In the "Protection" section of the CPU Properties, enable the "Permit access with PUT/GET" checkbox

#### Configuration
```yaml
input:
  s7comm:
    tcpDevice: '192.168.0.1' # IP address of the S7 PLC
    rack: 0                  # Rack number of the PLC. Defaults to 0
    slot: 1                  # Slot number of the PLC. Defaults to 1
    batchMaxSize: 480         # Maximum number of addresses per batch request. Defaults to 480
    timeout: 10             # Timeout in seconds for connections and requests. Default to 10
    disableCPUInfo: false # Set this to true to not fetch CPU information from the PLC. Should be used when you get the error 'Failed to get CPU information'
    addresses:               # List of addresses to read from
      - "DB1.DW20"     # Accesses a double word at location 20 in data block 1
      - "DB1.S30.10"   # Accesses a 10-byte string at location 30 in data block 1
```

#### Configuration Parameters

- **tcpDevice**: IP address of the Siemens S7 PLC.
- **rack**: Identifies the physical location of the CPU within the PLC rack.
- **slot**: Identifies the specific CPU slot within the rack.
- **batchMaxSize**: Maximum count of addresses bundled in a single batch request. This affects the PDU size.
- **timeout**: Timeout duration in milliseconds for connection attempts and read requests.
- **disableCPUInfo**: Set this to true to not fetch CPU information from the PLC. Should be used when you get the error 'Failed to get CPU information'
- **addresses**: Specifies the list of addresses to read. The format for addresses is `<area>.<type><address>[.extra]`, where:
  - `area`: Specifies the direct area access, e.g., "DB1" for data block one. Supported areas include inputs (`PE`), outputs (`PA`), Merkers (`MK`), DB (`DB`), counters (`C`), and timers (`T`).
  - `type`: Indicates the data type, such as bit (`X`), byte (`B`), word (`W`), double word (`DW`), integer (`I`), double integer (`DI`), real (`R`), date-time (`DT`), and string (`S`). Some types require an 'extra' parameter, e.g., the bit number for `X` or the maximum length for `S`.

#### Output

Similar to the OPC UA input, this outputs for each address a single message with the payload being the value that was read. To distinguish messages, you can use meta("s7_address") in a following benthos bloblang processor.

Here's the requested Modbus documentation formatted similarly to your existing OPC UA documentation for inclusion in your README:

### Modbus

The Modbus plugin facilitates communication with various types of Modbus devices. It supports reading from four types of registers: coils, discrete inputs, holding registers, and input registers. Each data item configuration requires specifying the register type, address, and the data type to be read. The plugin supports multiple data types including integers, unsigned integers, floats, and strings across different sizes and formats.

Data reads can be configured to occur at a set interval, allowing for consistent data polling. Advanced features like register optimization and workarounds for device-specific quirks are also supported to enhance communication efficiency and compatibility.

#### Metadata Outputs

For each read operation, the plugin outputs detailed metadata that includes various aspects of the read operation, which can be utilized to effectively tag, organize, and utilize the data within a system. This metadata encompasses identifiers, data types, and register specifics to ensure precise tracking and utilization of the Modbus data.

Below is the extended metadata output schema provided by the plugin:

| Metadata                   | Description                                                                     |
|----------------------------|---------------------------------------------------------------------------------|
| `modbus_tag_name`          | Sanitized tag name, with special characters removed for compatibility.          |
| `modbus_tag_name_original` | Original tag name, as defined in the device configuration.                      |
| `modbus_tag_datatype`      | Original Modbus data type of the tag.                                           |
| `modbus_tag_datatype_json` | Data type of the tag suitable for JSON representation: number, bool, or string. |
| `modbus_tag_address`       | String representation of the tag's Modbus address.                              |
| `modbus_tag_length`        | The length of the tag in registers, relevant for string or array data types.    |
| `modbus_tag_register`      | The specific Modbus register type where the tag is located.                     |
| `modbus_tag_slaveid`       | The slave ID where the tag is coming from                                       |

This enhanced metadata schema provides comprehensive data for each read operation, ensuring that users have all necessary details for effective data management and application integration.

#### Configuration Options

Below are the comprehensive configuration options available in the configuration file for the Modbus plugin. This includes settings for device connectivity, data reading intervals, optimization strategies, and detailed data item configurations.

```yaml
input:
  modbus:
    controller: 'tcp://localhost:502'
    transmissionMode: 'TCP'
    slaveIDs:
      - 1
    timeout: '1s'
    busyRetries: 3
    busyRetriesWait: '200ms'
    timeBetweenReads: '1s'
    optimization: 'none'
    byteOrder: 'ABCD'
    addresses:
      - name: "firstFlagOfDiscreteInput"
        register: "discrete"
        address: 1
        type: "BIT"
        output: "BOOL"
      - name: "zeroElementOfInputRegister"
        register: "input"
        address: 0
        type: "UINT16"
```

##### Controller

Specifies the network address of the Modbus controller:

```yaml
input:
  modbus:
    controller: 'tcp://localhost:502'
```

##### Transmission Mode

Defines the Modbus transmission mode. Can be "TCP" (default), "RTUOverTCP", "ASCIIOverTCP":

```yaml
input:
  modbus:
    transmissionMode: 'TCP'
```

##### Slave IDs

Configure the modbus slave IDs :

```yaml
input:
  modbus:
    slaveIDs:
      - 1
      - 2
```

For backwars compatbility there is also `slaveID: 1`, which allows setting only a single Modbus slave.

##### Retry Settings & Timeout

Configurations to handle retries in case of communication failures:

```yaml
input:
  modbus:
    busyRetries: 3
    busyRetriesWait: '200ms'
    timeout: '1s'
```

##### Time Between Reads

Defines how frequently the Modbus device should be polled:

```yaml
input:
  modbus:
    timeBetweenReads: '1s'
```

##### Optimization

The Modbus plugin offers several strategies to optimize data read requests, enhancing efficiency and reducing network load when interacting with Modbus devices. These strategies are designed to adjust the organization and batching of requests based on device capabilities and network conditions.

The available optimization strategies are:
- **none**: This is the default setting where no optimization is applied. The plugin groups read requests according to the defined metrics without further optimization. Suitable for systems with simple setups or minimal performance requirements.

- **max_insert**: Enhances efficiency by collating read requests across all defined metrics and filling in gaps (non-consecutive registers) to minimize the total number of requests. This strategy is ideal for complex systems with numerous data points, as it significantly reduces network traffic and processing time.

- **shrink**: Reduces the size of each request by stripping leading and trailing fields marked with an omit flag. This can decrease the overall data payload and improve processing times, especially when many fields are optional or conditional.

- **rearrange**: Allows rearranging fields between requests to reduce the number of registers accessed while maintaining the minimal number of requests. This strategy optimizes the order of fields to minimize the spread across registers.

- **aggressive**: Similar to "rearrange" but allows mixing of groups. This approach may reduce the number of requests at the cost of accessing more registers, potentially touching more data than necessary to consolidate requests.

Each strategy can be tailored with parameters such as `OptimizationMaxRegisterFill` to control how aggressively the system attempts to optimize data reads. For example, the `max_insert` option can be configured to limit the number of additional registers filled to reduce gaps:

```yaml
input:
  modbus:
    optimization: 'max_insert'
    optimizationMaxRegisterFill: 10
```

Additional Configuration for Optimization Strategies:

- **OptimizationMaxRegisterFill**: Specifies the maximum number of registers the optimizer is allowed to insert between non-consecutive registers in the `max_insert` strategy.

##### Byte Order

The `byteOrder` configuration specifies how bytes within the registers are ordered, which is essential for correctly interpreting the data read from a Modbus device. Different devices or systems may represent multi-byte data types (like integers and floating points) in various byte orders. The options are:

- **ABCD**: Big Endian (Motorola format) where the most significant byte is stored first.
- **DCBA**: Little Endian (Intel format) where the least significant byte is stored first.
- **BADC**: Big Endian with byte swap where bytes are stored in a big-endian order but each pair of bytes is swapped.
- **CDAB**: Little Endian with byte swap where bytes are stored in little-endian order with each pair of bytes swapped.

```yaml
input:
  modbus:
    byteOrder: 'ABCD'
```

##### Modbus Workaround

The Modbus plugin incorporates specific workarounds to address compatibility and performance issues that may arise with various Modbus devices. These workarounds ensure the plugin can operate efficiently even with devices that have unique quirks or non-standard Modbus implementations.

```yaml

input:
  modbus:
    workarounds:
      pauseAfterConnect: '500ms'
      oneRequestPerField: true
      readCoilsStartingAtZero: true
      timeBetweenRequests: '100ms'
      stringRegisterLocation: 'upper'
```

1. **Pause After Connect**
  - **Description**: Introduces a delay before sending the first request after establishing a connection.
  - **Purpose**: This is particularly useful for slow devices that need time to stabilize a connection before they can process requests.
  - **Default**: `0s`
  - **Configuration Example**:
    ```yaml
    pauseAfterConnect: '500ms'
    ```

2. **One Request Per Field**
  - **Description**: Configures the system to send each field request separately.
  - **Purpose**: Some devices may have limitations that prevent them from handling multiple field requests in a single Modbus transaction. Isolating requests ensures compatibility.
  - **Default**: `false`
  - **Configuration Example**:
    ```yaml
    oneRequestPerField: true
    ```

3. **Read Coils Starting at Zero**
  - **Description**: Adjusts the starting address for reading coils to begin at address 0 instead of 1.
  - **Purpose**: Certain devices may map their coil addresses starting from 0, which is non-standard but not uncommon.
  - **Default**: `false`
  - **Configuration Example**:
    ```yaml
    readCoilsStartingAtZero: true
    ```

4. **Time Between Requests**
  - **Description**: Sets the minimum interval between consecutive requests to the same device.
  - **Purpose**: Prevents the overloading of Modbus devices by spacing out the requests, which is critical in systems where devices are sensitive to high traffic.
  - **Default**: `0s`
  - **Configuration Example**:
    ```yaml
    timeBetweenRequests: '100ms'
    ```

5. **String Register Location**
  - **Description**: Specifies which part of the register to use for string data after byte-order conversion.
  - **Options**:
    - `lower`: Uses only the lower byte of each register.
    - `upper`: Uses only the upper byte of each register.
    - If left empty, both bytes of the register are used.
  - **Purpose**: Some devices may place string data only in specific byte locations within a register, necessitating this adjustment for correct string interpretation.
  - **Default**: Both bytes used.
  - **Configuration Example**:
    ```yaml
    stringRegisterLocation: 'upper'
    ```

##### Addresses

The Modbus plugin provides a highly configurable way to specify which data points (addresses) to read from Modbus devices. Each address configuration allows precise definition of what data to read, how it's interpreted, and how it should be scaled or formatted before use.
```yaml
input:
  modbus:
    addresses:
      - name: "firstFlagOfDiscreteInput"
        register: "discrete"
        address: 1
        type: "BIT"
        output: "BOOL"
      - name: "zeroElementOfInputRegister"
        register: "input"
        address: 0
        type: "UINT16"
```

1. **Name**
  - **Description**: Identifier for the data point being configured.
  - **Configuration Example**:
    ```yaml
    name: "TemperatureSensor"
    ```

2. **Register**
  - **Description**: Specifies the type of Modbus register to query. Options include "coil", "discrete", "holding", or "input".
  - **Default**: "holding"
  - **Configuration Example**:
    ```yaml
    register: "holding"
    ```

3. **Address**
  - **Description**: The Modbus register address from which data should be read.
  - **Configuration Example**:
    ```yaml
    address: 3
    ```

4. **Type**
  - **Description**: Specifies the data type of the field, which determines how the data read from the register is interpreted. This setting is crucial as it affects how the raw data from Modbus registers is processed and used. The available data types cater to various data resolutions and formats, ranging from single-bit signals to full 64-bit precision, including special formats for strings and floating-point numbers.
  - **Options**:
    - `BIT`: Single bit of a register.
    - `INT8L`: 8-bit integer (low byte).
    - `INT8H`: 8-bit integer (high byte).
    - `UINT8L`: 8-bit unsigned integer (low byte).
    - `UINT8H`: 8-bit unsigned integer (high byte).
    - `INT16`: 16-bit integer.
    - `UINT16`: 16-bit unsigned integer.
    - `INT32`: 32-bit integer.
    - `UINT32`: 32-bit unsigned integer.
    - `INT64`: 64-bit integer.
    - `UINT64`: 64-bit unsigned integer.
    - `FLOAT16`: 16-bit floating point (IEEE 754).
    - `FLOAT32`: 32-bit floating point (IEEE 754).
    - `FLOAT64`: 64-bit floating point (IEEE 754).
    - `STRING`: A sequence of bytes converted to a string.

5. **Length**
  - **Description**: Number of registers to read, primarily used when the data type is "STRING".
  - **Default**: 0
  - **Configuration Example**:
    ```yaml
    length: 2
    ```

6. **Bit**
  - **Description**: Relevant only for BIT data type, specifying which bit of the register to read.
  - **Default**: 0
  - **Configuration Example**:
    ```yaml
    bit: 7
    ```

7. **Scale**
  - **Description**: A multiplier applied to the numeric data read from the register, used to scale values to the desired range or unit.
  - **Default**: 0.0
  - **Configuration Example**:
    ```yaml
    scale: 0.1
    ```

8. **Output**
  - **Description**: Specifies the data type of the output field. Options include "INT64", "UINT64", "FLOAT64", or "native" (which retains the original data type without conversion).
  - **Default**: Defaults to FLOAT64 if "scale" is provided and to the input "type" class otherwise (i.e. INT* -> INT64, etc).
  - **Configuration Example**:
    ```yaml
    output: "FLOAT64"
    ```

### ifm IO-Link Master / "sensorconnect"
The SensorConnect plugin facilitates communication with ifm electronic’s IO-Link Masters devices, such as the AL1350 IO-Link Master.
It enables the integration of sensor data into Benthos pipelines by connecting to the device over HTTP and processing data from connected sensors, including digital inputs and IO-Link devices.
The plugin handles parsing and interpreting IO-Link data using IODD files, converting raw sensor outputs into human-readable data.

It was previously known as [sensorconnect](https://github.com/united-manufacturing-hub/united-manufacturing-hub/tree/staging/golang/cmd/sensorconnect).

#### Configuration
```yaml
input:
  sensorconnect:
    device_address: '192.168.0.1' # IP address of the IO-Link Master
```

#### Configuration Parameters

- **device_address**: IP address of the IO-Link Master

#### Output
The payload of each message is a JSON object containing the sensor data, structured according to the data provided by the connected device. The exact structure of the payload depends on the specific sensors connected to the SensorConnect device and the data they provide.

Example for a VVB001 vibration sensor:
```json
{
  "Crest": 41,
  "Device status": 0,
  "OUT1": true,
  "OUT2": true,
  "Temperature": 394,
  "a-Peak": 2,
  "a-Rms": 0,
  "v-Rms": 0
}

```
#### Metadata Outputs

For each read operation, the plugin outputs detailed metadata that includes various aspects of the read operation, which can be utilized to effectively tag, organize, and utilize the data within a system. This metadata encompasses identifiers, data types, and register specifics to ensure precise tracking and utilization of the Modbus data.

Below is the extended metadata output schema provided by the plugin:

| Metadata                                 | Description                                                       |
|------------------------------------------|-------------------------------------------------------------------|
| `sensorconnect_port_mode`                | The mode of the port, e.g., digital-input or io-link.             |
| `sensorconnect_port_number`              | The number of the port on the ifm IO-Link Master device.          |
| `sensorconnect_port_iolink_vendor_id`    | The IO-Link vendor ID of the connected device (if applicable).    |
| `sensorconnect_port_iolink_device_id`    | The IO-Link device ID of the connected device (if applicable).    |
| `sensorconnect_port_iolink_product_name` | The product name of the connected IO-Link device (if applicable). |
| `sensorconnect_port_iolink_serial`       | The serial number of the connected IO-Link device.                |
| `sensorconnect_device_product_code`      | The product code of the connected IO-Link device.                 |
| `sensorconnect_device_serial_number`     | The serial number of the connected IO-Link device                 |

### Beckhoff ADS
Input for Beckhoff's ADS protocol. Supports batch reading and notifications. Beckhoff recommends limiting notifications to approximately 500 to avoid overloading the controller.
This input only supports symbols and not direct addresses.

**This plugin is community supported only. If you encounter any issues, check out the [original repository](https://github.com/RuneRoven/benthosADS) for more information, or ask around in our Discord.**

```yaml
---
input:
  ads:
    targetIP: '192.168.3.70'        # IP address of the PLC
    targetAMS: '5.3.69.134.1.1'     # AMS net ID of the target
    targetPort: 48898               # Port of the target internal gateway
    runtimePort: 801                # Runtime port of PLC system
    hostAMS: '192.168.56.1.1.1'     # Host AMS net ID. Usually the IP address + .1.1
    hostPort: 10500                 # Host port
    readType: 'interval'            # Read type, interval or notification
    maxDelay: 100                   # Max delay for sending notifications in ms
    cycleTime: 100                  # Cycle time for notification handler in ms
    intervalTime: 1000              # Interval time for reading in ms
    upperCase: true                 # Convert symbol names to all uppercase for older PLCs
    logLevel: "disabled"            # Log level for ADS connection
    symbols:                        # List of symbols to read from
      - "MAIN.MYBOOL"               # variable in the main program
      - "MAIN.MYTRIGGER:0:10"       # variable in the main program with 0ms max delay and 10ms cycleTime
      - "MAIN.SPEEDOS"
      - ".superDuperInt"            # Global variable
      - ".someStrangeVar"

pipeline:
  processors:
    - bloblang: |
        root = {
          meta("symbol_name"): this,
          "timestamp_ms": (timestamp_unix_nano() / 1000000).floor()
        }
output:
  stdout: {}

logger:
  level: ERROR
  format: logfmt
  add_timestamp: true
  ```

#### Connection to ADS
Connecting to an ADS device involves routing traffic through a router using the AMS net ID.
There are basically 2 ways for setting up the connection. One approach involves using the Twincat connection manager to locally scan for the device on the host and add a connection using the correct PLC credentials. The other way is to log in to the PLC using the Twincat system manager and add a static route from the PLC to the client. This is the preferred way when using benthos on a Kubernetes cluster since you have no good way of installing the connection manager.

#### Configuration Parameters
- **targetIP**: IP address of the PLC
- **targetAMS**: AMS net ID of the target
- **targetPort**: Port of the target internal gateway
- **runtimePort**: Runtime port of PLC system,  800 to 899. Twincat 2 uses ports 800 to 850, while Twincat 3 is recommended to use ports 851 to 899. Twincat 2 usually have 801 as default and Twincat 3 uses 851
- **hostAMS**: Host AMS net ID. Usually the IP address + .1.1
- **hostPort**: Host port
- **readType**: Read type for the symbols. Interval means benthos reads all symbols at a specified interval and notification is a function in the PLC where benthos sends a notification request to the PLC and the PLC adds the symbol to its internal notification system and sends data whenever there is a change.
- **maxDelay**: Default max delay for sending notifications in ms. Sets a maximum time for how long after the change the PLC must send the notification
- **cycleTime**: Default cycle time for notification handler in ms. Tells the notification handler how often to scan for changes. For symbols like triggers that is only true or false for 1 PLC cycle it can be necessary to use a low value.
- **intervalTime**: Interval time for reading in ms. For reading batches of symbols this sets the time between readings
- **upperCase**: Converts symbol names to all uppercase for older PLCs. For Twincat 2 this is often necessary.
- **logLevel**: Log level for ADS connection sets the log level of the internal log function for the underlying ADS library
- **symbols**: List of symbols to read from in the format <function.variable:maxDelay:cycleTime>, e.g., "MAIN.MYTRIGGER:0:10" is a variable in the main program with 0ms max delay and 10ms cycle time,  "MAIN.MYBOOL" is a variable in the main program with no extra arguments, so it will use the default max delay and cycle time. ".superDuperInt" is a global variable with no extra arguments. All global variables must start with a <.> e.g., ".someStrangeVar"

#### Output

Similar to the OPC UA input, this outputs for each address a single message with the payload being the value that was read. To distinguish messages, you can use meta("symbol_name") in a following benthos bloblang processor.

## Testing

We execute automated tests and verify that benthos-umh works against various targets. All tests are started with `make test`, but might require environment parameters in order to not be skipped.

Some of these tests are executed with a local GitHub runner called "hercules", which is connected to an isolated testing network.

### Target: WAGO PFC100 (OPC UA)

Model number: 750-8101

Requires:
- TEST_WAGO_ENDPOINT_URI
- TEST_WAGO_USERNAME
- TEST_WAGO_PASSWORD

### Target: Microsoft OPC UA Simulator (OPC UA)

Docker tag: mcr.microsoft.com/iotedge/opc-plc:2.9.11

Requires:
- TEST_OPCUA_SIMULATOR

### Target: Prosys OPC UA Simulator (OPC UA)

Version: 5.4.6-148

Requires:
- TEST_PROSYS_ENDPOINT_URI

This requires additional to have the simulator setup somewhere (e.g., locally on your PC) and pointing the test towards it. This is not included in any CI andm ust be run manually.

### Target: Siemens S7-1200 (OPC UA)

Model number: SIMATIC S7-1200 6ES7211-1AE40-0XB0

Requires:
- TEST_S7_ENDPOINT_URI

### Target: Unit Tests (OPC UA)

Requires:
- TEST_OPCUA_UNITTEST

### Target: Siemens S7-1200 (S7comm)

Model number: SIMATIC S7-1200 6ES7211-1AE40-0XB0

Requires:
- TEST_S7_TCPDEVICE
- TEST_S7_RACK
- TEST_S7_SLOT

### Target: Unit Tests (S7comm)

Requires:
- TEST_S7COMM_UNITTEST

## License

All source code is distributed under the APACHE LICENSE, VERSION 2.0. See LICENSE for more information.


[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Funited-manufacturing-hub%2Fbenthos-umh.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Funited-manufacturing-hub%2Fbenthos-umh?ref=badge_large)

## Contact

Feel free to provide us feedback on our [Discord channel](https://discord.gg/F9mqkZnm9d).

For more information about the United Manufacturing Hub, visit [UMH Systems GmbH](https://www.umh.app). If you haven't worked with the United Manufacturing Hub before, [give it a try](https://management.umh.app))! Setting it up takes only a matter of minutes.
