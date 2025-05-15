# benthos-umh

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![GitHub Actions](https://github.com/united-manufacturing-hub/benthos-umh/workflows/main/badge.svg)](https://github.com/united-manufacturing-hub/benthos-umh/actions)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Funited-manufacturing-hub%2Fbenthos-umh.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Funited-manufacturing-hub%2Fbenthos-umh?ref=badge_shield)

benthos-umh is a specialized extension of Benthos (now known as Redpanda Connect) developed by the [United Manufacturing Hub (UMH)](https://www.umh.app). Tailored for the manufacturing industry, benthos-umh integrates additionally manufacturing protocols such as OPC UA, Siemens S7, and Modbus.

Learn more by visiting our [Protocol Converter product page](https://www.umh.app/product/protocol-converter). For comprehensive technical documentation and configuration details, please continue reading below.

## Manufacturing Specific Plugins

<details>
<summary>
OPC UA
</summary>

### OPC UA (Input)

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
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `opcua_tag_name`         | The sanitized ID of the Node that sent the message. This is always unique between nodes                                                              |
| `opcua_tag_path`         | A dot-separated path to the tag, created by joining the BrowseNames.                                                                                 |
| `opcua_tag_group`        | Other name for `opcua_tag_path`                                                                                                                      |
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
| ---------------- | ---------------------- |
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
    username: 'your-username' # optional (default: unset)
    password: 'your-password' # optional (default: unset)
    insecure: false | true # DEPRECATED, see below
    securityMode: None | SignAndEncrypt # optional (default: unset)
    securityPolicy: None | Basic128Rsa15 | Basic256 | Basic256Sha256 # optional (default: unset)
    serverCertificateFingerprint: 'sha3-fingerprint-of-cert' # optional (default: unset)
    clientCertificate: 'your-fixed-base64-encoded-certificate' # optional (default: unset)
    userCertificate: 'base64-encoded-user-PEM-certificate' # optional (default: unset)
    userPrivateKey: 'base64-encoded-user-PEM-private-key' # optional (default: unset)
    subscribeEnabled: false | true # optional (default: false)
    useHeartbeat: false | true # optional (default: false)
    pollRate: 1000 # optional (default: 1000) The rate in milliseconds at which to poll the OPC UA server when not using subscriptions
    autoReconnect: false | true # optional (default: false)
    reconnectIntervalInSeconds: 5 # optional (default: 5) The rate in seconds at which to reconnect to the OPC UA server when the connection is lost
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

#### User Certificate and Private Key

- **Keys**: `userCertificate`, `userPrivateKey`
- **Description**: Credentials for User certificate-based authentication.
- `userCertificate`: Base64-encoded certificate in either PEM (.pem) or DER (.der) format.
- `userPrivateKey`: Base64-encoded private key in PEM (.pem) format only.
- Certificate-based authentication provides stronger security than username/password for high-security environments.
- Proper protection of the private key and certificate validation on both client and server are essential.
- **Configuration Example**:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    securityMode: SignAndEncrypt
    securityPolicy: Basic256Sha256
    userCertificate: 'base64-encoded certificate (.pem or .der)'
    userPrivateKey: 'base64-encoded private key (.pem only)'
```

##### Security Options

> To ensure a fully secure connection, you must explicitly configure all of the following security options. However, if these settings seem overwhelming, you can leave them unspecified. In that case, **benthos-umh** will automatically scan for and connect to available endpoints until it succeeds—and then it will log the recommended security settings for your future configuration.

OPC UA supports various security modes and security policies. These options define how messages are signed or encrypted and which cryptographic algorithms are used. In the configuration, you can specify the following:

- **Security Mode**: Defines the level of security applied to messages.

  - **Key**: `securityMode`
  - **Values**:
    - **None**: No security is applied; messages are neither signed nor encrypted.
    - **Sign**: Messages are signed for integrity and authenticity but not encrypted.
    - **SignAndEncrypt**: The highest level of security where messages are both signed and encrypted.

- **Security Policy**: Specifies the cryptographic algorithms used for signing/encrypting messages.

  - **Key**: `securityPolicy`
  - **Values**:
    - **None**: No security applied.
    - **Basic128Rsa15** (**deprecated**): Insecure due to SHA-1. Often disabled on servers by default.
    - **Basic256** (**deprecated**): Insecure due to SHA-1. Often disabled on servers by default.
    - **Basic256Sha256**: Recommended. Uses SHA-256 and provides stronger security.

- **Server Certificate Fingerprint**:

  - **Key**: `serverCertificateFingerprint`
  - **Description**: A SHA3-512 hash of the server’s certificate, used to verify you are connecting to the correct server.
  - If you specify this field, the client will verify that the server’s certificate matches the given fingerprint. If there’s a mismatch, the connection is rejected.
  - If omitted while **still using encryption** (`Sign` or `SignAndEncrypt`), the client will attempt to connect and then **log** the server’s actual fingerprint. You can copy that fingerprint into your config to be certain you’re connecting to the intended server.
  - In future releases, omitting the fingerprint may become a warning or block deployment in certain environments.

- **Client Certificate**:
  - **Key**: `clientCertificate`
  - **Description**: A Base64‐encoded PEM bundle (certificate + private key).
  - When using encryption (`Sign` or `SignAndEncrypt`), the client must present a certificate to the server. If you **do not** provide one, the system **auto‐generates** a random certificate at startup.
  - The auto‐generated certificate is logged in Base64 so you can copy/paste it into your configuration. This allows the server to trust the same client certificate across restarts instead of generating a new one each time.
  - Whenever a certificate is created, and the OPC UA server's settings do not allow automatic acceptance of client certificates, you will need to manually trust the client certificate in the server's settings. The client's name will be displayed, enabling you to uniquely identify it in the certificate list.

If you want to connect with security options, you will at least have to provide the following sample:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    securityMode: SignAndEncrypt
    securityPolicy: Basic256Sha256
    serverCertificateFingerprint: 'sha3-fingerprint-of-cert'
    clientCertificate: 'your-fixed-base64-encoded-certificate' # optional but recommended
```

##### Insecure Mode

This is now deprecated. By default, benthos-umh will now connect via SignAndEncrypt and Basic256Sha256 and if this fails it will fall back to insecure mode.

##### Pull and Subscribe Methods

Benthos-umh supports two modes of operation: pull and subscribe. In pull mode, it pulls all nodes every second, regardless of changes. In subscribe mode, it only sends data when there's a change in value, reducing unnecessary data transfer.

| Method    | Advantages                                                                                                                                                                                                                                    | Disadvantages                                                                                                                         |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Pull      | - Provides real-time data visibility, e.g., in MQTT Explorer. <br> - Clearly differentiates between 'no data received' and 'value did not change' scenarios, which can be crucial for documentation and proving the OPC-UA client's activity. | - Results in higher data throughput as it pulls all nodes at the configured poll rate (default: every second), regardless of changes. |
| Subscribe | - Data is sent only when there's a change in value, reducing unnecessary data transfer.                                                                                                                                                       | - Less visibility into real-time data status, and it's harder to differentiate between no data and unchanged values.                  |

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

##### Browse Hierarchical References (Option until version 0.5.2)

**NOTE**: This property is removed in version 0.6.0 and made as a standard way to browse OPCUA nodes. From version 0.6.0 onwards, opcua_plugin will browse all nodes with Hierarchical References.

The plugin offers an option to browse OPCUA nodes by following Hierarchical References. By default, this feature is disabled (`false`), which means the plugin will only browse a limited subset of reference types, including:

- `HasComponent`
- `Organizes`
- `FolderType`
- `HasNotifier`

When set to `true`, the plugin will explore a broader range of node references. For a deeper understanding of the different reference types, refer to the [Standard References Type documentation](https://qiyuqi.gitbooks.io/opc-ua/content/Part3/Chapter7.html).

**Recommendation**: Enable this option (`browseHierarchicalReferences: true`) for more comprehensive node discovery.

```yaml
input:
  opcua:
    browseHierarchicalReferences: true
```

##### Auto Reconnect

If the connection is lost, the plugin will automatically reconnect to the OPC UA server. This is useful if the OPC UA server is unstable or if the network is unstable.

```yaml
input:
  opcua:
    autoReconnect: true
```

##### Reconnect Interval

The interval in seconds at which to reconnect to the OPC UA server when the connection is lost. This is only used if `autoReconnect` is set to true.

```yaml
input:
  opcua:
    reconnectIntervalInSeconds: 5
```

</details>

<details>
<summary>
OPC UA Output
</summary>

### OPC UA Output

The **OPC UA output** plugin writes data into an OPC UA server (e.g., a PLC). This plugin supports optional read-back (handshake) to confirm the write.

> **Data Transformations**
> It is recommended to perform JSON-to-field transformations _before_ this plugin (e.g., via [Node-RED JavaScript](#node-red-javascript-processor) or [Bloblang](https://www.benthos.dev/docs/guides/bloblang/about)). That way, you feed the final fields directly to this plugin without extra logic here.

---

#### Basic Configuration

```yaml
output:
  opcua:
    # endpoint, username, password, securityMode, securityPolicy, serverCertificateFingerprint, clientCertificate
    # see OPC UA Input for more information

    nodeMappings:
      - nodeId: 'ns=2;s=MySetpoint'
        valueFrom: 'setpoint' # The JSON field to write
        dataType: 'Int32' # OPC UA data type (required)
      - nodeId: 'ns=2;s=MyEnableFlag'
        valueFrom: 'enable_flag'
        dataType: 'Boolean' # OPC UA data type (required)

    handshake:
      enabled: true # enable read-back
      readbackTimeoutMs: 2000 # how long to wait for the new value to appear

      maxWriteAttempts: 3 # how many times to retry if the write fails
      timeBetweenRetriesMs: 1000 # time (ms) between write retries
```

#### Supported Data Types

The OPC UA output plugin supports the following commonly used data types for writing to OPC UA servers:

- `Boolean`: True/false values
- `Byte`: 8-bit unsigned integer (0 to 255)
- `SByte`: 8-bit signed integer (-128 to 127)
- `Int16`: 16-bit signed integer
- `UInt16`: 16-bit unsigned integer
- `Int32`: 32-bit signed integer
- `UInt32`: 32-bit unsigned integer
- `Int64`: 64-bit signed integer
- `UInt64`: 64-bit unsigned integer
- `Float`: 32-bit floating-point number
- `Double`: 64-bit floating-point number
- `String`: UTF-8 encoded string
- `DateTime`: Date and time values

**Note:** When selecting a data type, ensure it matches the expected type on the OPC UA server. Mismatched types may cause write operations to fail or data to be interpreted incorrectly.

**Fields:**

| Field                              | Description                                                                                                                                                                                                                                                                                          |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **nodeMappings** (array)           | List of nodes to write to, each with:<br>- `nodeId`: The OPC UA Node ID, e.g., `"ns=2;s=MyVariable"`<br>- `valueFrom`: The JSON field name (string) in your message containing the final value.<br>- `dataType`: The OPC UA data type for the value (e.g., `"Boolean"`, `"Int32"`, `"Double"`, etc.) |
| **handshake**                      | (Optional) A sub-config for read-back validation.                                                                                                                                                                                                                                                    |
| **handshake.enabled**              | Default `true` (for safe writes). If `true`, the plugin attempts to read the node(s) after writing. If the new value doesn’t match, the write is considered failed.                                                                                                                                  |
| **handshake.readbackTimeoutMs**    | How long to wait for the server to show the updated value. If it times out, the plugin fails (Benthos will not ACK the message).                                                                                                                                                                     |
| **handshake.maxWriteAttempts**     | Number of write attempts if the server fails (default 1).                                                                                                                                                                                                                                            |
| **handshake.timeBetweenRetriesMs** | Delay between write attempts (default 1000 ms).                                                                                                                                                                                                                                                      |

---

#### Usage Example

**Incoming Message** (e.g., from a Benthos input or pipeline):

```json
{
  "setpoint": 123,
  "enable_flag": true
}
```

**Plugin Configuration**:

```yaml
output:
  opcua:
    endpoint: 'opc.tcp://192.168.0.10:4840'
    nodeMappings:
      - nodeId: 'ns=2;s=MySetpoint'
        valueFrom: 'setpoint'
        dataType: 'Int32'
      - nodeId: 'ns=2;s=MyEnableFlag'
        valueFrom: 'enable_flag'
        dataType: 'Boolean'

    handshake:
      enabled: true
      readbackTimeoutMs: 2000
      maxWriteAttempts: 3
      timeBetweenRetriesMs: 1000
```

**Behavior**:

1. The plugin connects to the OPC UA server at `opc.tcp://192.168.0.10:4840`.
2. It **writes** the field `setpoint=123` to node `ns=2;s=MySetpoint` and `enable_flag=true` to `ns=2;s=MyEnableFlag`.
3. Immediately after, it **reads** these nodes back:
   - If the server now reports `MySetpoint=123` and `MyEnableFlag=true`, the write is considered successful.
   - If the read times out after 2 s or shows a different value, the plugin fails the write. Benthos will _not_ ACK the message upstream, so it can be retried or routed to an error output.
4. Assuming success, the message is **acknowledged** and removed from the pipeline.

If, for example, the `ns=2;s=MyEnableFlag` node is read-only or the server rejects the update, the read-back will fail, causing the plugin to retry up to 3 times (`maxWriteAttempts`). If all attempts fail, Benthos escalates the failure.

---

#### Handshake & Acknowledgment

By default, the plugin **reads back** each node it wrote to confirm the new value appears. This ensures:

1. **Benthos Message ACK**: If the read-back fails or times out, the output plugin fails. Benthos will _not_ acknowledge the message upstream, and you can configure fallback or retry strategies.
2. **Consistent Setpoints**: If the OPC UA server discards or modifies the value, you’ll see an immediate error.

> **Disable** the handshake by setting `handshake.enabled: false` if you prefer no read-back check (faster, but less safe).

**Example**: If you disable the handshake:

```yaml
handshake:
  enabled: false
```

The plugin will write to the OPC UA server but **not** confirm. It will “succeed” as soon as the write request is sent.

---

#### Implementation Details & Future Outlook

For many industrial use cases, you might need more than just writing a value and reading it back:

1. **De-duplication**: If you re-send the same “command” multiple times, do you want the PLC to ignore duplicates?

   - _Now_: Implement a unique command ID (UUID) in your message and let the PLC store/ignore duplicates. Or handle it in your Benthos pipeline (e.g., a “dedupe” processor).
   - _Future_: We may add a built-in “ActionUUID” handshake, which compares a known ID in another read node.

2. **Time-Window Checks**: Only accept a command if it arrives before a certain expiration.

   - _Now_: Use a preceding [nodered_js processor](#node-red-javascript-processor) or Bloblang to drop the message if `timestamp_now - msg.timestamp > threshold`.
   - _Future_: We might add plugin-level config like `rejectOlderThanMs` if demand arises.

3. **Separate Acknowledgment Node**: Some PLCs use a separate ack node (e.g., `CommandAck`) that signals the command was _processed_.
   - _Now_: Implement in the PLC + a custom “double read” with a second plugin instance (or a separate input that waits for the ack).
   - _Future_: We may add an advanced handshake config that reads a different node (rather than the same node) and checks for a specific “ACK” value.

With Benthos, the “at least once” acknowledgment ensures that if writing fails, the message can be retried or routed. This plugin’s minimal default handshake (read-back from the same node) is a strong start for safer OPC UA setpoints, and we’ll grow it over time if more advanced scenarios are needed.

</details>

<details>
<summary>
S7comm
</summary>

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
    rack: 0 # Rack number of the PLC. Defaults to 0
    slot: 1 # Slot number of the PLC. Defaults to 1
    batchMaxSize: 480 # Maximum number of addresses per batch request. Defaults to 480
    timeout: 10 # Timeout in seconds for connections and requests. Default to 10
    disableCPUInfo: false # Set this to true to not fetch CPU information from the PLC. Should be used when you get the error 'Failed to get CPU information'
    addresses: # List of addresses to read from
      - 'DB1.DW20' # Accesses a double word at location 20 in data block 1
      - 'DB1.S30.10' # Accesses a 10-byte string at location 30 in data block 1
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

</details>
<details>
<summary>
Modbus
</summary>

### Modbus

The Modbus plugin facilitates communication with various types of Modbus devices. It supports reading from four types of registers: coils, discrete inputs, holding registers, and input registers. Each data item configuration requires specifying the register type, address, and the data type to be read. The plugin supports multiple data types including integers, unsigned integers, floats, and strings across different sizes and formats.

Data reads can be configured to occur at a set interval, allowing for consistent data polling. Advanced features like register optimization and workarounds for device-specific quirks are also supported to enhance communication efficiency and compatibility.

#### Metadata Outputs

For each read operation, the plugin outputs detailed metadata that includes various aspects of the read operation, which can be utilized to effectively tag, organize, and utilize the data within a system. This metadata encompasses identifiers, data types, and register specifics to ensure precise tracking and utilization of the Modbus data.

Below is the extended metadata output schema provided by the plugin:

| Metadata                   | Description                                                                     |
| -------------------------- | ------------------------------------------------------------------------------- |
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
      - name: 'firstFlagOfDiscreteInput'
        register: 'discrete'
        address: 1
        type: 'BIT'
        output: 'BOOL'
      - name: 'zeroElementOfInputRegister'
        register: 'input'
        address: 0
        type: 'UINT16'
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
      - name: 'firstFlagOfDiscreteInput'
        register: 'discrete'
        address: 1
        type: 'BIT'
        output: 'BOOL'
      - name: 'zeroElementOfInputRegister'
        register: 'input'
        address: 0
        type: 'UINT16'
```

1. **Name**

- **Description**: Identifier for the data point being configured.
- **Configuration Example**:
  ```yaml
  name: 'TemperatureSensor'
  ```

2. **Register**

- **Description**: Specifies the type of Modbus register to query. Options include "coil", "discrete", "holding", or "input".
- **Default**: "holding"
- **Configuration Example**:
  ```yaml
  register: 'holding'
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
- **Default**: Defaults to FLOAT64 if "scale" is provided and to the input "type" class otherwise (i.e. INT\* -> INT64, etc).
- **Configuration Example**:
  ```yaml
  output: 'FLOAT64'
  ```

</details>
<details>
<summary>
ifm IO-Link Master / "sensorconnect"
</summary>

### ifm IO-Link Master / "sensorconnect"

The SensorConnect plugin facilitates communication with ifm electronic’s IO-Link Masters devices, such as the AL1350 or AL1352 IO-Link Masters.
It also supports EIO404 Bluetooth mesh base stations with EIO344 Bluetooth mesh IO-Link adapters.
It enables the integration of sensor data into Benthos pipelines by connecting to the device over HTTP and processing data from connected sensors, including digital inputs and IO-Link devices.
The plugin handles parsing and interpreting IO-Link data using IODD files, converting raw sensor outputs into human-readable data.

It was previously known as [sensorconnect](https://github.com/united-manufacturing-hub/united-manufacturing-hub/tree/staging/golang/cmd/sensorconnect).

#### Configuration

Below is an example configuration demonstrating all available options for the sensorconnect plugin. This includes settings for device connectivity, IODD API URLs, and detailed device-specific configurations.

```yaml
input:
  sensorconnect:
    device_address: '192.168.0.1' # IP address of the IO-Link Master
    iodd_api: 'https://management.umh.app/iodd' # URL of the IODD API
    devices:
      - device_id: 1234
        vendor_id: 5678
        iodd_url: 'https://example.com/iodd/device1234.xml'
      - device_id: 2345
        vendor_id: 6789
        iodd_url: 'https://example.com/iodd/device2345.xml'
```

#### Configuration Options

##### Device Address

Specifies the IP address of the ifm IO-Link Master device

```yaml
input:
  sensorconnect:
    device_address: '192.168.0.1'
```

##### IODD API

Defines the URL of the IODD API, which is used to fetch IODD files for connected devices. Defaults to `https://management.umh.app/iodd` and should not be changed except for development purposes.

```yaml
input:
  sensorconnect:
    iodd_api: 'https://management.umh.app/iodd'
```

##### Devices

Provides a list of devices to provide for a given device_id and vendor_id, a fallback iodd_url (in case the IODD file is not available via the IODD API).

```yaml
input:
  sensorconnect:
    devices:
      - device_id: 509 # Device ID of the IO-Link device
        vendor_id: 2035 # Vendor ID of the IO-Link device
        iodd_url: 'https://yourserver.com/iodd/KEYENCE-FD-EPA1-20230410-IODD1.1.xml' # URL of the IODD file for the device. You might need to download this from the vendors website and self-host it.
```

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

For each read operation, the plugin outputs detailed metadata that includes various aspects of the read operation, which can be utilized to effectively tag, organize, and utilize the data within a system.

Below is the extended metadata output schema provided by the plugin:

| Metadata                                 | Description                                                       |
| ---------------------------------------- | ----------------------------------------------------------------- |
| `sensorconnect_port_mode`                | The mode of the port, e.g., digital-input or io-link.             |
| `sensorconnect_port_number`              | The number of the port on the ifm IO-Link Master device.          |
| `sensorconnect_port_iolink_vendor_id`    | The IO-Link vendor ID of the connected device (if applicable).    |
| `sensorconnect_port_iolink_device_id`    | The IO-Link device ID of the connected device (if applicable).    |
| `sensorconnect_port_iolink_product_name` | The product name of the connected IO-Link device (if applicable). |
| `sensorconnect_port_iolink_serial`       | The serial number of the connected IO-Link device.                |
| `sensorconnect_device_product_code`      | The product code of the connected IO-Link device.                 |
| `sensorconnect_device_serial_number`     | The serial number of the connected IO-Link device                 |

</details>
<details>
<summary>
Beckhoff ADS
</summary>

### Beckhoff ADS

Input for Beckhoff's ADS protocol. Supports batch reading and notifications. Beckhoff recommends limiting notifications to approximately 500 to avoid overloading the controller.
This input only supports symbols and not direct addresses.

**This plugin is community supported only. If you encounter any issues, check out the [original repository](https://github.com/RuneRoven/benthosADS) for more information, or ask around in our Discord.**

```yaml
---
input:
  ads:
    targetIP: '192.168.3.70' # IP address of the PLC
    targetAMS: '5.3.69.134.1.1' # AMS net ID of the target
    targetPort: 48898 # Port of the target internal gateway
    runtimePort: 801 # Runtime port of PLC system
    hostAMS: '192.168.56.1.1.1' # Host AMS net ID. Usually the IP address + .1.1
    hostPort: 10500 # Host port
    readType: 'interval' # Read type, interval or notification
    maxDelay: 100 # Max delay for sending notifications in ms
    cycleTime: 100 # Cycle time for notification handler in ms
    intervalTime: 1000 # Interval time for reading in ms
    upperCase: true # Convert symbol names to all uppercase for older PLCs
    logLevel: 'disabled' # Log level for ADS connection
    symbols: # List of symbols to read from
      - 'MAIN.MYBOOL' # variable in the main program
      - 'MAIN.MYTRIGGER:0:10' # variable in the main program with 0ms max delay and 10ms cycleTime
      - 'MAIN.SPEEDOS'
      - '.superDuperInt' # Global variable
      - '.someStrangeVar'

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
- **runtimePort**: Runtime port of PLC system, 800 to 899. Twincat 2 uses ports 800 to 850, while Twincat 3 is recommended to use ports 851 to 899. Twincat 2 usually have 801 as default and Twincat 3 uses 851
- **hostAMS**: Host AMS net ID. Usually the IP address + .1.1
- **hostPort**: Host port
- **readType**: Read type for the symbols. Interval means benthos reads all symbols at a specified interval and notification is a function in the PLC where benthos sends a notification request to the PLC and the PLC adds the symbol to its internal notification system and sends data whenever there is a change.
- **maxDelay**: Default max delay for sending notifications in ms. Sets a maximum time for how long after the change the PLC must send the notification
- **cycleTime**: Default cycle time for notification handler in ms. Tells the notification handler how often to scan for changes. For symbols like triggers that is only true or false for 1 PLC cycle it can be necessary to use a low value.
- **intervalTime**: Interval time for reading in ms. For reading batches of symbols this sets the time between readings
- **upperCase**: Converts symbol names to all uppercase for older PLCs. For Twincat 2 this is often necessary.
- **logLevel**: Log level for ADS connection sets the log level of the internal log function for the underlying ADS library
- **symbols**: List of symbols to read from in the format <function.variable:maxDelay:cycleTime>, e.g., "MAIN.MYTRIGGER:0:10" is a variable in the main program with 0ms max delay and 10ms cycle time, "MAIN.MYBOOL" is a variable in the main program with no extra arguments, so it will use the default max delay and cycle time. ".superDuperInt" is a global variable with no extra arguments. All global variables must start with a <.> e.g., ".someStrangeVar"

#### Output

Similar to the OPC UA input, this outputs for each address a single message with the payload being the value that was read. To distinguish messages, you can use meta("symbol_name") in a following benthos bloblang processor.

</details>

<details>
<summary>
Node-RED JavaScript Processor
</summary>

### Node-RED JavaScript Processor

The Node-RED JavaScript processor allows you to write JavaScript code to process messages in a style similar to Node-RED function nodes. This makes it easy to port existing Node-RED functions to Benthos or write new processing logic using familiar JavaScript syntax.

Use the `nodered_js` processor instead of the `tag_processor` when you need full control over the payload and require custom processing logic that goes beyond standard tag or time series data handling. This processor allows you to write custom JavaScript code to manipulate both the payload and metadata, providing the flexibility to implement complex transformations, conditional logic, or integrate with other systems.

#### Configuration

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // Your JavaScript code here
          return msg;
```

#### Message Format

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
msg.payload // accesses the message content

// Metadata is in msg.meta
msg.meta.some_key // accesses metadata
```

The processor automatically converts between these formats.

#### Examples

1. **Pass Through Message**
   Input message:

```json
{
  "temperature": 25.5,
  "humidity": 60
}
```

Metadata:

```yaml
sensor_id: 'temp_1'
location: 'room_a'
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

2. **Modify Message Payload**
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

3. **Create New Message**
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

4. **Drop Messages (Filter)**
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

5. **Working with Metadata**
   Input message:

```json
{ "value": 42 }
```

Metadata:

```yaml
source: 'sensor_1'
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
source: 'modified-sensor_1'
processed: 'true'
count: '1'
```

Equivalent Bloblang:

```coffee
meta processed = "true"
meta count = "1"
meta source = "modified-" + meta("source")
```

6. **String Manipulation**
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

7. **Numeric Operations**
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

8. **Logging**
   Input message:

```json
{
  "sensor": "temp_1",
  "value": 25.5
}
```

Metadata:

```yaml
timestamp: '2024-03-12T12:00:00Z'
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

#### Performance Comparison

When choosing between Node-RED JavaScript and Bloblang for message processing, consider the performance implications. Here's a benchmark comparison of both processors performing a simple operation (doubling a number) on 1000 messages:

**JavaScript Processing:**

- Median: 15.4ms
- Mean: 20.9ms
- Standard Deviation: 9.4ms
- Range: 13.8ms - 39ms

**Bloblang Processing:**

- Median: 3.7ms
- Mean: 4ms
- Standard Deviation: 800µs
- Range: 3.3ms - 5.6ms

**Key Observations:**

1. Bloblang is approximately 4-5x faster for simple operations
2. Bloblang shows more consistent performance (smaller standard deviation)
3. However, considering typical protocol converter workloads (around 1000 messages/second), the performance difference is negligible for most use cases. The JavaScript processor's ease of use and familiarity often outweigh the performance benefits of Bloblang, especially for smaller user-generated flows.

Note that these benchmarks represent a simple operation. The performance difference may vary with more complex transformations or when using advanced JavaScript features.

</details>
<details>
<summary>
Tag Processor
</summary>

### Tag Processor

The Tag Processor is designed to prepare incoming data for the UMH data model. It processes messages through three configurable stages: defaults, conditional transformations, and advanced processing, all using a Node-RED style JavaScript environment.

Use the `tag_processor` compared to the `nodered_js` when you are processing tags or time series data and converting them to the UMH data model within the `_historian` data contract. This processor is optimized for handling structured time series data, automatically formats messages, and generates appropriate metadata.

#### Message Formatting Behavior

The processor automatically formats different input types into a consistent structure with a "value" field:

1. **Simple Values (numbers, strings, booleans)**
   Input:

```json
42
```

Output:

```json
{
  "value": 42
}
```

Input:

```json
"test string"
```

Output:

```json
{
  "value": "test string"
}
```

Input:

```json
true
```

Output:

```json
{
  "value": true
}
```

2. **Arrays** (converted to string representation)
   Input:

```json
["a", "b", "c"]
```

Output:

```json
{
  "value": "[a b c]"
}
```

3. **Objects** (preserved as JSON objects)
   Input:

```json
{
  "key1": "value1",
  "key2": 42
}
```

Output:

```json
{
  "value": {
    "key1": "value1",
    "key2": 42
  }
}
```

4. **Numbers** (preserved as numbers)
   Input:

```json
23.5
```

Output:

```json
{
  "value": 23.5
}
```

Input:

```json
42
```

Output:

```json
{
  "value": 42
}
```

This consistent formatting ensures that:

- All messages have a "value" field
- Simple types (numbers, strings, booleans) are preserved as-is
- Complex types (arrays, objects) are converted to their string representations
- Numbers are always preserved as numeric types (integers or floats)

#### Configuration

```yaml
pipeline:
  processors:
    - tag_processor:
        defaults: |

          // Set default location hierarchy and datacontract
          msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name = "value";
          msg.payload = msg.payload; //does not modify the payload
          return msg;
        conditions:
          - if: msg.meta.opcua_node_id === "ns=1;i=2245"
            then: |
              // Set path hierarchy and tag name for specific OPC UA node
              msg.meta.virtual_path = "axis.x.position";
              msg.meta.tag_name = "actual";
              return msg;
        advancedProcessing: |
          // Optional advanced message processing
          // Example: double numeric values
          msg.payload = parseFloat(msg.payload) * 2;
          return msg;
```

#### Processing Stages

1. **Defaults**

   - Sets initial metadata values
   - Runs first on every message
   - Must return a message object

2. **Conditions**

   - List of conditional transformations
   - Each condition has an `if` expression and a `then` code block
   - Runs after defaults
   - Must return a message object

3. **Advanced Processing**
   - Optional final processing stage
   - Can modify both metadata and payload
   - Must return a message object

#### Metadata Fields

The processor uses the following metadata fields:

**Required Fields:**

- `location_path`: Hierarchical location path in dot notation (e.g., "enterprise.site.area.line.workcell.plc123")
- `data_contract`: Data schema identifier (e.g., "\_historian", "\_analytics")
- `tag_name`: Name of the tag/variable (e.g., "temperature", "pressure")

**Optional Fields:**

- `virtual_path`: Logical, non-physical grouping path in dot notation (e.g., "axis.x.position")

**Generated Fields:**

- `topic`: Automatically generated from the above fields in the format:
  ```
  umh.v1.<location_path>.<data_contract>.<virtual_path>.<tag_name>
  ```
  Empty or undefined fields are skipped, and dots are normalized.

#### Message Structure

Messages in the Tag Processor follow the Node-RED style format:

```javascript
{
  payload: {
    // The message content - can be a simple value or complex object
    "temperature": 23.5,
    "timestamp_ms": 1733903611000
  },
  meta: {
    // Required fields
    location_path: "enterprise.site.area.line.workcell.plc123",  // Hierarchical location path
    data_contract: "_historian",                                 // Data schema identifier
    tag_name: "temperature",                                     // Name of the tag/variable

    // Optional fields
    virtual_path: "axis.x.position",                            // Logical grouping path

    // Generated field (by processor)
    topic: "umh.v1.enterprise.site.area.line.workcell.plc123._historian.axis.x.position.temperature",

    // Input-specific fields (e.g., from OPC UA)
    opcua_node_id: "ns=1;i=2245",
    opcua_tag_name: "temperature_sensor_1",
    opcua_tag_group: "sensors.temperature",
    opcua_tag_path: "sensors.temperature",
    opcua_tag_type: "number",
    opcua_source_timestamp: "2024-03-12T10:00:00Z",
    opcua_server_timestamp: "2024-03-12T10:00:00.001Z",
    opcua_attr_nodeid: "ns=1;i=2245",
    opcua_attr_nodeclass: "Variable",
    opcua_attr_browsename: "Temperature",
    opcua_attr_description: "Temperature Sensor 1",
    opcua_attr_accesslevel: "CurrentRead",
    opcua_attr_datatype: "Double"
  }
}
```

#### Examples

1. **Basic Defaults Processing**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "actual";
    return msg;
```

Input:

```json
23.5
```

Output:

```json
{
  "actual": 23.5,
  "timestamp_ms": 1733903611000
}
```

Topic: `umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.actual`

2. **OPC UA Node ID Based Processing**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
    msg.meta.data_contract = "_historian";
    return msg;
  conditions:
    - if: msg.meta.opcua_attr_nodeid === "ns=1;i=2245"
      then: |
        msg.meta.virtual_path = "axis.x.position";
        msg.meta.tag_name = "actual";
        return msg;
```

Input with metadata `opcua_attr_nodeid: "ns=1;i=2245"`:

```json
23.5
```

Output:

```json
{
  "actual": 23.5,
  "timestamp_ms": 1733903611000
}
```

Topic: `umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.axis.x.position.actual`

3. **Moving Folder Structures in Virtual Path**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.plant1";
    msg.meta.data_contract = "_historian";
    msg.meta.virtual_path = msg.meta.opcua_tag_path;
    msg.meta.tag_name = msg.meta.opcua_tag_name;
    return msg;
  conditions:
    # Move the entire DataAccess_AnalogType folder and its children into axis.x
    - if: msg.meta.opcua_tag_path && msg.meta.opcua_tag_path.includes("DataAccess_AnalogType")
      then: |
        msg.meta.location_path += ".area1.machining_line.cnc5.plc123";
        msg.meta.virtual_path = "axis.x." + msg.meta.opcua_tag_path;
        return msg;
```

Input messages with OPC UA tags:

```javascript
// Original tag paths from OPC UA:
// DataAccess_AnalogType
// DataAccess_AnalogType.EURange
// DataAccess_AnalogType.Min
// DataAccess_AnalogType.Max
```

Output topics will be:

```
umh.v1.enterprise.plant1.area1.machining_line.cnc5.plc123._historian.axis.x.DataAccess_AnalogType
umh.v1.enterprise.plant1.area1.machining_line.cnc5.plc123._historian.axis.x.DataAccess_AnalogType.EURange
umh.v1.enterprise.plant1.area1.machining_line.cnc5.plc123._historian.axis.x.DataAccess_AnalogType.Min
umh.v1.enterprise.plant1.area1.machining_line.cnc5.plc123._historian.axis.x.DataAccess_AnalogType.Max
```

This example shows how to:

- Match an entire folder structure using `includes("DataAccess_AnalogType")`
- Move all matching nodes into a new virtual path prefix (`axis.x`)
- Preserve the original folder hierarchy under the new location
- Apply consistent location path for the entire folder structure

4. **Advanced Processing with getLastPayload**

getLastPayload is a function that returns the last payload of a message that was avaialble in Kafka. Remember that you will get the full payload, and might still need to extract the value you need.

**This is not yet implemented, but will be available in the future.**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area.line.workcell";
    msg.meta.data_contract = "_analytics";
    msg.meta.virtual_path = "work_order";
    return msg;
  advancedProcessing: |
    msg.payload = {
      "work_order_id": msg.payload.work_order_id,
      "work_order_start_time": umh.getLastPayload("enterprise.site.area.line.workcell._historian.workorder.work_order_start_time").work_order_start_time,
      "work_order_end_time": umh.getLastPayload("enterprise.site.area.line.workcell._historian.workorder.work_order_end_time").work_order_end_time
    };
    return msg;
```

Input:

```json
{
  "work_order_id": "WO123"
}
```

Output:

```json
{
  "work_order_id": "WO123",
  "work_order_start_time": "2024-03-12T10:00:00Z",
  "work_order_end_time": "2024-03-12T18:00:00Z"
}
```

Topic: `umh.v1.enterprise.site.area.line.workcell._analytics.work_order`

4. **Dropping Messages Based on Value**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    return msg;
  advancedProcessing: |
    if (msg.payload < 0) {
      // Drop negative values
      return null;
    }
    return msg;
```

Input:

```json
-10
```

Output: Message is dropped (no output)

Input:

```json
10
```

Output:

```json
{
  "temperature": 10,
  "timestamp_ms": 1733903611000
}
```

Topic: `umh.v1.enterprise._historian.temperature`

5. **Duplicating Messages for Different Data Contracts**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    return msg;
  conditions:
    - if: true
      then: |
        msg.meta.location_path += ".production";
        return msg;
  advancedProcessing: |
    // Create two versions of the message:
    // 1. Original value for historian
    // 2. Doubled value for custom
    let doubledValue = msg.payload * 2;

    msg1 = {
      payload: msg.payload,
      meta: { ...msg.meta, data_contract: "_historian" }
    };

    msg2 = {
      payload: doubledValue,
      meta: { ...msg.meta, data_contract: "_custom", tag_name: msg.meta.tag_name + "_doubled" }
    };

    return [msg1, msg2];
```

Input:

```json
23.5
```

Output 1 (Historian):

```json
{
  "temperature": 23.5,
  "timestamp_ms": 1733903611000
}
```

Topic: `umh.v1.enterprise.production._historian.temperature`

Output 2 (custom):

```json
{
  "temperature_doubled": 47,
  "timestamp_ms": 1733903611000
}
```

Topic: `umh.v1.enterprise.production._custom.temperature_doubled`

6. **Processing Full MQTT Message Payload**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.area._workorder";
    msg.meta.data_contract = "_workorder";
    msg.meta.virtual_path = "new";
    msg.meta.tag_name = "maintenance";
    return msg;
```

Input:

```json
{
  "maintenanceSchedule": {
    "eventType": "ScheduledMaintenance",
    "eventId": "SM-20240717-025",
    "timestamp": "2024-07-17T13:00:00Z",
    "equipmentId": "InjectionMoldingMachine5",
    "equipmentName": "Engel Victory 120",
    "scheduledDate": "2024-07-22",
    "maintenanceType": "Preventive",
    "description": "Inspection and cleaning of injection unit and mold.",
    "maintenanceDuration": "6 hours",
    "assignedTo": {
      "employeeId": "EMP-5005",
      "name": "Hans Becker"
    },
    "status": "Scheduled",
    "partsRequired": [
      {
        "partId": "NOZZLE-015",
        "description": "Injection Nozzle",
        "quantity": 1
      }
    ],
    "notes": "Replace worn nozzle to prevent defects."
  }
}
```

Output:

```json
{
  "maintenance": {
    "maintenanceSchedule": {
      "eventType": "ScheduledMaintenance",
      "eventId": "SM-20240717-025",
      "timestamp": "2024-07-17T13:00:00Z",
      "equipmentId": "InjectionMoldingMachine5",
      "equipmentName": "Engel Victory 120",
      "scheduledDate": "2024-07-22",
      "maintenanceType": "Preventive",
      "description": "Inspection and cleaning of injection unit and mold.",
      "maintenanceDuration": "6 hours",
      "assignedTo": {
        "employeeId": "EMP-5005",
        "name": "Hans Becker"
      },
      "status": "Scheduled",
      "partsRequired": [
        {
          "partId": "NOZZLE-015",
          "description": "Injection Nozzle",
          "quantity": 1
        }
      ],
      "notes": "Replace worn nozzle to prevent defects."
    }
  },
  "timestamp_ms": 1733903611000
}
```

Topic: `umh.v1.enterprise.area._workorder.maintenance`

**Note:** In the `tag_processor`, the resulting payload will always include `timestamp_ms` and one additional key corresponding to the `tag_name`. If you need to fully control the resulting payload structure, consider using the `nodered_js` processor instead. You can set the topic and payload manually, as shown below:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // set kafka topic manually
          msg.meta.topic = "umh.v1.enterprise.site.area._workorder.new"

          // only take two fields from the payload
          msg.payload = {
            "maintenanceSchedule": {
              "eventType": msg.payload.maintenanceSchedule.eventType,
              "description": msg.payload.maintenanceSchedule.description
            }
          }
          return msg;
```

</details>

<details>
<summary>
EthernetIP (nightly)
</summary>

### Ethernet/IP

The plugin is designed to read data from configured tags within Rockwell Automation PLCs that support the CIP protocol over Ethernet/IP, such as **ControlLogix**, **CompactLogix**, and **Micro820**. It uses the [`gologix`](https://github.com/danomagnum/gologix) driver—a native Go implementation modeled after pylogix—to establish and maintain communication with the controller.

Currently, the plugin behaves similarly to the `s7comm` plugin: you must explicitly configure the **tag names** and **data types** you want to read. Automatic browsing or discovery of tags is **not yet implemented**. As such, you should already know the tag names and structures from the PLC program (e.g., via Studio 5000 or Connected Components Workbench).

> ⚠️ Support is limited to modern controllers that use **CIP over Ethernet/IP**. Legacy PLCs such as **PLC-5**, **SLC-500**, or **MicroLogix** models using **PCCC** are _not supported_.

In future versions, support for **browsing** and **listing available tags** directly from the controller may be added to improve usability and reduce manual configuration.

#### Datatypes

The plugin is being tested with multiple datatypes, therefore the following datatypes are verified for compatibility:

- `Boolean`
- `Byte`
- `Int8`
- `Int16`
- `Int32`
- `Int64`
- `UInt16`
- `UInt32`
- `UInt64`
- `Float32`
- `Float64`
- `String`
- `Array of Byte`
- `Array of Int8`
- `Array of Int16`
- `Array of Int32`
- `Array of Int64`
- `Array of Uint16`
- `Array of Uint32`
- `Array of Uint64`
- `Array of Float32`
- `Array of Float64`
- `Array of String`

#### Metadata outputs

The plugin provides metadata for each message, that can be used to create a topic for the output, as shown in the example above. The metadata can also be used to create a unique identifier for each message, which is useful for deduplication.

| Metadata       | Description                                                                                                          |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `eip_tag_name` | The Name of the Tag that sent the message. If an alias was specified it will replace the original naming of the tag. |
| `eip_tag_path` | A dot-separated path to the tag, which is usually used for TagSets and Tags.                                         |
| `eip_tag_type` | The data type of the node optimized for benthos, which can be either a number, string or bool.                       |

#### Configuration Options

The following options can be specified in the `benthos.yaml` configuration file:

```yaml
input:
  opcua:
    endpoint: '127.0.0.1'
    pollRate: 1000 # optional (default: 1000) The rate in milliseconds at which to poll the EthernetIP plc
    listAllTags: false | true # (currently not supported)
    useMutliRead: false | true # (currently not supported)
    attributes:
      - path: '1-1-1' # specify the path like `Class-Instance-Attribute`
        type: 'int16'
        alias: 'vendorID' # optional (default: unset)
    tags:
      - name: 'tagName'
        type: 'bool'
        alias: 'tagAlias' # optional (default: unset)
        length: 1 # optional (default: 1) you only need to set this for arrays
```

##### Endpoint

You can specify the endpoint in the configuration file. Node endpoints are automatically discovered and selected based on the authentication method.

```yaml
input:
  opcua:
    endpoint: '127.0.0.1'
    tags:
      - name: 'tagName'
        type: 'bool'
```

##### Attributes

You can specify the Attributes in the configuration file:

```yaml
input:
  opcua:
    endpoint: '127.0.0.1'
    attributes:
      - path: '1-1-1' # specify the path like `Class-Instance-Attribute`
        type: 'int16'
        alias: 'vendorID' # optional (default: unset)
```

- **Key**: `path`
- **Description**: The `path` always consists of this pattern: "Class-Instance-Attribute". Path to the specific attribute you want to read. Attributes are usually used for some device specific information, therefore you have to check your device's manual to check whether a specific attribute exists.
- **Example**: `1-1-1`

- **Key**: `type`
- **Description**: The `type` specifies the attributes type, which has to be set correctly otherwise you will receive an error. You can also get this from your device's manual.
- **Example**: `int16`

- **Key**: `alias`
- **Description**: The `alias` is an option to store your data into a specific name, so you can later easilier access this attribute.
- **Example**: `testAlias`

##### Tags

You can specify the Tags in the configuration file:

```yaml
input:
  opcua:
    endpoint: '127.0.0.1'
    tags:
      - name: 'testInt16' # specify the path like `Class-Instance-Attribute`
        type: 'int16'
        alias: 'counter' # optional (default: unset)
```

- **Key**: `name`
- **Description**: The `name` is basically the name of the tag you want to read data from. This is specified in your device's software and you have to know the name of that.
- **Example**: `testInt16`

- **Key**: `type`
- **Description**: The `type` specifies the tags type, which has to be set correctly otherwise you will receive an error. You can also get this from your device's software.
- **Example**: `int16`

- **Key**: `alias`
- **Description**: The `alias` is an option to store your data into a specific name, so you can later easilier access this tags data.
- **Example**: `testAlias`

- **Key**: `length` (only if you read from a `type` e.g. `arrayofint16`)
- **Description**: The `length` is an option to set the length of the data you want to read. This is only needed when reading arrays. Otherwise you will receive an error here.
- **Example**: 3

**Important:** The functionality of reading tag-sets in specific is not yet implemented and will need further investigation.

##### List all tags (not yet implemented)

This is not yet implemented and currently not set to a specific timeline.

##### MultiRead (not yet implemented)

This is not yet implemented and currently not set to a specific timeline, but will improve reading time out of your plc if it supports this service.

</details>

<details>
<summary>
UNS (Output)
</summary>

### UNS (Output)

> **Works exclusively with UMH Core**
> Inside UMH Core you can leave the block empty — `uns: {}`.
> If you run the plugin elsewhere, at minimum set `broker_address`.

#### 1. Quick-start (99 % of users)

```yaml
pipeline:
  processors:
    - tag_processor: # or Bloblang / Node-RED JS
        defaults: |
          // Minimal example
          msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = "value";
          // tag_processor now auto-creates msg.meta.topic
          return msg;

output:
  uns: {} # nothing else needed on UMH Core
```

| What                    | Default inside UMH Core                                                                                                                  |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **Broker address**      | `localhost:9092` (embedded Redpanda)                                                                                                     |
| **Kafka topic**         | **`umh.messages`** (hard-wired)                                                                                                          |
| **Kafka key**           | `${! meta("topic") }` (created by the `tag_processor` or your own mapping)                                                               |
| **Header `bridged_by`** | `umh-core` – automatically replaced with `protocol-converter-<INSTANCE>-<NAME>` when the container is deployed by the Management Console |

_Open the **Tag Browser** (Management Console → Unified Namespace) to watch the live values._

#### 2. Optional overrides

```yaml
output:
  uns:
    topic: '${! meta("topic") }' # rarely changed
    broker_address: 'redpanda.prod.example:9092'
    bridged_by: 'edge-gw-01' # any free-form ID
```

| Field            | Purpose & Default                                                                                         |
| ---------------- | --------------------------------------------------------------------------------------------------------- |
| `topic`          | The **Kafka key**. Must follow `umh.v1.<…>` naming. Default `${! meta("topic") }`.                        |
| `broker_address` | Comma-separated Kafka/Redpanda bootstrap list. Default `localhost:9092`.                                  |
| `bridged_by`     | Traceability header. Default `umh-core`; overridden automatically by protocol-converters inside UMH Core. |

#### 3. What the plugin does behind the scenes

1. **Batching** 100 messages _or_ 100 ms – whichever comes first.
2. **Sanitising** Illegal chars in the key become “\_”.
3. **Headers** All Benthos metadata (except `kafka_*`) plus `bridged_by` are forwarded as Kafka headers.
4. **Topic check** If `umh.messages` is missing the plugin creates it (1 partition, `compact,delete`).

#### Troubleshooting / FAQs

- **“topic is not set or is empty”** – your pipeline never wrote `msg.meta.topic`.
  Add a `tag_processor` (auto) or a Bloblang line:
  `meta topic = "umh.v1.demo.plant1.line1._historian.temperature"`

</details>

<details>
<summary>
Tag Browser
</summary>

### Tag Browser

> This plugin is designed for internal usage in umh-core

#### 1. What does this plugin do?

1. For each incoming message, this plugin extracts:
   - Topic hierarchy information (level0 through level5)
   - Data contract information
   - Virtual path and event tag (if present)
   - Kafka headers and metadata
   - Payload information and type (timeseries vs non-timeseries)
2. The plugin implements several optimizations:
   - Uses an LRU cache to prevent re-sending unchanged topic metadata
   - Batches multiple messages into a single protobuf message
   - Only includes topics in the output when their metadata has changed
3. The processed data is structured into:
   - A topic map containing hierarchical topic information and metadata
   - An event table containing the actual message data and timestamps
4. The final output is a single protobuf-encoded message containing:
   - A map of all new or changed topics
   - A list of all events processed in the current batch

#### 2. Usage

1. Select the uns-input plugin as input, without any topic filter
2. Use this plugin as the processor plugin
3. Use stdout as output plugin

#### 3. Data Structure

The plugin uses the following protobuf structures:

- `TopicInfo`: Contains hierarchical topic information, including:
  - Level0-Level5
  - Datacontract
  - Virtual_path
  - Event_tag
  - Latest values for each metadata that was ever observed in this topic
- `EventTableEntry`: Contains the actual message data, including:
  - UNS tree ID (hash of the topic info)
  - Payload value
  - Timestamp (for timeseries data)
  - Raw Kafka message data
  - Processing history

#### 4. Performance Considerations

- The plugin uses an LRU cache to minimize traffic by avoiding re-sending unchanged topic metadata
- Message batching is implemented to reduce the number of output messages
- Topic metadata is only updated when changes are detected

</details>

## Testing

We execute automated tests and verify that benthos-umh works against various targets. All tests are started with `make test`, but might require environment parameters in order to not be skipped.

Some of these tests are executed with a local GitHub runner called "hercules", which is connected to an isolated testing network.

### Target: WAGO PFC100 (OPC UA)

- Model number: 750-8101 PFC100 CS 2ETH
- Firmware: 03.10.08(22)
- OPC-UA-Server Version: 1.3.1

Requires:

```bash
TEST_WAGO_ENDPOINT_URI="opc.tcp://your_wago_endpoint_uri:port"
TEST_WAGO_USERNAME="your_wago_username"
TEST_WAGO_PASSWORD="your_wago_password"
```

### Target: Microsoft OPC UA Simulator (OPC UA)

- Docker tag: mcr.microsoft.com/iotedge/opc-plc:2.9.11

Requires:

```bash
TEST_OPCUA_SIMULATOR="opc.tcp://localhost:50000"
```

### Target: Prosys OPC UA Simulator (OPC UA)

- Version: 5.4.6-148

Requires:

```bash
TEST_PROSYS_ENDPOINT_URI="opc.tcp://your_prosys_endpoint:port"
```

This requires additional to have the simulator setup somewhere (e.g., locally on your PC) and pointing the test towards it. This is not included in any CI and must be run manually.

### Target: Siemens S7-1200 (OPC UA)

- Model number: SIMATIC S7-1200 (6ES7211-1AE40-0XB0)
- Firmware: v4.4

Requires:

```bash
TEST_S7_ENDPOINT_URI="opc.tcp://your_s7_endpoint_uri:port"
```

### Target: Unit Tests (OPC UA)

Requires:

```bash
TEST_OPCUA_UNITTEST=true
```

### Target: Siemens S7-1200 (S7comm)

- Model number: SIMATIC S7-1200 (6ES7211-1AE40-0XB0)
- Firmware: v4.4

Requires:

```bash
TEST_S7_TCPDEVICE="your_s7_ip:port"
TEST_S7_RACK=0
TEST_S7_SLOT=1
```

The rack and slotnumbers are just an example. Ensure to pick the matching ones for your test-setup.

### Target: Unit Tests (S7comm)

Requires:

```bash
TEST_S7COMM_UNITTEST=true
```

## License

All source code is distributed under the APACHE LICENSE, VERSION 2.0. See LICENSE for more information.

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Funited-manufacturing-hub%2Fbenthos-umh.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Funited-manufacturing-hub%2Fbenthos-umh?ref=badge_large)

## Contact

Feel free to provide us feedback on our [Discord channel](https://discord.gg/F9mqkZnm9d).

For more information about the United Manufacturing Hub, visit [UMH Systems GmbH](https://www.umh.app). If you haven't worked with the United Manufacturing Hub before, [give it a try](https://management.umh.app)! Setting it up takes only a matter of minutes.
