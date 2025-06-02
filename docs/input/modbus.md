# Modbus

The Modbus plugin facilitates communication with various types of Modbus devices. It supports reading from four types of registers: coils, discrete inputs, holding registers, and input registers. Each data item configuration requires specifying the register type, address, and the data type to be read. The plugin supports multiple data types including integers, unsigned integers, floats, and strings across different sizes and formats.

Data reads can be configured to occur at a set interval, allowing for consistent data polling. Advanced features like register optimization and workarounds for device-specific quirks are also supported to enhance communication efficiency and compatibility.

**Metadata Outputs**

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

**Configuration Options**

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

**Controller**

Specifies the network address of the Modbus controller:

```yaml
input:
  modbus:
    controller: 'tcp://localhost:502'
```

**Transmission Mode**

Defines the Modbus transmission mode. Can be "TCP" (default), "RTUOverTCP", "ASCIIOverTCP":

```yaml
input:
  modbus:
    transmissionMode: 'TCP'
```

**Slave IDs**

Configure the modbus slave IDs :

```yaml
input:
  modbus:
    slaveIDs:
      - 1
      - 2
```

For backward compatibility, there is also `slaveID: 1`, which allows setting only a single Modbus slave.

**Retry Settings & Timeout**

Configurations to handle retries in case of communication failures:

```yaml
input:
  modbus:
    busyRetries: 3
    busyRetriesWait: '200ms'
    timeout: '1s'
```

**Time Between Reads**

Defines how frequently the Modbus device should be polled:

```yaml
input:
  modbus:
    timeBetweenReads: '1s'
```

**Optimization**

The Modbus plugin offers several strategies to optimize data read requests, enhancing efficiency and reducing network load when interacting with Modbus devices. These strategies are designed to adjust the organization and batching of requests based on device capabilities and network conditions.

The available optimization strategies are:

* **none**: This is the default setting where no optimization is applied. The plugin groups read requests according to the defined metrics without further optimization. Suitable for systems with simple setups or minimal performance requirements.
* **max\_insert**: Enhances efficiency by collating read requests across all defined metrics and filling in gaps (non-consecutive registers) to minimize the total number of requests. This strategy is ideal for complex systems with numerous data points, as it significantly reduces network traffic and processing time.
* **shrink**: Reduces the size of each request by stripping leading and trailing fields marked with an omit flag. This can decrease the overall data payload and improve processing times, especially when many fields are optional or conditional.
* **rearrange**: Allows rearranging fields between requests to reduce the number of registers accessed while maintaining the minimal number of requests. This strategy optimizes the order of fields to minimize the spread across registers.
* **aggressive**: Similar to "rearrange" but allows mixing of groups. This approach may reduce the number of requests at the cost of accessing more registers, potentially touching more data than necessary to consolidate requests.

Each strategy can be tailored with parameters such as `OptimizationMaxRegisterFill` to control how aggressively the system attempts to optimize data reads. For example, the `max_insert` option can be configured to limit the number of additional registers filled to reduce gaps:

```yaml
input:
  modbus:
    optimization: 'max_insert'
    optimizationMaxRegisterFill: 10
```

Additional Configuration for Optimization Strategies:

* **OptimizationMaxRegisterFill**: Specifies the maximum number of registers the optimizer is allowed to insert between non-consecutive registers in the `max_insert` strategy.

**Byte Order**

The `byteOrder` configuration specifies how bytes within the registers are ordered, which is essential for correctly interpreting the data read from a Modbus device. Different devices or systems may represent multi-byte data types (like integers and floating points) in various byte orders. The options are:

* **ABCD**: Big Endian (Motorola format) where the most significant byte is stored first.
* **DCBA**: Little Endian (Intel format) where the least significant byte is stored first.
* **BADC**: Big Endian with byte swap where bytes are stored in a big-endian order but each pair of bytes is swapped.
* **CDAB**: Little Endian with byte swap where bytes are stored in little-endian order with each pair of bytes swapped.

```yaml
input:
  modbus:
    byteOrder: 'ABCD'
```

**Modbus Workaround**

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

* **Description**: Introduces a delay before sending the first request after establishing a connection.
* **Purpose**: This is particularly useful for slow devices that need time to stabilize a connection before they can process requests.
* **Default**: `0s`
*   **Configuration Example**:

    ```yaml
    pauseAfterConnect: '500ms'
    ```

2. **One Request Per Field**

* **Description**: Configures the system to send each field request separately.
* **Purpose**: Some devices may have limitations that prevent them from handling multiple field requests in a single Modbus transaction. Isolating requests ensures compatibility.
* **Default**: `false`
*   **Configuration Example**:

    ```yaml
    oneRequestPerField: true
    ```

3. **Read Coils Starting at Zero**

* **Description**: Adjusts the starting address for reading coils to begin at address 0 instead of 1.
* **Purpose**: Certain devices may map their coil addresses starting from 0, which is non-standard but not uncommon.
* **Default**: `false`
*   **Configuration Example**:

    ```yaml
    readCoilsStartingAtZero: true
    ```

4. **Time Between Requests**

* **Description**: Sets the minimum interval between consecutive requests to the same device.
* **Purpose**: Prevents the overloading of Modbus devices by spacing out the requests, which is critical in systems where devices are sensitive to high traffic.
* **Default**: `0s`
*   **Configuration Example**:

    ```yaml
    timeBetweenRequests: '100ms'
    ```

5. **String Register Location**

* **Description**: Specifies which part of the register to use for string data after byte-order conversion.
* **Options**:
  * `lower`: Uses only the lower byte of each register.
  * `upper`: Uses only the upper byte of each register.
  * If left empty, both bytes of the register are used.
* **Purpose**: Some devices may place string data only in specific byte locations within a register, necessitating this adjustment for correct string interpretation.
* **Default**: Both bytes used.
*   **Configuration Example**:

    ```yaml
    stringRegisterLocation: 'upper'
    ```

**Addresses**

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

* **Description**: Identifier for the data point being configured.
*   **Configuration Example**:

    ```yaml
    name: "TemperatureSensor"
    ```

2. **Register**

* **Description**: Specifies the type of Modbus register to query. Options include "coil", "discrete", "holding", or "input".
* **Default**: "holding"
*   **Configuration Example**:

    ```yaml
    register: "holding"
    ```

3. **Address**

* **Description**: The Modbus register address from which data should be read.
*   **Configuration Example**:

    ```yaml
    address: 3
    ```

4. **Type**

* **Description**: Specifies the data type of the field, which determines how the data read from the register is interpreted. This setting is crucial as it affects how the raw data from Modbus registers is processed and used. The available data types cater to various data resolutions and formats, ranging from single-bit signals to full 64-bit precision, including special formats for strings and floating-point numbers.
* **Options**:
  * `BIT`: Single bit of a register.
  * `INT8L`: 8-bit integer (low byte).
  * `INT8H`: 8-bit integer (high byte).
  * `UINT8L`: 8-bit unsigned integer (low byte).
  * `UINT8H`: 8-bit unsigned integer (high byte).
  * `INT16`: 16-bit integer.
  * `UINT16`: 16-bit unsigned integer.
  * `INT32`: 32-bit integer.
  * `UINT32`: 32-bit unsigned integer.
  * `INT64`: 64-bit integer.
  * `UINT64`: 64-bit unsigned integer.
  * `FLOAT16`: 16-bit floating point (IEEE 754).
  * `FLOAT32`: 32-bit floating point (IEEE 754).
  * `FLOAT64`: 64-bit floating point (IEEE 754).
  * `STRING`: A sequence of bytes converted to a string.

5. **Length**

* **Description**: Number of registers to read, primarily used when the data type is "STRING".
* **Default**: 0
*   **Configuration Example**:

    ```yaml
    length: 2
    ```

6. **Bit**

* **Description**: Relevant only for BIT data type, specifying which bit of the register to read.
* **Default**: 0
*   **Configuration Example**:

    ```yaml
    bit: 7
    ```

7. **Scale**

* **Description**: A multiplier applied to the numeric data read from the register, used to scale values to the desired range or unit.
* **Default**: 0.0
*   **Configuration Example**:

    ```yaml
    scale: 0.1
    ```

8. **Output**

* **Description**: Specifies the data type of the output field. Options include "INT64", "UINT64", "FLOAT64", or "native" (which retains the original data type without conversion).
* **Default**: Defaults to FLOAT64 if "scale" is provided and to the input "type" class otherwise (i.e. INT\* -> INT64, etc).
*   **Configuration Example**:

    ```yaml
    output: "FLOAT64"
    ```
