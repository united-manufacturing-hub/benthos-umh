# ifm IO-Link Master / "sensorconnect"

The SensorConnect plugin facilitates communication with ifm electronic’s IO-Link Masters devices, such as the AL1350 or AL1352 IO-Link Masters.\


It also supports EIO404 Bluetooth mesh base stations with EIO344 Bluetooth mesh IO-Link adapters.\
It enables the integration of sensor data into Benthos pipelines by connecting to the device over HTTP and processing data from connected sensors, including digital inputs and IO-Link devices.\
The plugin handles parsing and interpreting IO-Link data using IODD files, converting raw sensor outputs into human-readable data.

It was previously known as [sensorconnect](https://github.com/united-manufacturing-hub/united-manufacturing-hub/tree/staging/golang/cmd/sensorconnect).

**Configuration**

Below is an example configuration demonstrating all available options for the sensorconnect plugin. This includes settings for device connectivity, IODD API URLs, and detailed device-specific configurations.

```yaml
input:
  sensorconnect:
    device_address: '192.168.0.1' # IP address of the IO-Link Master
    iodd_api: 'https://management.umh.app/iodd' # URL of the IODD API
    devices:
      - device_id: 1234
        vendor_id: 5678
        iodd_url: "https://example.com/iodd/device1234.xml"
      - device_id: 2345
        vendor_id: 6789
        iodd_url: "https://example.com/iodd/device2345.xml"
```

**Configuration Options**

**Device Address**

Specifies the IP address of the ifm IO-Link Master device

```yaml
input:
  sensorconnect:
    device_address: '192.168.0.1'
```

**IODD API**

Defines the URL of the IODD API, which is used to fetch IODD files for connected devices. Defaults to `https://management.umh.app/iodd` and should not be changed except for development purposes.

```yaml
input:
  sensorconnect:
    iodd_api: 'https://management.umh.app/iodd'
```

**Devices**

Provides a list of devices to provide for a given device\_id and vendor\_id, a fallback iodd\_url (in case the IODD file is not available via the IODD API).

```yaml
input:
  sensorconnect:
    devices:
      - device_id: 509 # Device ID of the IO-Link device
        vendor_id: 2035 # Vendor ID of the IO-Link device
        iodd_url: "https://yourserver.com/iodd/KEYENCE-FD-EPA1-20230410-IODD1.1.xml" # URL of the IODD file for the device. You might need to download this from the vendors website and self-host it.
```

**Output**

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

**Metadata Outputs**

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
