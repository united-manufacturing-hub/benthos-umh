# Siemens S7

This input is tailored for the S7 communication protocol, facilitating a direct connection with S7-300, S7-400, S7-1200, and S7-1500 series PLCs.

For more modern PLCs like the S7-1200 and S7-1500 the following two changes need to be done to use them:

1. "Optimized block access" must be disabled for the DBs we want to access
2. In the "Protection" section of the CPU Properties, enable the "Permit access with PUT/GET" checkbox

**Configuration**

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

**Configuration Parameters**

* **tcpDevice**: IP address of the Siemens S7 PLC.
* **rack**: Identifies the physical location of the CPU within the PLC rack.
* **slot**: Identifies the specific CPU slot within the rack.
* **batchMaxSize**: Maximum count of addresses bundled in a single batch request. This affects the PDU size.
* **timeout**: Timeout duration in seconds for connection attempts and read requests.
* **disableCPUInfo**: Set this to true to not fetch CPU information from the PLC. Should be used when you get the error 'Failed to get CPU information'
* **addresses**: Specifies the list of addresses to read. The format for addresses is `<area>.<type><address>[.extra]`, where:
  * `area`: Specifies the direct area access, e.g., "DB1" for data block one. Supported areas include inputs (`PE`), outputs (`PA`), Merkers (`MK`), DB (`DB`), counters (`C`), and timers (`T`).
  * `type`: Indicates the data type, such as bit (`X`), byte (`B`), word (`W`), double word (`DW`), integer (`I`), double integer (`DI`), real (`R`), date-time (`DT`), and string (`S`). Some types require an 'extra' parameter, e.g., the bit number for `X` or the maximum length for `S`.

**Output**

Similar to the OPC UA input, this outputs for each address a single message with the payload being the value that was read. To distinguish messages, you can use meta("s7\_address") in a following benthos bloblang processor.
