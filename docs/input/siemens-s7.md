# Siemens S7

This input is tailored for the S7 communication protocol, facilitating a direct connection with S7-300, S7-400, S7-1200, and S7-1500 series PLCs.

For more modern PLCs like the S7-1200 and S7-1500 the following two changes need to be done to use them:

1. "Optimized block access" must be disabled for the DBs we want to access
2. In the "Protection" section of the CPU Properties, enable the "Permit access with PUT/GET" checkbox

**Configuration**

```yaml
input:
  s7comm:
    tcpDevice: '192.168.0.1' # IP address of the S7 PLC (optionally with port, e.g., '192.168.0.1:102')
    rack: 0                  # Rack number of the PLC. Defaults to 0
    slot: 1                  # Slot number of the PLC. Defaults to 1
    batchMaxSize: 480        # DEPRECATED: This field never worked correctly because batches were calculated before connecting to the PLC, so the actual negotiated PDU size was unknown. PDU size is now automatically negotiated during connection.
    timeout: 10              # Timeout in seconds for connections and requests. Default to 10
    disableCPUInfo: false    # Set this to true to not fetch CPU information from the PLC
    addresses:               # List of addresses to read from
      - "DB1.DW20"     # Accesses a double word at location 20 in data block 1
      - "DB1.S30.10"   # Accesses a 10-byte string at location 30 in data block 1
```

**Configuration Parameters**

* **tcpDevice**: IP address or hostname of the Siemens S7 PLC, optionally with port (e.g., `192.168.0.1:102`). If no port is specified, the default S7 port 102 is used.
* **rack**: Identifies the physical location of the CPU within the PLC rack.
* **slot**: Identifies the specific CPU slot within the rack.
* **timeout**: Timeout duration in seconds for connection attempts and read requests.
* **disableCPUInfo**: Set this to true to not fetch CPU information from the PLC. Should be used when you get the error 'Failed to get CPU information'
* **addresses**: List of PLC memory addresses to read. See [Address Format](#address-format) below.

## Address Format

Each address tells benthos-umh **where** to read in the PLC memory and **what data type** to expect.

**For Data Blocks** (block number required):
```text
DB<number>.<type><offset>[.<extra>]
```
Example: `DB1.DW20` — double word at offset 20 in data block 1.

**For all other areas** (no block number):
```text
<area>.<type><offset>[.<extra>]
```
Example: `PE.W0` — input word at offset 0.

Breaking this down:

| Part | What it means | Example |
|------|---------------|---------|
| `area` | Memory area in the PLC | `DB`, `MK`, `PE`, `PA`, `C`, `T` |
| `number` | Which data block (DB only) | `1` in `DB1` (Data Block 1) |
| `type` | Data type to read | `DW`, `X`, `S`, `R`, etc. |
| `offset` | Byte position within the area | `20` in `DB1.DW20` (starts at byte 20) |
| `extra` | Required for some types only | Bit number for `X`, string length for `S` |

Only **DB** (Data Blocks) uses the block number. For all other areas there is only a single memory region in the PLC, so no block number is needed.

### Memory Areas

| Area | Name | Description | Has blocks? | Example |
|------|------|-------------|-------------|---------|
| `DB` | Data Block | Main data storage — most commonly used | Yes — `DB1`, `DB2`, etc. are different blocks | `DB1.DW20` |
| `PE` | Process Input | Physical inputs (sensors, switches) | No — only one area | `PE.B0` |
| `PA` | Process Output | Physical outputs (actuators, relays) | No — only one area | `PA.W0` |
| `MK` | Merker (Flags) | Internal boolean/word flags | No — only one area | `MK.W0` |
| `C` | Counter | Hardware counters | No — only one area | `C.W0` |
| `T` | Timer | Hardware timers | No — only one area | `T.W0` |

### Data Types

| Type | Name | Size | Output Type | Extra Required? | Example Address |
|------|------|------|-------------|-----------------|-----------------|
| `X` | Bit | 1 bit | `bool` | Yes — bit number (0–7) | `DB1.X5.2` |
| `B` | Byte | 1 byte | `uint8` | No | `DB1.B10` |
| `C` | Char | 1 byte | `string` (single character) | No | `DB1.C10` |
| `W` | Word | 2 bytes | `uint16` | No | `DB1.W20` |
| `I` | Integer | 2 bytes | `int16` (signed) | No | `DB1.I20` |
| `DW` | Double Word | 4 bytes | `uint32` | No | `DB1.DW100` |
| `DI` | Double Integer | 4 bytes | `int32` (signed) | No | `DB1.DI100` |
| `R` | Real | 4 bytes | `float32` | No | `DB1.R200` |
| `DT` | Date/Time | 8 bytes | `int64` (Unix nanoseconds) | No | `DB1.DT0` |
| `S` | String | variable | `string` | Yes — max length (≥ 1) | `DB1.S30.10` |

### The Extra Parameter

Two data types require the extra parameter (the part after the second `.`):

**Bit (`X`) — specify which bit (0–7) within the byte:**
```text
DB1.X5.2
       │ └─ bit 2 (third bit, counting from 0)
       └─── byte offset 5
```
Bit numbering: `0` is the least significant bit, `7` is the most significant.

**String (`S`) — specify the maximum string length:**
```text
DB1.S30.10
        │  └─ max 10 characters
        └──── byte offset 30
```
The PLC stores strings with a 2-byte header (max length + actual length), so `DB1.S30.10` reads 12 bytes starting at offset 30.

All other types must **not** have an extra parameter.

### Examples

```yaml
addresses:
  # Data Block reads
  - "DB1.X0.0"      # Bit 0 of byte 0 — a boolean flag
  - "DB1.DW8"       # Unsigned 32-bit double word at offset 8
  - "DB1.R16"       # 32-bit float at offset 16
  - "DB1.S28.50"    # String of up to 50 chars starting at offset 28

  # Process Inputs (PE) — reading from sensors, switches, etc.
  - "PE.X0.0"       # Input bit 0 — e.g., a digital sensor
  - "PE.X0.7"       # Input bit 7
  - "PE.B2"         # Input byte at offset 2
  - "PE.W4"         # Input word at offset 4 — e.g., an analog sensor value

  # Process Outputs (PA) — reading back output states
  - "PA.X0.0"       # Output bit 0 — e.g., a relay state
  - "PA.W0"         # Output word at offset 0

  # Merker / Flags (MK)
  - "MK.X0.0"       # Merker bit — internal boolean flag
  - "MK.W10"        # Merker word at offset 10
```

### Mapping from TIA Portal

When reading addresses from a TIA Portal project, map them like this:

| TIA Portal | benthos-umh | Notes |
|------------|-------------|-------|
| `DB1.DBX 5.2` | `DB1.X5.2` | Data bit — drop "DB" prefix from type |
| `DB1.DBB 10` | `DB1.B10` | Data byte |
| `DB1.DBW 20` | `DB1.W20` | Data word |
| `DB1.DBD 100` | `DB1.DW100` | Data double word (unsigned) |
| `DB1.DBD 100` | `DB1.DI100` | Data double word (signed) — same offset, different type |
| `DB1.DBD 200` | `DB1.R200` | Data real (float) |
| `M 0.0` | `MK.X0.0` | Merker bit |
| `MW 10` | `MK.W10` | Merker word |
| `I 0.0` | `PE.X0.0` | Input bit |
| `IW 0` | `PE.W0` | Input word |
| `Q 0.0` | `PA.X0.0` | Output bit |
| `QW 0` | `PA.W0` | Output word |

**Output**

Similar to the OPC UA input, this outputs for each address a single message with the payload being the value that was read. To distinguish messages, you can use meta("s7\_address") in a following benthos bloblang processor.

**Batching Behavior**

Addresses are automatically split into batches based on S7 protocol constraints:

- **Max 20 items per request**: The S7 protocol limits AGReadMulti to 20 addresses per request
- **PDU size**: The request and response must fit within the negotiated PDU size (typically 240-480 bytes depending on the PLC model)

When more than 20 addresses are configured or the combined data exceeds PDU limits, multiple sequential requests are made. This has implications:

- **Timing**: Addresses in different batches are read at slightly different times, resulting in different timestamps
- **Performance**: More batches means more round-trips to the PLC, increasing total read time
