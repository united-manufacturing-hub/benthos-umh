# Symbols and metadata

How to address a symbol, and what each message carries once it is read.

## Addressing symbols

Symbols are specified as `name[:opt1[:opt2...]]`. Options are either positional integers or `key=value` pairs. Both forms can be mixed.

| Format | maxDelay | cycleTime |
|--------|----------|-----------|
| `MAIN.var` | default | default |
| `MAIN.var:50:100` | 50 | 100 |
| `MAIN.var:50` | 50 | default |
| `MAIN.var::100` | default | 100 |
| `MAIN.var:cycleTime=100` | default | 100 |
| `MAIN.var:maxDelay=50` | 50 | default |
| `MAIN.var:50:cycleTime=100` | 50 (positional) | 100 (key) |
| `MAIN.var:30:maxDelay=50` | 50 (key overrides positional 30) | default |
| `MAIN.var:maxDelay=50:cycleTime=100` | 50 | 100 |

**Rules:**
- Positional integers fill `maxDelay` then `cycleTime` in order
- An empty slot (`::`) reserves the position but keeps the default — use to skip `maxDelay` and set only `cycleTime`
- `key=value` options override by name and do not consume a positional slot
- A keyed option always wins over a positional option for the same field
- Invalid or omitted values fall back to the plugin-level `maxDelay`/`cycleTime` defaults

**Examples:**
- `MAIN.MYBOOL` — uses plugin-level defaults for both
- `MAIN.MYTRIGGER:0:10` — 0ms max delay, 10ms cycle time
- `MAIN.MYSENSOR::10` — default max delay, 10ms cycle time
- `.superDuperInt` — global variable (TC2, must start with `.`)

**TwinCAT 3** qualifies every symbol with the object that declares it. Global variables are `<GVL name>.<variable>`, where `<GVL name>` is the name you gave the Global Variable List object — TwinCAT names a new GVL `GVL`, so `GVL.nCounter` is the common case, while a list named `GVL_ProcessData` yields `GVL_ProcessData.nCounter`. Program variables use the POU name: `MAIN.MyVariable`, `PRG_Machine.nUpdates`. There is no fixed `GVL_` prefix and no leading dot.

**TwinCAT 2** puts globals in one flat namespace with no list name, reached by a leading dot: `.nCounter`, `.stProcessData.fValue`. Program variables still carry the POU name and no dot: `MAIN.someVar`. Symbol names are case-insensitive — the PLC accepts any casing, and the plugin always preserves the casing you configured (TC3 returns original casing; TC2 returns uppercase, which the plugin maps back to your configured casing).

## Symbol names in topics

`ads_symbol_name` becomes the last segment of the UMH topic, where a dot separates levels, so the
symbol name has to be rewritten to fit. Array indices keep one separator, a TwinCAT 2 global's
leading dot is dropped (it marks the namespace, it is not part of the name), and runs of underscores
collapse:

| PLC symbol | `ads_symbol_name` |
|---|---|
| `GVL_ProcessData.stMachineStatus.stMotor1.fSpeed` | `GVL_ProcessData_stMachineStatus_stMotor1_fSpeed` |
| `PRG_Diagnostics.astSensorHistory[0].fValue` | `PRG_Diagnostics_astSensorHistory_0_fValue` |
| `.afMeasurements[0]` (TC2 global) | `afMeasurements_0` |
| `.nMasterCycleCounter` (TC2 global) | `nMasterCycleCounter` |
| `_internalVar` | `_internalVar` (a leading underscore may be the PLC's own naming) |

The original is always available as `ads_symbol_name_original`, so a symbol can still be traced back
to the PLC. Two symbols can in principle collapse to the same tag name — `.a[0]` and a variable
actually named `a_0` — so if a program relies on that distinction, use
`ads_symbol_name_original` in the `tag_processor` instead.

## Metadata outputs

| Metadata Field | Description |
|---|---|
| `ads_symbol_name` | PLC symbol name as one topic segment — see [Symbol names in topics](#symbol-names-in-topics) |
| `ads_symbol_name_original` | The symbol exactly as the PLC names it, dots and brackets intact (e.g. `PRG_Diagnostics.astSensorHistory[0].fValue`) |
| `ads_datatype` | PLC data type string as reported by the symbol table (e.g. `DINT`, `E_MachineState`, `REAL`). Set after first successful symbol resolution — may be absent on the very first batch after connect. |
| `ads_base_type` | Resolved IEC 61131-3 primitive underlying the symbol (e.g. `DINT` for an INT-aliased enum). Only set when the type resolves to a known primitive. |
| `ads_data_size` | Byte length of the symbol as reported by the PLC (e.g. `4` for DINT, `82` for STRING). |
| `ads_tag_type` | Value shape of the payload: `number`, `bool`, or `string`. Set for every message so downstream processors can branch on payload type without inspecting `ads_datatype`/`ads_base_type`. |
| `timestamp_ms` | Unix milliseconds for when the value was captured. For `readType: notification`, this is the PLC's sample time for that update; for `readType: interval`, this is the time the plugin performed the read. |

## Output

Each symbol produces a single message with the payload being the value read from the PLC. Use `meta("ads_symbol_name")` in a following benthos bloblang processor to distinguish messages.
