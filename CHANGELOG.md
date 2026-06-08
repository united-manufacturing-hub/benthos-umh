# Changelog

## [Unreleased]

### Fixes

- Sparkplug B output: a standard `tag_processor → sparkplug_b` flow now publishes its tags. The output read the value from a payload field named after the tag, but `tag_processor` emits it under `value`, so every message was silently dropped — leaving only an empty `NBIRTH` on the broker. The output now extracts the value the same way the Sparkplug B input does (`value`/`val`/`data`/`measurement`), makes `virtual_path` optional, and warns instead of dropping silently when a payload can't be turned into a metric (ENG-5087)
- Sparkplug B input: metric datatypes now survive from BIRTH to DATA. Per spec, `NDATA`/`DDATA` carries only alias + value while the BIRTH certificate defines name and datatype — but the input cached only the name, so DATA messages lost their `spb_datatype` metadata and signed integers decoded as their unsigned two's-complement wire value (an `Int32` of `-12` surfaced as `4294967284`). The alias cache now restores the datatype alongside the name, and `Int8`/`Int16`/`Int32`/`Int64` wire values are reinterpreted as signed (ENG-5126)
- OPC-UA input now preserves string values exactly. Previously, a string tag whose value looked like a large number, such as a long marking or serial code like `83275631010238526`, was emitted as a number and rounded to something like `8.33e+16` (ENG-5011)

## [0.12.6]

### Fixes

- Sparkplug B input: `identity.group_id` now filters the MQTT subscription by default. Previously an empty `subscription.groups` subscribed to every Sparkplug group on the broker (`spBv1.0/+/#`) regardless of `identity.group_id`. To restore the old behavior, set `subscription.groups: ["+"]` explicitly (ENG-4974)
- Sparkplug B input: bridges now request a rebirth when DATA references aliases the cache hasn't seen (typically after a bridge restart with no retained `NBIRTH`/`DBIRTH` on the broker). Previously tags surfaced as `…/_historian/alias_<n>` until something external triggered recovery (ENG-5002)

### Improvements

- Sparkplug B input: `request_birth_on_connect` now defaults to `true`, so `secondary_active`/`primary` bridges proactively rebirth newly seen nodes on connect. Set it to `false` to keep the prior behavior (ignored under `secondary_passive`) (ENG-5002)
- Sparkplug B input: field descriptions and the primary-role startup log now clarify that `identity.edge_node_id` is the Sparkplug v3.0-compatible `host_id` in the STATE topic (`spBv1.0/STATE/<host_id>`) (ENG-4974)
- New `snowflake_put` output: ports the [warpstreamlabs/bento Snowflake output](https://warpstreamlabs.github.io/bento/docs/components/outputs/snowflake_put/) into benthos-umh for writing batched messages to Snowflake stages with optional Snowpipe ingestion. Supports user/password and key-pair auth, all gosnowflake compression modes, and per-message stage/Snowpipe interpolation (ENG-5061)

## [0.12.5]

### Fixes

- OPC-UA input could get stuck while browsing when a configured NodeID did not exist on the server, requiring a manual restart. Browse failures now trigger a clean reconnect
- OPC-UA input no longer spams `Variant is nil` errors when a node sends a status update without a value. These are harmless and now logged at debug level with the NodeID and status code
- Modbus TCP input now reconnects immediately on any transport-level error (timeouts, resets, network failures), not just broken pipes. Previously these stuck the connection for up to 10 seconds
- Modbus TCP input now recovers automatically from transaction-ID mismatches. Previously, when a slow PLC reply arrived after its read timeout, the next poll picked up the stale frame and failed with `modbus: response transaction id 'X' does not match request 'Y'`. The connection thrashed (reconnect, mismatch, reconnect) and reads stalled until conditions cleared or the input was restarted

## [0.12.4]

### Improvements

- Tag processor now supports `msg.meta.datatype` to override value type auto-detection. Set to `"string"`, `"number"`, or `"bool"` to force the output type

## [0.12.3]

### Improvements

- Cache API for JavaScript processors: new `cache.set(key, value)`, `cache.get(key)`, `cache.exists(key)`, and `cache.delete(key)` methods for tracking state across messages. Previously, state management required complex Benthos `branch`/`request_map`/`result_map` configurations. Now you can store any JSON-compatible value (strings, numbers, objects, arrays) directly from JavaScript. Use `cache.exists(key)` before `cache.get(key)` to handle missing keys. Available in both `nodered_js` and `tag_processor`. Currently in-memory only (lost on restart), persistent backend planned

## [0.12.2]

### Improvements

- Updated Go dependencies, includes security fixes for OIDC and JOSE authentication libraries

## [0.12.1]

### Improvements

- Unified Address Field for Modbus: introduces `unifiedAddresses` as a single-string alternative to the existing address object list. Format: `name.register.address.type[:key=value]*` (e.g., `temperature.holding.100.INT16:scale=0.1`). The legacy `addresses` object list continues to work with a deprecation warning. Both fields are mutually exclusive

### Fixes

- Map fields in the JSON schema incorrectly produced `"type": "string"` instead of `"type": "object"` with `additionalProperties`. Component reference types (`input`, `output`, `processor`, `scanner`) and unknown types had the same issue -- all now map to the correct schema types
- ADS symbol downloads failed in certain configurations -- bumped ADS plugin to v1.0.8 which fixes the issue

## [0.12.0]

### Improvements

- S7 addresses for PE, PA, MK, C, and T areas no longer require a block number. You can now write `PE.X0.0` instead of `PE0.X0.0`. The old format still works but logs a deprecation warning and will be removed in a future version. Data Block addresses (`DB1.DW20`) are unchanged

### Fixes

- The S7 `DateAndTime` data type crashed due to an incorrect buffer size and now reads correctly
- Fields with children that already have default values were incorrectly marked as required when editing bridge configurations -- they are now correctly treated as optional
- Fields marked as deprecated in bridge plugin definitions were not flagged in the Management Console editor -- they now correctly appear as deprecated
