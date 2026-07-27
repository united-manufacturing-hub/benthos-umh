# Changelog

## [v0.14.0]

### New

- Node-RED JavaScript processor can return an array of message objects to publish one output message per array element (ENG-5240)

### Fixes

- A Node-RED JavaScript processor function that throws now drops only the failing message (warn-logged, counted in messages_dropped{reason=js_throw}) and lets the rest of the batch continue, instead of the whole batch failing and retrying (ENG-5240)
- A tag processor condition that throws now drops only the failing message (warn-logged, counted in messages_dropped{reason=js_throw}) and lets the rest of the batch continue, instead of the whole batch failing and retrying (ENG-5240)
- Metadata values in Kafka headers are now serialized consistently across the Node-RED JavaScript and tag processors; non-scalar and nested-null values serialize as JSON so a nested null no longer appears as the literal string <nil>, and in the Node-RED JavaScript processor numeric and boolean metadata values that were previously silently dropped now appear as strings (ENG-5240)
- The Node-RED JavaScript processor messages_processed counter now counts successfully produced outputs; a throwing message is counted in messages_dropped{reason=js_throw} rather than processed, so the counter no longer counts every input attempt or inflates on retries (ENG-5240)
- UNS schema validation now reports a clear datatype mismatch (e.g. sent timeseries-number, tag registered as timeseries-string) instead of a confusing error that listed the tag as both valid and invalid (ENG-5347)
- Sparkplug B input derives spb_timestamp from each metric's own timestamp when present, falling back to the payload timestamp; multi-metric NDATA/DDATA no longer collapse to one timestamp (ENG-5341)
- The nodered_js and tag_processor plugins no longer publish the messages_errored counter; JS errors are counted in messages_dropped{reason=js_throw} instead (ENG-5240)
- The TimescaleDB historian output logs a one-time info notice confirming the first message stored for its data contract. When it is receiving data but has stored nothing for that contract, it logs a one-time notice that the messages all belong to other contracts and points at data_contract_name and the flow's topic subscription. Other-contract-only batches no longer warn once data is flowing, while genuine faults are still logged as warnings (ENG-5394)
- The tag processor no longer converts numeric-looking string values to numbers; a string read from the source stays a string in the UNS, and msg.meta.datatype = "number" remains available for explicit coercion (ENG-5422)

## [v0.13.2]

### New

- TimescaleDB Historian output that saves a UNS data contract into TimescaleDB under a dedicated umh schema. By default every metadata key is stored; `metadata_keys_exclude` drops selected keys by exact name or `prefix_*` while keeping the rest (ENG-5181)

## [v0.13.1]

### New

- Node-RED JS and tag processor expose a `protobuf` namespace (`protobuf.decode` / `protobuf.encode`) to decode and encode protobuf messages inline using an embedded base64 descriptor set, including proto2 extension fields (ENG-5243)
- Sparkplug B input decodes proto2 extension fields from an inline schema, exposing them per metric as `spb_ext_*` and `spb_metric_decoded` metadata (ENG-5229)

### Fixes

- Sparkplug B input can nest device data under its edge node via include_edge_node_in_location, so identically-named devices on different edge nodes no longer collide at the top of the hierarchy (ENG-5175)

## [v0.13.0]

### Improvements

- OPC UA: encrypted connections (`Basic256Sha256`, both `Sign` and `Sign & Encrypt`) now complete reliably, including signing-only mode which previously failed to connect

### Fixes

- OPC UA: reading or subscribing to large tag sets (roughly a thousand or more) over an encrypted connection no longer drops the connection with an `EOF` error
- OPC UA: subscriptions now survive a reconnect, so data collection resumes automatically after a network interruption instead of stalling until the input is restarted
- OPC UA: connecting to servers that present their certificate as a chain (leaf plus intermediates), such as Siemens WinCC Unified, now works
- OPC UA: a single unreadable or invalid node ID no longer blocks monitoring the rest of a subscribed batch
- OPC UA: browsing certain servers no longer crashes the input

## [v0.12.7]

### Fixes

- Sparkplug B output: a standard `tag_processor → sparkplug_b` flow now publishes its tags. The output read the value from a payload field named after the tag, but `tag_processor` emits it under `value`, so every message was silently dropped — leaving only an empty `NBIRTH` on the broker. The output now extracts the value the same way the Sparkplug B input does (`value`/`val`/`data`/`measurement`), makes `virtual_path` optional, and warns instead of dropping silently when a payload can't be turned into a metric (ENG-5087)
- OPC-UA input now preserves string values exactly; previously, numeric-looking strings like serial codes were emitted as numbers and lost precision. For tags processed by `tag_processor`, also set `msg.meta.datatype = "string"` so auto-detection does not convert them back (ENG-5011)
- Sparkplug B input: metric datatypes now survive from BIRTH to DATA. Per spec, `NDATA`/`DDATA` carries only alias + value while the BIRTH certificate defines name and datatype — but the input cached only the name, so DATA messages lost their `spb_datatype` metadata and signed integers decoded as their unsigned two's-complement wire value (an `Int32` of `-12` surfaced as `4294967284`). The alias cache now restores the datatype alongside the name, and `Int8`/`Int16`/`Int32`/`Int64` wire values are reinterpreted as signed (ENG-5126)

## [v0.12.6]

### Improvements

- Sparkplug B input: `request_birth_on_connect` now defaults to `true`, so `secondary_active`/`primary` bridges proactively rebirth newly seen nodes on connect. Set it to `false` to keep the prior behavior (ignored under `secondary_passive`) (ENG-5002)
- Sparkplug B input: field descriptions and the primary-role startup log now clarify that `identity.edge_node_id` is the Sparkplug v3.0-compatible `host_id` in the STATE topic (`spBv1.0/STATE/<host_id>`) (ENG-4974)
- New `snowflake_put` output: ports the [warpstreamlabs/bento Snowflake output](https://warpstreamlabs.github.io/bento/docs/components/outputs/snowflake_put/) into benthos-umh for writing batched messages to Snowflake stages with optional Snowpipe ingestion. Supports user/password and key-pair auth, all gosnowflake compression modes, and per-message stage/Snowpipe interpolation (ENG-5061)

### Fixes

- Sparkplug B input: `identity.group_id` now filters the MQTT subscription by default. Previously an empty `subscription.groups` subscribed to every Sparkplug group on the broker (`spBv1.0/+/#`) regardless of `identity.group_id`. To restore the old behavior, set `subscription.groups: ["+"]` explicitly (ENG-4974)
- Sparkplug B input: bridges now request a rebirth when DATA references aliases the cache hasn't seen (typically after a bridge restart with no retained `NBIRTH`/`DBIRTH` on the broker). Previously tags surfaced as `…/_historian/alias_<n>` until something external triggered recovery (ENG-5002)

## [v0.12.5]

### Fixes

- OPC-UA input could get stuck while browsing when a configured NodeID did not exist on the server, requiring a manual restart. Browse failures now trigger a clean reconnect
- OPC-UA input no longer spams `Variant is nil` errors when a node sends a status update without a value. These are harmless and now logged at debug level with the NodeID and status code
- Modbus TCP input now reconnects immediately on any transport-level error (timeouts, resets, network failures), not just broken pipes. Previously these stuck the connection for up to 10 seconds
- Modbus TCP input now recovers automatically from transaction-ID mismatches. Previously, when a slow PLC reply arrived after its read timeout, the next poll picked up the stale frame and failed with `modbus: response transaction id 'X' does not match request 'Y'`. The connection thrashed (reconnect, mismatch, reconnect) and reads stalled until conditions cleared or the input was restarted

## [v0.12.4]

### Improvements

- Tag processor now supports `msg.meta.datatype` to override value type auto-detection. Set to `"string"`, `"number"`, or `"bool"` to force the output type

## [v0.12.3]

### Improvements

- Cache API for JavaScript processors: new `cache.set(key, value)`, `cache.get(key)`, `cache.exists(key)`, and `cache.delete(key)` methods for tracking state across messages. Previously, state management required complex Benthos `branch`/`request_map`/`result_map` configurations. Now you can store any JSON-compatible value (strings, numbers, objects, arrays) directly from JavaScript. Use `cache.exists(key)` before `cache.get(key)` to handle missing keys. Available in both `nodered_js` and `tag_processor`. Currently in-memory only (lost on restart), persistent backend planned

## [v0.12.2]

### Improvements

- Updated Go dependencies, includes security fixes for OIDC and JOSE authentication libraries

## [v0.12.1]

### Improvements

- Unified Address Field for Modbus: introduces `unifiedAddresses` as a single-string alternative to the existing address object list. Format: `name.register.address.type[:key=value]*` (e.g., `temperature.holding.100.INT16:scale=0.1`). The legacy `addresses` object list continues to work with a deprecation warning. Both fields are mutually exclusive

### Fixes

- Map fields in the JSON schema incorrectly produced `"type": "string"` instead of `"type": "object"` with `additionalProperties`. Component reference types (`input`, `output`, `processor`, `scanner`) and unknown types had the same issue -- all now map to the correct schema types
- ADS symbol downloads failed in certain configurations -- bumped ADS plugin to v1.0.8 which fixes the issue

## [v0.12.0]

### Improvements

- S7 addresses for PE, PA, MK, C, and T areas no longer require a block number. You can now write `PE.X0.0` instead of `PE0.X0.0`. The old format still works but logs a deprecation warning and will be removed in a future version. Data Block addresses (`DB1.DW20`) are unchanged

### Fixes

- The S7 `DateAndTime` data type crashed due to an incorrect buffer size and now reads correctly
- Fields with children that already have default values were incorrectly marked as required when editing bridge configurations -- they are now correctly treated as optional
- Fields marked as deprecated in bridge plugin definitions were not flagged in the Management Console editor -- they now correctly appear as deprecated
