# Changelog

## [Unreleased]

### Fixes

- Sparkplug B input: `identity.group_id` now filters the MQTT subscription by default. Previously an empty `subscription.groups` caused the plugin to subscribe to every Sparkplug group on the broker (`spBv1.0/+/#`) regardless of `identity.group_id`. To restore the old behavior, set `subscription.groups: ["+"]` explicitly.
- Sparkplug B input: field descriptions and the primary-role startup log now state that `identity.edge_node_id` is used as the Sparkplug v3.0-compatible `host_id` in the STATE topic (`spBv1.0/STATE/<host_id>`).
- Sparkplug B input: `secondary_active` and `primary` roles now send a rebirth request when DATA arrives referencing aliases the bridge hasn't seen (typically after a bridge restart with no retained `NBIRTH`/`DBIRTH` on the broker). Previously these roles stayed silent and tags surfaced as `…/_historian/alias_<n>` until something external triggered recovery.
- Sparkplug B input: the sequence-gap rebirth path is now rate-limited by `birth_request_throttle`, sharing the same per-node throttle window as the discovery and unresolved-aliases paths. Previously it issued one rebirth per detected gap, which could overwhelm the broker when a flaky edge produced repeated gaps. See docs for the `birth_request_throttle` setting (default `1s`).
- Sparkplug B input: suppressed rebirths now log at `info` when the throttle has been active longer than 100ms, so operators can find "why didn't we rebirth?" in logs at default verbosity. Same-dispatch co-fires (multiple reasons triggered by one DATA message) log at `debug` to avoid noise on bridge restart.

### Improvements

- Sparkplug B input: `request_birth_on_connect` now defaults to `true`, so newly seen nodes are rebirthed automatically once `secondary_active`/`primary` bridges connect. Set it to `false` to opt back into the previous behavior. The flag is ignored under `secondary_passive` (passive bridges never publish commands).
- Sparkplug B input: new `sequence_gap_rebirths` metric counter (parity with `discovery_rebirths` and `alias_rebirths`) ticks whenever a sequence-gap rebirth passes the throttle gate.

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
