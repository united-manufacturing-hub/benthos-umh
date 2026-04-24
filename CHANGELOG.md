# Changelog

## [Unreleased]

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
