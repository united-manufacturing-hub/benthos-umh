# Changelog

All notable changes to benthos-umh will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **[BREAKING]** `uns_input`: Kafka headers now stored as strings in Benthos metadata by default instead of byte arrays. This fixes the issue where `msg.meta.location_path` appeared as ASCII byte array `[69,103,111,...]` in JavaScript processors instead of human-readable string `"enterprise.site..."`.

  **Migration:** If you have existing flows with workarounds like `String.fromCharCode(...msg.meta.location_path)`, you have two options:

  1. **Keep legacy behavior (no code changes needed):**
     ```yaml
     input:
       uns:
         metadata_format: "bytes"  # Preserves old byte array behavior
     ```

  2. **Migrate to new behavior (recommended):**
     - Remove `String.fromCharCode()` workarounds from your processors
     - Use metadata directly: `const location = msg.meta.location_path;`
     - Leave `metadata_format` unset or explicitly set to `"string"`

  **Affected metadata fields:** `location_path`, `data_contract`, `tag_name`, `virtual_path`, and any custom Kafka headers.

  **Default behavior:**
  - New configs: `metadata_format: "string"` (correct behavior)
  - To use legacy mode: explicitly set `metadata_format: "bytes"`

  Addresses: #ENG-3435
