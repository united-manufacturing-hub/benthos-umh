---
paths:
  - "*_plugin/**/*.go"
  - "cmd/schema-export/**"
---

# Plugin config fields

The ManagementConsole renders a plugin's form from the schema that `cmd/schema-export` exports. The exporter marks a field `Required` when it is neither optional nor has a default, and `Advanced` when it is marked advanced (`cmd/schema-export/exporter.go`).

| Field | Modifiers | In the UI |
|---|---|---|
| Basic | no `.Default()`, no `.Advanced()` | required (asterisk) |
| Advanced | `.Default().Advanced()` | optional, collapsed |

`.Default()` alone already makes a field not required. `.Optional()` is rarely needed.

```go
Field(service.NewStringField("endpoint").
    Description("OPC UA endpoint URL"))

Field(service.NewIntField("sessionTimeout").
    Description("Session timeout in milliseconds").
    Default(10000).
    Advanced())
```

## Basic or Advanced

**Basic**: needed for the plugin to work (endpoint, device address, credentials).
**Advanced**: a value whose default works for nearly every user. Before adding one, check [Opinionated simplicity](https://engineering.umh.app/product/product-standards/opinionated-simplicity): if one value fits nearly everyone, hard-code it instead of adding a field.

## Examples

- Enum-like fields list every valid option in `.Examples()`, e.g. `Examples("", "None", "Sign", "SignAndEncrypt")`.
- Boolean fields list `true` and `false`.
- Numeric fields list only the default, unless another value is a meaningful threshold.
- String lists show one item and several items.
