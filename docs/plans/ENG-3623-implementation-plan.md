# ENG-3623: Form-Based UI for Industrial Protocol Configuration

**Status**: Ready for Implementation
**Architecture Decision**: Runtime JSON Rendering
**Estimated Effort**: 8-12 hours (Phase 1)
**Target Protocols**: Siemens S7, Modbus, OPC UA
**Linear Issue**: https://linear.app/unitedmanufacturinghub/issue/ENG-3623

---

## Background & Context

### Problem Statement
MAHLE automation engineers struggle with YAML configuration for industrial protocol bridges. They need a form-based UI that:
- Allows configuration via clicks (not code)
- Generates readable YAML automatically
- Supports bidirectional conversion (UI ↔ YAML) without data loss
- Follows Progressive Power principle from UX_STANDARDS.md

### Investigation Summary
Complete investigation conducted to validate feasibility:
- **Schema Access**: CONFIRMED via `service.GlobalEnvironment()` public API
- **Complex Datatypes**: PROVEN - nested objects, arrays of objects, arrays of primitives all supported
- **No Blockers**: All requirements achievable

**Investigation Documents** (reference for implementation details):
- `/Users/jeremytheocharis/Documents/git/troubleshooting/INVESTIGATION_SUMMARY.txt` - Executive summary
- `/Users/jeremytheocharis/Documents/git/troubleshooting/SCHEMA_ACCESS_INVESTIGATION.md` - Complete 624-line report
- `/Users/jeremytheocharis/Documents/git/troubleshooting/SCHEMA_ACCESS_QUICK_REFERENCE.md` - Code examples

### Architecture Decision Rationale

**Three Options Evaluated**:
1. **Manual Forms** - 80 hours effort, high maintenance ❌
2. **TypeScript Codegen** - 4-step pipeline, 20 hours effort ❌
3. **Runtime JSON Rendering** - 2-step pipeline, 10 hours effort ✅

**Why Runtime JSON Rendering Won**:
- Meets Architecture Simplification threshold (2 steps vs 4)
- 50% effort reduction vs codegen, 90% vs manual
- Proven complex datatype support (see investigation docs)
- Single source of truth (benthos ConfigSpec)
- Simpler debugging (JSON visible in DevTools)

### Reference Implementation

**Existing Pattern**: OPC UA configuration in ManagementConsole
- File: `ManagementConsole/frontend/src/lib/components/network-devices/opcua-config.svelte.ts`
- Pattern: Zod `.passthrough()` + `additionalSettings` preserves unknown fields
- Bidirectional: `parseInputYaml()` → UI → `updateYaml()` → YAML

**Key Pattern**:
```typescript
// Parse YAML preserving unknown fields
const parsed = safeParseYaml(inputYaml, {
  schema: z.object({
    opcua: z.object({
      endpoint: z.string(),
      nodeIDs: z.array(z.string()),
    }).passthrough(),  // CRITICAL: Preserves unknown fields
  }),
});

// Extract known + unknown fields
const { endpoint, nodeIDs, ...additionalSettings } = parsed.opcua;

// Rebuild YAML with all fields
const yaml = {
  opcua: {
    endpoint,
    nodeIDs,
    ...Object.fromEntries(
      additionalSettings.map(({ key, value }) => [key, value])
    ),
  },
};
```

### Required Reading

**Before implementation, read these files**:
1. `benthos-umh/CLAUDE.md` - Project architecture, plugin system, testing approach
2. `ManagementConsole/UX_STANDARDS.md` - Progressive Power principle, UI requirements
3. Investigation docs listed above - Schema structure, complex datatype handling

**benthos-umh Key Concepts** (from CLAUDE.md):
- Plugins auto-register via `init()` functions
- ConfigSpec uses `service.NewConfigSpec()` API
- Nested objects: `service.NewObjectField()` with children
- Arrays of objects: `service.NewObjectListField()` with children
- Arrays of primitives: `service.NewIntListField()`, `service.NewStringListField()`

**Complex Datatype Examples** (proven in investigation):
```go
// Modbus plugin (modbus_plugin/modbus.go lines 166-196)

// Nested object
service.NewObjectField("workarounds",
    service.NewDurationField("pauseAfterConnect").Default("0s"),
    service.NewBoolField("oneRequestPerField").Default(false),
    // ... 5 nested fields total
).Description("Modbus workarounds")

// Array of objects
service.NewObjectListField("addresses",
    service.NewStringField("name").Description("Field name"),
    service.NewStringField("register").Default("holding"),
    service.NewIntField("address"),
    service.NewStringField("type"),
    // ... 8 fields per address object
).Description("List of Modbus addresses")

// Array of primitives
service.NewIntListField("slaveIDs").Default([]int{1})
```

**How these render in JSON schema**:
```json
{
  "workarounds": {
    "type": "object",
    "kind": "scalar",
    "children": [
      {"name": "pauseAfterConnect", "type": "duration", "kind": "scalar"}
    ]
  },
  "addresses": {
    "type": "object",
    "kind": "array",
    "children": [
      {"name": "name", "type": "string", "kind": "scalar"}
    ]
  },
  "slaveIDs": {
    "type": "int",
    "kind": "array",
    "children": []
  }
}
```

---

## Architecture Overview

**Chosen Approach**: Runtime JSON Rendering (Option 3)

**Pipeline** (2 steps):
```
benthos-umh                    ManagementConsole
    |                                |
    v                                v
ConfigSpec ──JSON──> benthos-schemas.json ──Runtime──> Dynamic Form Builder
    |                                                          |
    |                                                          v
    └──────────────────────────────────────────────> Bidirectional YAML ↔ UI
```

**Why This Approach**:
- ✅ 2-step transformation (meets Architecture Simplification threshold)
- ✅ 8-12 hour implementation (vs 80h manual, 20h codegen)
- ✅ Complex datatypes proven feasible (nested objects, arrays of objects)
- ✅ Single source of truth (benthos ConfigSpec)
- ✅ Progressive Power compliant (form → YAML conversion)

---

## Implementation Tasks

### Task 1: Schema Exporter Tool (benthos-umh)

**Estimated Effort**: 4-6 hours

**Files to Create**:
- `cmd/schema-export/main.go` - CLI entry point
- `cmd/schema-export/exporter.go` - Schema extraction logic
- `cmd/schema-export/types.go` - JSON schema types

**Implementation**:

```go
// cmd/schema-export/types.go
package main

type SchemaOutput struct {
    Metadata   Metadata                `json:"metadata"`
    Inputs     map[string]PluginSpec  `json:"inputs"`
    Processors map[string]PluginSpec `json:"processors"`
    Outputs    map[string]PluginSpec  `json:"outputs"`
}

type Metadata struct {
    BenthosVersion    string    `json:"benthos_version"`
    GeneratedAt       time.Time `json:"generated_at"`
    BenthosUMHVersion string    `json:"benthos_umh_version"`
}

type PluginSpec struct {
    Name        string                `json:"name"`
    Type        string                `json:"type"`
    Source      string                `json:"source"` // "benthos-umh" | "upstream"
    Summary     string                `json:"summary"`
    Description string                `json:"description,omitempty"`
    Config      map[string]FieldSpec `json:"config"`
}

type FieldSpec struct {
    Name        string        `json:"name"`
    Type        string        `json:"type"`
    Kind        string        `json:"kind"`
    Description string        `json:"description"`
    Required    bool          `json:"required"`
    Default     interface{}   `json:"default"`
    Examples    []interface{} `json:"examples,omitempty"`
    Options     []string      `json:"options,omitempty"`
    Advanced    bool          `json:"advanced,omitempty"`
    Children    []FieldSpec   `json:"children,omitempty"` // For nested objects/arrays
}
```

```go
// cmd/schema-export/exporter.go
package main

import (
    "encoding/json"
    "github.com/redpanda-data/benthos/v4/public/service"
    _ "github.com/united-manufacturing-hub/benthos-umh/cmd/benthos/bundle"
)

func generateSchemas() (*SchemaOutput, error) {
    env := service.GlobalEnvironment()

    output := &SchemaOutput{
        Metadata: Metadata{
            BenthosVersion:    getBenthosVersion(),
            GeneratedAt:       time.Now(),
            BenthosUMHVersion: getUMHVersion(),
        },
        Inputs:     make(map[string]PluginSpec),
        Processors: make(map[string]PluginSpec),
        Outputs:    make(map[string]PluginSpec),
    }

    // Walk all inputs
    env.WalkInputs(func(name string, config *service.ConfigView) {
        spec := extractPluginSpec(name, "input", config)
        output.Inputs[name] = spec
    })

    // Walk all processors
    env.WalkProcessors(func(name string, config *service.ConfigView) {
        spec := extractPluginSpec(name, "processor", config)
        output.Processors[name] = spec
    })

    // Walk all outputs
    env.WalkOutputs(func(name string, config *service.ConfigView) {
        spec := extractPluginSpec(name, "output", config)
        output.Outputs[name] = spec
    })

    return output, nil
}

func extractPluginSpec(name, pluginType string, config *service.ConfigView) PluginSpec {
    jsonData, _ := config.FormatJSON()

    var rawSpec map[string]interface{}
    json.Unmarshal(jsonData, &rawSpec)

    // Transform internal format to our schema format
    spec := PluginSpec{
        Name:        name,
        Type:        pluginType,
        Source:      detectSource(name), // "benthos-umh" or "upstream"
        Summary:     config.Summary(),
        Description: config.Description(),
        Config:      make(map[string]FieldSpec),
    }

    // Extract fields recursively
    configObj := rawSpec["config"].(map[string]interface{})
    spec.Config = extractFields(configObj)

    return spec
}

func extractFields(configObj map[string]interface{}) map[string]FieldSpec {
    fields := make(map[string]FieldSpec)
    children := configObj["children"].([]interface{})

    for _, child := range children {
        fieldMap := child.(map[string]interface{})
        field := FieldSpec{
            Name:        fieldMap["name"].(string),
            Type:        fieldMap["type"].(string),
            Kind:        fieldMap["kind"].(string),
            Description: fieldMap["description"].(string),
            Required:    !fieldMap["is_optional"].(bool),
            Advanced:    fieldMap["is_advanced"].(bool),
        }

        // Handle default value
        if defaultVal, ok := fieldMap["default"]; ok {
            field.Default = defaultVal
        }

        // Handle examples
        if examples, ok := fieldMap["examples"].([]interface{}); ok {
            field.Examples = examples
        }

        // Handle options (for enum-like fields)
        if options, ok := fieldMap["options"].([]interface{}); ok {
            field.Options = make([]string, len(options))
            for i, opt := range options {
                field.Options[i] = opt.(string)
            }
        }

        // Recursively handle children (nested objects/arrays)
        if childFields, ok := fieldMap["children"].([]interface{}); ok && len(childFields) > 0 {
            field.Children = extractFieldsArray(childFields)
        }

        fields[field.Name] = field
    }

    return fields
}

func detectSource(pluginName string) string {
    umhPlugins := []string{
        "opcua", "modbus", "s7comm", "sparkplug", "eip", "sensorconnect",
        "tag_processor", "stream_processor", "downsampler", "topic_browser",
        "classic_to_core", "nodered_js", "uns",
    }

    for _, umh := range umhPlugins {
        if pluginName == umh {
            return "benthos-umh"
        }
    }
    return "upstream"
}
```

```go
// cmd/schema-export/main.go
package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "os"
)

func main() {
    output := flag.String("output", "benthos-schemas.json", "Output file path")
    flag.Parse()

    schemas, err := generateSchemas()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error generating schemas: %v\n", err)
        os.Exit(1)
    }

    data, err := json.MarshalIndent(schemas, "", "  ")
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
        os.Exit(1)
    }

    if err := os.WriteFile(*output, data, 0644); err != nil {
        fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
        os.Exit(1)
    }

    fmt.Printf("✅ Generated schemas to %s\n", *output)
    fmt.Printf("   - %d inputs\n", len(schemas.Inputs))
    fmt.Printf("   - %d processors\n", len(schemas.Processors))
    fmt.Printf("   - %d outputs\n", len(schemas.Outputs))
}
```

**Testing**:
```bash
cd benthos-umh
go run ./cmd/schema-export -output test-schemas.json
cat test-schemas.json | jq '.inputs | keys'
cat test-schemas.json | jq '.inputs.modbus.config'
```

**Success Criteria**:
- [ ] All benthos-umh plugins present (opcua, modbus, s7comm, etc.)
- [ ] Upstream plugins present (mqtt, kafka, http_client, etc.)
- [ ] Complex types correctly represented (nested objects, arrays of objects)
- [ ] Metadata includes correct versions

---

### Task 2: CI/CD Workflow (benthos-umh → ManagementConsole)

**Estimated Effort**: 2-3 hours

**File to Create**: `.github/workflows/schema-gen.yml`

```yaml
name: Benthos Schema Auto-Generation

on:
  push:
    branches: [master]
    paths:
      - 'go.mod'                      # Benthos version bump
      - '**/read.go'                  # Input plugins
      - '**/write.go'                 # Output plugins
      - '**/*_plugin.go'              # Any plugin file
      - 'cmd/schema-export/**'        # Schema exporter tool
  workflow_dispatch:                  # Manual trigger

concurrency:
  group: schema-gen-${{ github.ref }}
  cancel-in-progress: true

jobs:
  generate-and-update:
    name: Generate schemas and create PR
    runs-on: ubuntu-latest
    steps:
      - name: Checkout benthos-umh
        uses: actions/checkout@v4
        with:
          path: benthos-umh

      - name: Setup Go
        uses: actions/setup-go@v5
        with:
          go-version-file: benthos-umh/go.mod

      - name: Build schema exporter
        working-directory: benthos-umh
        run: |
          go build -o schema-exporter ./cmd/schema-export

      - name: Generate schemas
        working-directory: benthos-umh
        run: |
          ./schema-exporter -output benthos-schemas.json
          echo "Generated schemas:"
          cat benthos-schemas.json | jq '.metadata'

      - name: Checkout ManagementConsole
        uses: actions/checkout@v4
        with:
          repository: united-manufacturing-hub/ManagementConsole
          path: ManagementConsole
          token: ${{ secrets.CROSS_REPO_TOKEN }}

      - name: Copy schemas to ManagementConsole
        run: |
          mkdir -p ManagementConsole/frontend/src/generated
          cp benthos-umh/benthos-schemas.json ManagementConsole/frontend/src/generated/

      - name: Check for changes
        id: check_changes
        working-directory: ManagementConsole
        run: |
          git add frontend/src/generated/benthos-schemas.json

          if git diff --staged --quiet; then
            echo "✅ No schema changes detected"
            echo "has_changes=false" >> $GITHUB_OUTPUT
          else
            echo "📝 Schema changes detected"
            git diff --staged --stat
            echo "has_changes=true" >> $GITHUB_OUTPUT
          fi

      - name: Create Pull Request
        if: steps.check_changes.outputs.has_changes == 'true'
        working-directory: ManagementConsole
        env:
          GH_TOKEN: ${{ secrets.CROSS_REPO_TOKEN }}
        run: |
          git config user.name "benthos-schema-bot"
          git config user.email "noreply@umh.app"

          BRANCH_NAME="auto/benthos-schemas"
          BENTHOS_VERSION=$(jq -r '.metadata.benthos_version' frontend/src/generated/benthos-schemas.json)

          git checkout -b $BRANCH_NAME || git checkout $BRANCH_NAME
          git add frontend/src/generated/benthos-schemas.json
          git commit -m "chore: update benthos schemas to v${BENTHOS_VERSION}"

          git push origin $BRANCH_NAME --force-with-lease

          gh pr create \
            --title "chore: update benthos schemas to v${BENTHOS_VERSION}" \
            --body "Auto-generated schema update from benthos-umh" \
            --base staging \
            --head $BRANCH_NAME \
            --label "auto-generated" \
            --label "dependencies" \
            || gh pr comment $BRANCH_NAME --body "Updated schemas"
```

**Setup**:
1. Generate GitHub PAT with `repo` and `workflow` scopes
2. Add as secret in benthos-umh: `CROSS_REPO_TOKEN`

**Testing**:
```bash
# Manual trigger
gh workflow run schema-gen.yml

# Watch progress
gh run watch
```

**Success Criteria**:
- [ ] Workflow triggers on go.mod change
- [ ] Workflow triggers on plugin file change
- [ ] No PR created if no changes
- [ ] PR created in ManagementConsole when schemas change
- [ ] PR has correct labels and description

---

### Task 3: Dynamic Form Builder (ManagementConsole)

**Estimated Effort**: 2-3 hours

**File to Create**: `frontend/src/lib/components/protocol-forms/DynamicProtocolForm.svelte`

```svelte
<script lang="ts">
  import { onMount } from 'svelte';
  import type { FieldSpec, PluginSpec } from '$lib/types/benthos-schemas';

  interface Props {
    pluginName: string;
    pluginType: 'input' | 'output' | 'processor';
    value: any;
    onUpdate?: (value: any) => void;
  }

  let { pluginName, pluginType, value = $bindable({}), onUpdate }: Props = $props();

  let schema = $state<PluginSpec | null>(null);
  let schemas = $state<any>(null);

  onMount(async () => {
    const response = await fetch('/schemas/benthos-schemas.json');
    schemas = await response.json();
    schema = schemas[`${pluginType}s`][pluginName];
  });

  function updateValue(fieldName: string, newValue: any) {
    value[fieldName] = newValue;
    onUpdate?.(value);
  }

  function renderField(field: FieldSpec, path: string) {
    const fullPath = path ? `${path}.${field.name}` : field.name;

    // Nested objects
    if (field.type === 'object' && field.kind === 'scalar') {
      return {
        type: 'nested-object',
        field,
        path: fullPath
      };
    }

    // Arrays of objects
    if (field.type === 'object' && field.kind === 'array') {
      return {
        type: 'object-array',
        field,
        path: fullPath
      };
    }

    // Arrays of primitives
    if (field.kind === 'array') {
      return {
        type: 'primitive-array',
        field,
        path: fullPath
      };
    }

    // Scalar inputs
    return {
      type: 'scalar',
      field,
      path: fullPath
    };
  }
</script>

{#if schema}
  <div class="protocol-form">
    <h3>{schema.name}</h3>
    <p class="description">{schema.summary}</p>

    <div class="fields">
      {#each Object.values(schema.config) as field}
        {@const rendered = renderField(field, '')}

        {#if rendered.type === 'nested-object'}
          <div class="nested-object">
            <h4>{field.name}</h4>
            <p>{field.description}</p>
            <div class="nested-fields">
              {#each field.children as childField}
                <svelte:self
                  field={childField}
                  bind:value={value[field.name][childField.name]}
                  path={rendered.path}
                />
              {/each}
            </div>
          </div>

        {:else if rendered.type === 'object-array'}
          <div class="object-array">
            <h4>{field.name}</h4>
            <p>{field.description}</p>

            {#each value[field.name] || [] as item, index}
              <div class="array-item">
                <div class="array-item-header">
                  <span>Item {index + 1}</span>
                  <button onclick={() => removeArrayItem(field.name, index)}>
                    Remove
                  </button>
                </div>

                {#each field.children as childField}
                  <svelte:self
                    field={childField}
                    bind:value={item[childField.name]}
                    path={`${rendered.path}[${index}]`}
                  />
                {/each}
              </div>
            {/each}

            <button onclick={() => addArrayItem(field.name, field.children)}>
              Add {field.name}
            </button>
          </div>

        {:else if rendered.type === 'primitive-array'}
          <div class="primitive-array">
            <label>
              <span>{field.name}</span>
              {#if field.description}
                <span class="help-text">{field.description}</span>
              {/if}
            </label>
            <input
              type="text"
              bind:value={primitiveArrayString[field.name]}
              onblur={() => syncPrimitiveArray(field.name, field.type)}
              placeholder={field.type === 'int' ? '1, 2, 3' : 'value1, value2'}
            />
          </div>

        {:else if rendered.type === 'scalar'}
          <div class="scalar-field">
            <label>
              <span>{field.name}{field.required ? ' *' : ''}</span>
              {#if field.description}
                <span class="help-text">{field.description}</span>
              {/if}
            </label>

            {#if field.options && field.options.length > 0}
              <select bind:value={value[field.name]}>
                <option value="">-- Select --</option>
                {#each field.options as option}
                  <option value={option}>{option}</option>
                {/each}
              </select>
            {:else if field.type === 'string'}
              <input type="text" bind:value={value[field.name]} />
            {:else if field.type === 'int'}
              <input type="number" step="1" bind:value={value[field.name]} />
            {:else if field.type === 'float'}
              <input type="number" step="0.01" bind:value={value[field.name]} />
            {:else if field.type === 'bool'}
              <input type="checkbox" bind:checked={value[field.name]} />
            {:else if field.type === 'duration'}
              <input type="text" bind:value={value[field.name]} placeholder="10s" />
            {/if}
          </div>
        {/if}
      {/each}
    </div>
  </div>
{:else}
  <p>Loading schema...</p>
{/if}

<style>
  .protocol-form {
    padding: 1rem;
  }

  .nested-object {
    border-left: 3px solid var(--primary-color);
    padding-left: 1rem;
    margin: 1rem 0;
  }

  .object-array {
    margin: 1rem 0;
  }

  .array-item {
    border: 1px solid var(--border-color);
    padding: 1rem;
    margin-bottom: 0.5rem;
    border-radius: 4px;
  }

  .array-item-header {
    display: flex;
    justify-content: space-between;
    margin-bottom: 0.5rem;
  }

  .help-text {
    color: var(--text-secondary);
    font-size: 0.875rem;
    display: block;
  }

  .scalar-field {
    margin-bottom: 1rem;
  }

  label {
    display: block;
    margin-bottom: 0.25rem;
    font-weight: 500;
  }

  input, select {
    width: 100%;
    padding: 0.5rem;
    border: 1px solid var(--border-color);
    border-radius: 4px;
  }

  button {
    padding: 0.5rem 1rem;
    background: var(--primary-color);
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
  }
</style>
```

**Integration with Existing Code**:

```svelte
<!-- frontend/src/lib/components/network-devices/BridgeConfigurator.svelte -->
<script lang="ts">
  import DynamicProtocolForm from '$lib/components/protocol-forms/DynamicProtocolForm.svelte';

  let selectedProtocol = $state('modbus');
  let protocolConfig = $state({});
</script>

<div class="bridge-config">
  <select bind:value={selectedProtocol}>
    <option value="modbus">Modbus</option>
    <option value="s7comm">Siemens S7</option>
    <option value="opcua">OPC UA</option>
  </select>

  <DynamicProtocolForm
    pluginName={selectedProtocol}
    pluginType="input"
    bind:value={protocolConfig}
    onUpdate={(config) => {
      // Generate YAML from config
      updateYaml(config);
    }}
  />
</div>
```

**Success Criteria**:
- [ ] Form renders all field types correctly
- [ ] Nested objects display with proper indentation
- [ ] Arrays of objects support add/remove
- [ ] Arrays of primitives handle comma-separated input
- [ ] Required fields marked with asterisk
- [ ] Enum fields show dropdown
- [ ] Value updates trigger YAML generation

---

## Testing Plan

### Unit Tests

**Schema Exporter**:
```bash
cd benthos-umh
go test ./cmd/schema-export/... -v
```

Test cases:
- [ ] Extracts all benthos-umh plugins
- [ ] Extracts upstream plugins
- [ ] Correctly identifies plugin source
- [ ] Handles nested objects
- [ ] Handles arrays of objects
- [ ] Handles arrays of primitives

### Integration Tests

**End-to-End Flow**:
1. Make plugin change in benthos-umh
2. Push to master
3. Verify workflow runs
4. Verify PR created in ManagementConsole
5. Verify schemas updated
6. Verify form renders correctly

**Test Protocols** (Phase 1):
- [ ] Modbus (nested objects: workarounds, arrays: addresses)
- [ ] Siemens S7 (arrays: addresses)
- [ ] OPC UA (arrays: nodeIDs)

### Manual Testing

**Form Validation**:
- [ ] Fill out Modbus form completely
- [ ] Verify YAML generates correctly
- [ ] Switch to YAML mode, verify all fields present
- [ ] Edit YAML, switch back to form, verify changes preserved
- [ ] Toggle advanced fields, verify they appear/disappear

---

## Rollout Plan

### Week 1: Implementation
**Days 1-2**: Schema exporter tool
- [ ] Implement types.go, exporter.go, main.go
- [ ] Test locally with benthos-umh plugins
- [ ] Verify JSON output structure

**Days 3-4**: CI/CD workflow
- [ ] Create GitHub Actions workflow
- [ ] Set up CROSS_REPO_TOKEN secret
- [ ] Test manual trigger
- [ ] Verify PR creation

**Day 5**: Dynamic form builder
- [ ] Implement DynamicProtocolForm.svelte
- [ ] Test with Modbus schema
- [ ] Verify all field types render

### Week 2: Testing & Refinement
**Days 1-2**: End-to-end testing
- [ ] Test complete flow with all 3 protocols
- [ ] Fix any rendering issues
- [ ] Optimize form layout

**Days 3-4**: Integration with existing UI
- [ ] Replace manual Modbus form with dynamic form
- [ ] Replace manual S7 form with dynamic form
- [ ] Replace manual OPC UA form with dynamic form

**Day 5**: Deploy to staging
- [ ] Merge to staging branch
- [ ] Test in staging environment
- [ ] Gather feedback

### Week 3: Production Deployment
**Days 1-2**: MAHLE testing
- [ ] Deploy to MAHLE test environment
- [ ] Collect user feedback
- [ ] Fix critical issues

**Days 3-5**: Production rollout
- [ ] Deploy to production
- [ ] Monitor for issues
- [ ] Document any limitations

---

## Success Metrics

**Week 1**:
- [ ] Schema exporter generates valid JSON for all plugins
- [ ] CI/CD workflow runs successfully
- [ ] Dynamic form renders Modbus config

**Week 2**:
- [ ] All 3 Phase 1 protocols working in form UI
- [ ] Bidirectional YAML ↔ UI conversion working
- [ ] No data loss when toggling modes

**Week 3**:
- [ ] MAHLE users can configure protocols via form
- [ ] Zero manual YAML editing required for basic configs
- [ ] Advanced users can still use YAML mode

---

## Known Limitations & Future Work

### Current Limitations

1. **No Breaking Change Detection**
   - Schema updates don't detect breaking changes
   - Relies on TypeScript compilation errors
   - Future: Add semantic versioning

2. **No Runtime Validation**
   - Form validation is basic (required/type checking)
   - No Benthos-level validation until deployment
   - Future: Add Zod schemas for runtime validation

3. **No Schema Versioning**
   - Single JSON file, no migration path
   - Future: Version schemas (benthos-schemas-v1.json)

### Future Enhancements

**Short-term (1-3 months)**:
- [ ] Add form validation with Zod
- [ ] Generate TypeScript types from JSON (optional compile-time safety)
- [ ] Add field-level help tooltips
- [ ] Support for conditional fields

**Medium-term (3-6 months)**:
- [ ] Auto-merge workflow in ManagementConsole
- [ ] Breaking change detection in CI
- [ ] Schema diff visualization in PR
- [ ] Support for all benthos plugins (not just Phase 1)

**Long-term (6-12 months)**:
- [ ] Submit PR to Benthos for public ConfigSpec API
- [ ] Generate OpenAPI spec from schemas
- [ ] Config validation in UI before save
- [ ] Template library for common configurations

---

## Risk Mitigation

### Risk 1: Benthos API Changes
**Mitigation**: Pin Benthos version, add compatibility checks

### Risk 2: Schema Exporter Bugs
**Mitigation**: Comprehensive unit tests, manual validation

### Risk 3: Form Rendering Issues
**Mitigation**: Test with real plugin configs, iterate based on feedback

### Risk 4: CI/CD Failures
**Mitigation**: Manual trigger option, rollback plan

---

## Rollback Plan

If issues arise:
1. Disable auto-generated forms in UI
2. Revert to manual form components
3. Fix schema exporter bugs
4. Re-enable when stable

Manual forms (OPC UA, Modbus) remain available as fallback.

---

## Quick Reference

### Key File Locations

**Investigation Documents**:
- `/Users/jeremytheocharis/Documents/git/troubleshooting/INVESTIGATION_SUMMARY.txt`
- `/Users/jeremytheocharis/Documents/git/troubleshooting/SCHEMA_ACCESS_INVESTIGATION.md`
- `/Users/jeremytheocharis/Documents/git/troubleshooting/SCHEMA_ACCESS_QUICK_REFERENCE.md`

**benthos-umh** (schema source):
- `benthos-umh/CLAUDE.md` - Project context
- `benthos-umh/modbus_plugin/modbus.go` lines 166-196 - Complex datatype example
- `cmd/schema-export/` - Tool to create (Task 1)
- `.github/workflows/schema-gen.yml` - Workflow to create (Task 2)

**ManagementConsole** (UI):
- `ManagementConsole/UX_STANDARDS.md` - UI requirements
- `frontend/src/lib/components/network-devices/opcua-config.svelte.ts` - Reference pattern
- `frontend/src/lib/components/protocol-forms/DynamicProtocolForm.svelte` - Component to create (Task 3)
- `frontend/src/generated/benthos-schemas.json` - Generated schema (CI/CD output)

### Quick Commands

**Test schema exporter locally**:
```bash
cd benthos-umh
go run ./cmd/schema-export -output test-schemas.json
cat test-schemas.json | jq '.inputs | keys'
cat test-schemas.json | jq '.inputs.modbus.config.workarounds'
```

**Trigger CI/CD manually**:
```bash
cd benthos-umh
gh workflow run schema-gen.yml
gh run watch
```

**Check for ManagementConsole PR**:
```bash
cd ManagementConsole
gh pr list --head auto/benthos-schemas
```

**Test form locally**:
```bash
cd ManagementConsole/frontend
npm run dev
# Navigate to bridge configuration page
```

### API Quick Reference

**Benthos Public API** (for schema exporter):
```go
import "github.com/redpanda-data/benthos/v4/public/service"

env := service.GlobalEnvironment()

// Enumerate all inputs
env.WalkInputs(func(name string, config *service.ConfigView) {
    jsonData, _ := config.FormatJSON()
    summary := config.Summary()
    description := config.Description()
})

// Same for processors and outputs
env.WalkProcessors(fn)
env.WalkOutputs(fn)
```

**JSON Schema Structure**:
```typescript
interface PluginSpec {
  name: string;
  type: "input" | "output" | "processor";
  source: "benthos-umh" | "upstream";
  config: Record<string, FieldSpec>;
}

interface FieldSpec {
  name: string;
  type: "string" | "int" | "float" | "bool" | "object" | "duration";
  kind: "scalar" | "array" | "map";
  description: string;
  required: boolean;
  default?: any;
  children?: FieldSpec[];  // For nested objects/arrays
}
```

### Common Issues & Solutions

**Issue**: `service.GlobalEnvironment()` returns incomplete plugin list
**Solution**: Ensure bundle import: `_ "github.com/united-manufacturing-hub/benthos-umh/cmd/benthos/bundle"`

**Issue**: Complex types not rendering
**Solution**: Check `field.kind === "array" && field.type === "object"` for arrays of objects

**Issue**: Unknown fields lost when switching UI → YAML
**Solution**: Implement `.passthrough()` pattern from OPC UA reference

**Issue**: CI/CD creates empty PRs
**Solution**: Add `git diff --staged --quiet` check before PR creation

---

## Summary

**Architecture**: Runtime JSON Rendering (2-step pipeline)
**Effort**: 8-12 hours Phase 1 implementation
**Protocols**: Modbus, Siemens S7, OPC UA
**Deliverables**:
1. Schema exporter tool (benthos-umh)
2. CI/CD workflow (auto-update schemas)
3. Dynamic form builder (ManagementConsole)

**Starting Point**:
1. Read investigation docs
2. Review benthos-umh/CLAUDE.md and ManagementConsole/UX_STANDARDS.md
3. Study OPC UA reference implementation
4. Begin with Task 1 (Schema Exporter)

**Next Steps**: Begin implementation with Task 1 (Schema Exporter)
