// Copyright 2026 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opcua_plugin

import (
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// These are the deprecation-path tests (OUT-1): the deprecated read-back
// fields and the programmable handshake live under the same `handshake` key,
// a config that sets both is refused, and a config that sets only the old
// fields keeps working exactly as before the block existed.

// parseOutputForTest runs one output config through the same parse the
// stream builder performs, and returns the constructed output.
func parseOutputForTest(t *testing.T, yamlConfig string) (*OPCUAOutput, error) {
	t.Helper()
	conf, err := opcuaOutputConfig().ParseYAML(yamlConfig, nil)
	if err != nil {
		return nil, err
	}
	return newOPCUAOutput(conf, service.MockResources())
}

// oldHandshakeYAML is a config that sets only the deprecated fields.
const oldHandshakeYAML = `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  enabled: true
  readbackTimeoutMs: 3000
  maxWriteAttempts: 3
  timeBetweenRetriesMs: 500
`

// A config with only the old fields parses into today's fields, unchanged.
func TestHandshakeConfig_OldFieldsOnlyKeepsTodaysBehavior(t *testing.T) {
	output, err := parseOutputForTest(t, oldHandshakeYAML)
	if err != nil {
		t.Fatalf("a config with only the deprecated fields must still parse: %v", err)
	}
	if !output.HandshakeEnabled {
		t.Error("HandshakeEnabled: got false, want true")
	}
	if output.ReadbackTimeoutMs != 3000 {
		t.Errorf("ReadbackTimeoutMs: got %d, want 3000", output.ReadbackTimeoutMs)
	}
	if output.MaxWriteAttempts != 3 {
		t.Errorf("MaxWriteAttempts: got %d, want 3", output.MaxWriteAttempts)
	}
	if output.TimeBetweenRetriesMs != 500 {
		t.Errorf("TimeBetweenRetriesMs: got %d, want 500", output.TimeBetweenRetriesMs)
	}
	if output.handshakeEngine != nil {
		t.Error("the deprecated fields must not build a programmable engine")
	}
}

// A partial old config gets today's defaults for the fields it omits.
func TestHandshakeConfig_PartialOldFieldsGetTodaysDefaults(t *testing.T) {
	output, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  enabled: false
`)
	if err != nil {
		t.Fatalf("partial old config must parse: %v", err)
	}
	if output.HandshakeEnabled {
		t.Error("HandshakeEnabled: got true, want false")
	}
	if output.ReadbackTimeoutMs != 2000 {
		t.Errorf("ReadbackTimeoutMs: got %d, want today's default 2000", output.ReadbackTimeoutMs)
	}
	if output.MaxWriteAttempts != 1 {
		t.Errorf("MaxWriteAttempts: got %d, want today's default 1", output.MaxWriteAttempts)
	}
	if output.TimeBetweenRetriesMs != 1000 {
		t.Errorf("TimeBetweenRetriesMs: got %d, want today's default 1000", output.TimeBetweenRetriesMs)
	}
}

// A config with no handshake block at all behaves as it does today.
func TestHandshakeConfig_NoHandshakeBlockGetsTodaysDefaults(t *testing.T) {
	for name, yamlConfig := range map[string]string{
		"no handshake block": `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
`,
		"empty handshake block": `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake: {}
`,
	} {
		t.Run(name, func(t *testing.T) {
			output, err := parseOutputForTest(t, yamlConfig)
			if err != nil {
				t.Fatalf("must parse as it does today: %v", err)
			}
			if !output.HandshakeEnabled {
				t.Error("HandshakeEnabled: got false, want true")
			}
			if output.ReadbackTimeoutMs != 2000 {
				t.Errorf("ReadbackTimeoutMs: got %d, want 2000", output.ReadbackTimeoutMs)
			}
			if output.MaxWriteAttempts != 1 {
				t.Errorf("MaxWriteAttempts: got %d, want 1", output.MaxWriteAttempts)
			}
			if output.TimeBetweenRetriesMs != 1000 {
				t.Errorf("TimeBetweenRetriesMs: got %d, want 1000", output.TimeBetweenRetriesMs)
			}
			if output.handshakeEngine != nil {
				t.Error("no engine may be built for the deprecated shape")
			}
		})
	}
}

// newHandshakeYAML is the ODU-shaped config: only the new fields, no
// nodeMappings, because the script performs the writes.
const newHandshakeYAML = `
endpoint: "opc.tcp://localhost:4840"
handshake:
  script: |
    const APPLY = { nodeId: "ns=3;s=Params.Apply", dataType: "Boolean" };
    if (opcua.read(APPLY) !== false) { return opcua.error_not_ready(); }
    opcua.write({ nodeId: "ns=3;s=Params.Value", dataType: "Int32" }, msg.payload.value);
    opcua.write(APPLY, true);
    return opcua.success({ value: msg.payload.value });
  onSuccess: |
    opcua.write({ nodeId: "ns=3;s=Params.Apply", dataType: "Boolean" }, false);
  onError: |
    opcua.write({ nodeId: "ns=3;s=Params.Apply", dataType: "Boolean" }, false);
  maxScriptRuntimeMs: 120000
  maxOnSuccessRuntimeMs: 15000
  maxOnErrorRuntimeMs: 15000
`

// A config with only the new fields builds the engine and needs no
// nodeMappings.
func TestHandshakeConfig_NewFieldsOnlyBuildsTheEngine(t *testing.T) {
	output, err := parseOutputForTest(t, newHandshakeYAML)
	if err != nil {
		t.Fatalf("a config with only the new fields must parse: %v", err)
	}
	if output.handshakeEngine == nil {
		t.Fatal("the new fields must build a handshake engine")
	}
	defer output.handshakeEngine.Close()
}

// The three deadlines are optional on the new path, with the documented
// defaults standing in for the ones the config omits.
func TestHandshakeConfig_NewFieldsWithoutDeadlinesBuild(t *testing.T) {
	output, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  script: |
    return opcua.success({});
`)
	if err != nil {
		t.Fatalf("deadlines the config omits must default: %v", err)
	}
	if output.handshakeEngine == nil {
		t.Fatal("the new fields must build a handshake engine")
	}
	defer output.handshakeEngine.Close()
}

// A config that sets an old field and a new field is refused, and the error
// names the two that conflict (OUT-1).
func TestHandshakeConfig_OldAndNewFieldsConflict(t *testing.T) {
	_, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  enabled: true
  readbackTimeoutMs: 2000
  script: |
    return opcua.success({});
`)
	if err == nil {
		t.Fatal("a config that sets both shapes must be refused")
	}
	for _, want := range []string{"enabled", "script"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error must name the conflicting field %q: %v", want, err)
		}
	}
}

// Every old field conflicts with every new one, and each pair is named.
func TestHandshakeConfig_EveryOldFieldConflictsWithEveryNewOne(t *testing.T) {
	// One type-valid value per field, so the pair is refused for the
	// conflict, not for its shape.
	values := map[string]string{
		"enabled":               "true",
		"readbackTimeoutMs":     "2000",
		"maxWriteAttempts":      "1",
		"timeBetweenRetriesMs":  "1000",
		"script":                "\"return opcua.success({});\"",
		"onSuccess":             "''",
		"onError":               "''",
		"maxScriptRuntimeMs":    "1000",
		"maxOnSuccessRuntimeMs": "1000",
		"maxOnErrorRuntimeMs":   "1000",
	}
	oldFields := []string{"enabled", "readbackTimeoutMs", "maxWriteAttempts", "timeBetweenRetriesMs"}
	newFields := []string{"script", "onSuccess", "onError", "maxScriptRuntimeMs", "maxOnSuccessRuntimeMs", "maxOnErrorRuntimeMs"}
	for _, old := range oldFields {
		for _, field := range newFields {
			yamlConfig := `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  ` + old + `: ` + values[old] + `
  ` + field + `: ` + values[field] + `
`
			_, err := parseOutputForTest(t, yamlConfig)
			if err == nil {
				t.Errorf("handshake.%s with handshake.%s must be refused", old, field)
				continue
			}
			if !strings.Contains(err.Error(), old) || !strings.Contains(err.Error(), field) {
				t.Errorf("the error must name both %q and %q: %v", old, field, err)
			}
		}
	}
}

// The new path without a script is refused: the script is the handshake.
func TestHandshakeConfig_NewWithoutScriptIsRefused(t *testing.T) {
	_, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  onSuccess: ""
  onError: ""
  maxScriptRuntimeMs: 1000
`)
	if err == nil {
		t.Fatal("the new path without a script must be refused")
	}
	if !strings.Contains(err.Error(), "script") {
		t.Errorf("the error must name the script: %v", err)
	}
}

// A script that does not compile is refused at config time, not against a
// machine (LIB-14).
func TestHandshakeConfig_NewScriptMustCompile(t *testing.T) {
	_, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  script: |
    this is not ( javascript
`)
	if err == nil {
		t.Fatal("a script that does not compile must be refused at config time")
	}
}

// Literal waits that exceed the script's deadline are refused (LIB-9).
func TestHandshakeConfig_NewWaitsMustFitTheDeadline(t *testing.T) {
	_, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  maxScriptRuntimeMs: 1000
  script: |
    opcua.wait_until({ nodeId: "ns=2;s=Loaded" }, v => v === true, 30000);
    return opcua.success({});
`)
	if err == nil {
		t.Fatal("waits that exceed the deadline must be refused at config time")
	}
}

// The node mappings are the deprecated fixed write; the script replaces them,
// so the two cannot be combined.
func TestHandshakeConfig_NewPathRefusesNodeMappings(t *testing.T) {
	_, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  script: |
    return opcua.success({});
`)
	if err == nil {
		t.Fatal("nodeMappings with a handshake script must be refused")
	}
	for _, want := range []string{"nodeMappings", "script"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error must name %q: %v", want, err)
		}
	}
}

// The old path still requires the node mappings, as it always did.
func TestHandshakeConfig_OldPathStillRequiresNodeMappings(t *testing.T) {
	_, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  enabled: true
`)
	if err == nil {
		t.Fatal("the old path without nodeMappings must be refused, as today")
	}
	if !strings.Contains(err.Error(), "node mapping") {
		t.Errorf("the error must still speak of node mappings: %v", err)
	}
}

// The three deadlines are positive whenever they reach the engine. Omitted,
// they fall back to the documented defaults; set to zero or below, the
// config is refused, because the engine reads a zero cleanup budget as "no
// limit" and a cleanup that hangs would hang the output with nothing
// logged. This is the config surface's half of that engine defect's fix.
func TestHandshakeConfig_DeadlinesArePositiveWhenTheyReachTheEngine(t *testing.T) {
	conf, err := opcuaOutputConfig().ParseYAML(`
endpoint: "opc.tcp://localhost:4840"
handshake:
  script: |
    return opcua.success({});
`, nil)
	if err != nil {
		t.Fatalf("the config must parse: %v", err)
	}
	cfg, err := parseProgrammableHandshake(conf.Namespace("handshake"))
	if err != nil {
		t.Fatalf("a config that omits every deadline must parse: %v", err)
	}
	if cfg.MaxScriptRuntime != 120*time.Second {
		t.Errorf("MaxScriptRuntime: got %v, want the documented default 120s", cfg.MaxScriptRuntime)
	}
	if cfg.MaxOnSuccessRuntime != 15*time.Second {
		t.Errorf("MaxOnSuccessRuntime: got %v, want the documented default 15s", cfg.MaxOnSuccessRuntime)
	}
	if cfg.MaxOnErrorRuntime != 15*time.Second {
		t.Errorf("MaxOnErrorRuntime: got %v, want the documented default 15s", cfg.MaxOnErrorRuntime)
	}
	if cfg.PollInterval != 100*time.Millisecond {
		t.Errorf("PollInterval: got %v, want the documented 100ms", cfg.PollInterval)
	}

	// Set to zero or below: refused, naming the field. Zero is the one a
	// user reaches for when they want "no deadline", and the one the engine
	// would read as unbounded.
	for _, field := range []string{"maxScriptRuntimeMs", "maxOnSuccessRuntimeMs", "maxOnErrorRuntimeMs"} {
		for _, value := range []string{"0", "-1"} {
			yamlConfig := `
endpoint: "opc.tcp://localhost:4840"
handshake:
  script: |
    return opcua.success({});
  ` + field + `: ` + value + "\n"
			_, err := parseOutputForTest(t, yamlConfig)
			if err == nil {
				t.Errorf("handshake.%s: %s must be refused", field, value)
				continue
			}
			if !strings.Contains(err.Error(), field) {
				t.Errorf("the error must name %q: %v", field, err)
			}
			if !strings.Contains(err.Error(), "positive") {
				t.Errorf("the error must say the deadlines must be positive: %v", err)
			}
		}
	}
}

// The same four configs through the stream builder, which is the deploy-time
// path a user's config actually takes.
func TestHandshakeConfig_StreamBuilder(t *testing.T) {
	newStyle := strings.ReplaceAll(newHandshakeYAML, "endpoint:", "endpoint:")
	bothBlocks := `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  enabled: true
  script: |
    return opcua.success({});
`
	cases := []struct {
		name    string
		yaml    string
		wantErr bool
	}{
		{name: "only old fields builds", yaml: oldHandshakeYAML},
		{name: "only new fields builds", yaml: newStyle},
		{name: "both is refused", yaml: bothBlocks, wantErr: true},
		{
			name: "neither builds",
			yaml: `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := service.NewStreamBuilder()
			if err := builder.SetLoggerYAML(`level: off`); err != nil {
				t.Fatal(err)
			}
			err := builder.AddOutputYAML("opcua:\n" + indentForBuilder(tc.yaml))
			if tc.wantErr && err == nil {
				stream, buildErr := builder.Build()
				if buildErr == nil {
					_ = stream
					t.Fatal("expected the stream build to fail")
				}
				err = buildErr
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				stream, buildErr := builder.Build()
				if buildErr != nil {
					t.Fatalf("expected the config to build: %v", buildErr)
				}
				_ = stream
				return
			}
			if _, err := builder.Build(); err != nil {
				t.Fatalf("expected the config to build: %v", err)
			}
		})
	}
}

// indentForBuilder indents a whole config by two spaces so it fits under the
// `opcua:` key.
func indentForBuilder(yamlConfig string) string {
	lines := strings.Split(strings.TrimPrefix(yamlConfig, "\n"), "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) != "" {
			lines[i] = "  " + line
		}
	}
	return strings.Join(lines, "\n")
}

// The deprecated read-back path's numbers are refused when they cannot work.
//
// maxWriteAttempts is the one that matters. The write loop is
// `for attempt := 1; attempt <= MaxWriteAttempts`, so zero or less runs it
// no times: nothing reaches the machine, the error it would have set stays
// nil, and Write returns success for every message. The read-back is inside
// the same loop, so it does not run either. A single unchecked integer
// turned the output into a silent acknowledger.
func TestDeprecatedHandshake_RefusesNumbersThatCannotWork(t *testing.T) {
	for _, tc := range []struct {
		name, yaml, says string
	}{
		{
			name: "maxWriteAttempts of zero writes nothing and acknowledges everything",
			yaml: "maxWriteAttempts: 0",
			says: "at least 1",
		},
		{
			name: "a negative maxWriteAttempts does the same",
			yaml: "maxWriteAttempts: -3",
			says: "at least 1",
		},
		{
			name: "a negative readback timeout",
			yaml: "readbackTimeoutMs: -5000",
			says: "cannot be negative",
		},
		{
			name: "a negative delay between retries",
			yaml: "timeBetweenRetriesMs: -1000",
			says: "cannot be negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseOutputForTest(t, `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  enabled: true
  `+tc.yaml+`
`)
			if err == nil {
				t.Fatalf("a config with %q was accepted", tc.yaml)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the refusal should say %q, got %v", tc.says, err)
			}
		})
	}
}
