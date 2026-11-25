package handshake

import (
	"strings"
	"testing"
	"time"
)

// validConfig returns a configuration that can run, for tests that adapt it.
func validConfig() Config {
	return Config{
		Script:              `return opcua.success({});`,
		OnSuccess:           ``,
		OnError:             ``,
		MaxScriptRuntime:    time.Minute,
		MaxOnSuccessRuntime: time.Second,
		MaxOnErrorRuntime:   time.Second,
		PollInterval:        100 * time.Millisecond,
	}
}

// LIB-14: Validate compiles all three blocks; a script that does not parse is
// refused at deploy time, not against a machine.
func TestConfig_ValidateRefusesAScriptThatDoesNotCompile(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"script":    func(c *Config) { c.Script = `return {` },
		"onSuccess": func(c *Config) { c.OnSuccess = `opcua.write(` },
		"onError":   func(c *Config) { c.OnError = `function(){` },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("Validate accepted a %s that does not compile", name)
			}
		})
	}

	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate refused a config that can run: %v", err)
	}
}

// LIB-9: a configuration whose literal waits do not clear the script deadline
// is refused. Literal waits only; a computed wait is beyond it (SCR-4).
func TestConfig_ValidateRefusesWaitsThatExceedTheScriptDeadline(t *testing.T) {
	tests := []struct {
		name    string
		script  string
		limit   time.Duration
		wantErr bool
	}{
		{
			name:    "one wait over the limit",
			script:  `opcua.wait_until(A, v => v === true, 5000); return opcua.success({});`,
			limit:   3 * time.Second,
			wantErr: true,
		},
		{
			name: "waits that add up over the limit",
			script: `opcua.wait_until(A, v => v === true, 4000);
				 opcua.wait_until(B, v => v === true, 7000);
				 return opcua.success({});`,
			limit:   10 * time.Second,
			wantErr: true,
		},
		{
			name:    "waits inside the limit",
			script:  `opcua.wait_until(A, v => v === true, 5000); return opcua.success({});`,
			limit:   6 * time.Second,
			wantErr: false,
		},
		{
			// A computed wait is a person's to check (SCR-4), not Validate's.
			name:    "computed wait is beyond it",
			script:  `opcua.wait_until(A, v => v === true, msg.payload.wait); return opcua.success({});`,
			limit:   time.Second,
			wantErr: false,
		},
		{
			// An address object with commas in it must not fool the scanner.
			name:    "address object with a comma",
			script:  `opcua.wait_until({nodeId: "ns=2;s=A", dataType: "Boolean"}, v => v === true, 100); return opcua.success({});`,
			limit:   time.Second,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Script = tt.script
			cfg.MaxScriptRuntime = tt.limit
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate accepted a script whose waits exceed its deadline")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate refused a script whose waits clear its deadline: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), "MaxScriptRuntime") {
				t.Fatalf("refusal does not name the deadline it violated: %v", err)
			}
		})
	}
}
