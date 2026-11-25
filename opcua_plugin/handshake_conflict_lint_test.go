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
	"errors"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// These are the lint tests (OUT-1): handshakeConflictLint and
// nodeMappingsConflictLint are what refuse a conflicting config where the
// user's tooling looks — `benthos lint`, the stream builder's AddOutputYAML —
// before anything is constructed. Every other pair of conflicting fields was
// already covered somewhere; these ten lines of Bloblang in a string constant
// had no test at all, so an edit that broke them silently degraded a
// deploy-time refusal into a start-time one with nothing going red.
//
// The tests are written against the stream builder rather than the rule
// constant directly because the rule is only real once it is wired and
// executed the way a user's config is: AddOutputYAML lints the YAML first,
// and only then parses and constructs. A refusal that surfaces from
// AddOutputYAML as a service.LintError is the lint rule firing; the
// constructor's refusal (newOPCUAOutput, a plain error at Build time) is the
// start-time failure the rules exist to preempt, and each refused case below
// asserts the error IS a LintError so the two can never be confused again.

// lintOutputYAML lints one output config exactly the way a user's tooling
// does, and returns the error. A nil error means the config lints clean; the
// quiet cases additionally build the output to prove the config is not merely
// broken somewhere else.
func lintOutputYAML(yamlConfig string) error {
	builder := service.NewStreamBuilder()
	return builder.AddOutputYAML("opcua:\n" + indentForBuilder(yamlConfig))
}

// lintedClean also builds the output, so "quiet" means no lint error and a
// constructible config, not just an error that happens to appear later.
func lintedClean(t *testing.T, yamlConfig string) {
	t.Helper()
	builder := service.NewStreamBuilder()
	if err := builder.SetLoggerYAML(`level: off`); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddOutputYAML("opcua:\n" + indentForBuilder(yamlConfig)); err != nil {
		t.Fatalf("the config must lint clean: %v", err)
	}
	stream, err := builder.Build()
	if err != nil {
		t.Fatalf("the config must build: %v", err)
	}
	_ = stream
}

// refusedByLint asserts the config is refused at LINT time: the error is a
// service.LintError (not the constructor's plain error, which only appears
// at Build time and is what a broken rule silently degrades into), and it
// names every field the assertion is about.
func refusedByLint(t *testing.T, yamlConfig string, wantFields ...string) {
	t.Helper()
	err := lintOutputYAML(yamlConfig)
	if err == nil {
		t.Fatal("the config must be refused at lint time, before anything is constructed")
	}
	var lintErr service.LintError
	if !errors.As(err, &lintErr) {
		t.Fatalf("the refusal must be a lint error (service.LintError), got %T: %v", err, err)
	}
	for _, field := range wantFields {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("the lint error must name %q: %v", field, err)
		}
	}
}

func TestHandshakeConflictLint(t *testing.T) {
	// handshakeConflictLint: a block that sets a deprecated field together
	// with a programmable one is refused, naming both fields.
	t.Run("enabled with script is refused at lint time, naming both fields", func(t *testing.T) {
		refusedByLint(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  enabled: true
  script: |
    return opcua.success({});
`, "deprecated field enabled", "programmable field script")
	})

	t.Run("readbackTimeoutMs with onSuccess is refused at lint time, naming both fields", func(t *testing.T) {
		refusedByLint(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  readbackTimeoutMs: 3000
  onSuccess: |
    return;
`, "deprecated field readbackTimeoutMs", "programmable field onSuccess")
	})

	// handshakeConflictLint: a programmable block without a script is
	// refused — the script is the handshake.
	t.Run("onSuccess without script is refused at lint time", func(t *testing.T) {
		refusedByLint(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  onSuccess: |
    return;
`, "no script")
	})

	t.Run("maxOnErrorRuntimeMs without script is refused at lint time", func(t *testing.T) {
		refusedByLint(t, `
endpoint: "opc.tcp://localhost:4840"
handshake:
  maxOnErrorRuntimeMs: 15000
`, "no script")
	})

	// The quiet shapes: script-only, deprecated-only and no block at all.
	t.Run("a script-only block is quiet", func(t *testing.T) {
		lintedClean(t, newHandshakeYAML)
	})

	t.Run("a deprecated-only block is quiet", func(t *testing.T) {
		lintedClean(t, oldHandshakeYAML)
	})

	t.Run("no handshake block at all is quiet", func(t *testing.T) {
		lintedClean(t, `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
`)
	})

	// nodeMappingsConflictLint: the third conflicting pair. nodeMappings is
	// the deprecated fixed write and lives OUTSIDE the handshake block, so
	// the block's own rule cannot see it; the constructor refuses it at
	// Build time, and before the rule existed that refusal was the only one
	// — the asymmetry these subtests close.
	t.Run("nodeMappings with script is refused at lint time, naming both", func(t *testing.T) {
		refusedByLint(t, `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  script: |
    return opcua.success({});
`, "nodeMappings", "handshake field script")
	})

	t.Run("nodeMappings with onError is refused at lint time, naming both", func(t *testing.T) {
		refusedByLint(t, `
endpoint: "opc.tcp://localhost:4840"
nodeMappings:
  - nodeId: "ns=2;s=Setpoint"
    valueFrom: "value"
    dataType: "Int32"
handshake:
  onError: |
    return;
`, "nodeMappings", "handshake field onError")
	})

	// nodeMappings with only the DEPRECATED handshake fields is the
	// deprecated shape and must stay quiet.
	t.Run("nodeMappings with a deprecated handshake block is quiet", func(t *testing.T) {
		lintedClean(t, oldHandshakeYAML)
	})

	// An EMPTY nodeMappings list builds with a script today (the
	// constructor refuses only a non-empty list), so the rule must not
	// fire on it either.
	t.Run("an empty nodeMappings list with a script is quiet", func(t *testing.T) {
		lintedClean(t, `
endpoint: "opc.tcp://localhost:4840"
nodeMappings: []
handshake:
  script: |
    return opcua.success({});
`)
	})
}
