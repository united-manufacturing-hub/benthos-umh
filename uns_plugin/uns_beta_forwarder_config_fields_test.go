//go:build connect_patched

// Copyright 2025 UMH Systems GmbH
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

package uns_plugin

import (
	"reflect"
	"slices"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	// The package is placed under connect/v4/ because only packages there can
	// import internal/impl/kafka. It is created by the build-time patch
	// patches/connect-redpanda-forward.patch and is only present after
	// `make patch-connect` has applied that patch. This test proves the shim
	// re-exports connect's redpanda input field set.
	"github.com/redpanda-data/connect/v4/public/components/kafka/redpandaforward"
)

// configFieldName extracts the declared name of a *service.ConfigField.
//
// The public service API does not expose a field-name getter, so we reach the
// underlying docs.FieldSpec through service.ConfigField.XUnwrapper (the seam
// the service package itself uses to interop with its docs layer) and read its
// exported Name field via reflection. This avoids importing the internal docs
// package from benthos-umh. Each reflection step is guarded so a future
// service API shape change fails the test with a readable message instead of
// an opaque panic, or empty strings that hide a shim regression behind a
// passing snapshot).
func configFieldName(t *testing.T, f *service.ConfigField) string {
	t.Helper()
	if f == nil {
		t.Fatal("nil ConfigField")
	}
	unwrapper := reflect.ValueOf(f.XUnwrapper())
	if !unwrapper.IsValid() {
		t.Fatal("XUnwrapper() returned a nil/invalid value; service.ConfigField API shape changed")
	}
	unwrapMethod := unwrapper.MethodByName("Unwrap")
	if !unwrapMethod.IsValid() {
		t.Fatal("XUnwrapper result has no Unwrap method; service.ConfigField API shape changed")
	}
	call := unwrapMethod.Call(nil)
	if len(call) == 0 {
		t.Fatal("Unwrap() returned no values; service.ConfigField API shape changed")
	}
	specVal := call[0] // docs.FieldSpec
	if !specVal.IsValid() {
		t.Fatal("Unwrap() returned an invalid docs.FieldSpec; service.ConfigField API shape changed")
	}
	// reflect.Indirect dereferences a pointer before FieldByName, so a future
	// benthos bump returning *docs.FieldSpec degrades to the readable t.Fatal
	// below instead of panicking.
	specVal = reflect.Indirect(specVal)
	if specVal.Kind() != reflect.Struct {
		t.Fatal("Unwrap() returned a non-struct docs.FieldSpec; service.ConfigField API shape changed")
	}
	nameVal := specVal.FieldByName("Name")
	if !nameVal.IsValid() {
		t.Fatal("docs.FieldSpec has no Name field; service.ConfigField API shape changed")
	}
	return nameVal.String()
}

// TestRedpandaForwarderConfigFields asserts that the
// forwarder shim package redpandaforward exports ConfigFields() returning the
// full redpanda input field set — every field from
// FranzConnectionOptionalFields(), FranzConsumerFields(),
// FranzReaderToggledConfigFields(), plus the three wrapper toggle fields
// NewAutoRetryNacksToggleField(), NewForceTimelyNacksField() and
// NewExtractTracingSpanMappingField().
//
// Rather than spot-checking a few representative names (which a stub returning
// exactly those names would satisfy), the test snapshots the complete sorted
// field-name set. A connect upstream change that adds, removes, renames or
// reorders a field — or a shim that drops a contributing group while keeping a
// representative name from each — flips this snapshot red, so silent schema
// drift is caught. The expected set is the snapshot of the pinned
// connect version's redpanda input fields (the key_pattern field is added by
// the connect-key-filter patch applied in the same patched lane).
func TestRedpandaForwarderConfigFields(t *testing.T) {
	RegisterTestingT(t)

	fields := redpandaforward.ConfigFields()

	// The full redpanda field set is large (connection + consumer + reader
	// toggled + three wrappers); a non-empty return is the floor.
	Expect(fields).NotTo(BeEmpty(), "ConfigFields() must re-export the redpanda input field set, got an empty slice")

	// Collect declared names preserving duplicates so a field accidentally
	// re-exported twice shows up as a length mismatch against the snapshot.
	got := make([]string, 0, len(fields))
	for _, f := range fields {
		got = append(got, configFieldName(t, f))
	}
	slices.Sort(got)

	// Complete sorted snapshot of the redpanda input field set as exposed by
	// the pinned connect version (after both connect patches are applied).
	want := []string{
		"auto_replay_nacks",
		"client_id",
		"commit_period",
		"conn_idle_timeout",
		"consumer_group",
		"extract_tracing_map",
		"fetch_max_bytes",
		"fetch_max_partition_bytes",
		"fetch_max_wait",
		"fetch_min_bytes",
		"heartbeat_interval",
		"instance_id",
		"key_pattern",
		"max_yield_batch_bytes",
		"metadata_max_age",
		"partition_buffer_bytes",
		"rack_id",
		"rebalance_timeout",
		"regexp_topics",
		"regexp_topics_exclude",
		"regexp_topics_include",
		"request_timeout_overhead",
		"sasl",
		"seed_brokers",
		"session_timeout",
		"start_from_oldest",
		"start_offset",
		"tcp",
		"timely_nacks_maximum_wait",
		"tls",
		"topic_lag_refresh_period",
		"topics",
		"transaction_isolation_level",
		"unordered_processing",
	}

	// A missing, extra, renamed or duplicated field flips this red: the shim
	// no longer mirrors connect's redpanda input field set exactly.
	Expect(got).To(Equal(want), "ConfigFields() field-name set drifted from the redpanda input snapshot")
}
