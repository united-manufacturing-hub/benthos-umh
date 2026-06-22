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

package historian_plugin

import (
	"encoding/json"
	"sort"
	"strings"
)

// Structural keys are already stored as columns/dimensions or are transport routing.
var skipStructural = toSet(
	"location_path", "data_contract", "virtual_path", "tag_name",
	"data_contract_name", "data_contract_version",
	"data_contract_bypassed", "data_contract_bypass_reason",
	"umh_topic", "bridged_by", "origin", "origin_id",
	"kafka_topic", "kafka_key", "kafka_msg_key", "kafka_partition", "kafka_offset",
	"kafka_timestamp_unix", "kafka_lag", "kafka_tombstone_message",
)

// High-churn keys change on nearly every message and defeat metadata dedup.
var highChurn = toSet(
	"kafka_timestamp_ms",
	"opcua_source_timestamp", "opcua_server_timestamp", "opcua_attr_statuscode",
	"opcua_heartbeat_message",
	"spb_sequence", "spb_bdseq", "spb_timestamp", "spb_metric_index",
	"spb_metrics_in_payload", "spb_message_type", "spb_state",
	"event_type", "umh_conversion_status", "umh_conversion_error",
)

func toSet(keys ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

// SelectMetaKeys picks which metadata keys to store: in all-keys mode it drops
// "_"-prefixed, structural, and high-churn keys; in allowlist mode it takes the list verbatim.
func SelectMetaKeys(meta map[string]string, allKeys bool, allowlist []string) []string {
	if !allKeys {
		return append([]string(nil), allowlist...)
	}
	keys := make([]string, 0, len(meta))
	for k := range meta {
		if strings.HasPrefix(k, "_") {
			continue
		}
		if _, ok := skipStructural[k]; ok {
			continue
		}
		if _, ok := highChurn[k]; ok {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

// BuildMetadata keeps only the keys that are present in meta.
func BuildMetadata(meta map[string]string, keys []string) map[string]string {
	md := make(map[string]string)
	for _, k := range keys {
		if v, ok := meta[k]; ok {
			md[k] = v
		}
	}
	return md
}

// Fingerprint serializes metadata as a JSONB object ({"key":"value"}) — both the stored
// attribute value and the dedup key. The object shape (not an array) is what makes
// attribute->>'key' / @> queries resolve. json.Marshal sorts keys, so it is deterministic.
func Fingerprint(md map[string]string) string {
	b, _ := json.Marshal(md)
	return string(b)
}

// HighChurnKeys returns the stored keys that are known high-churn (reachable only in
// allowlist mode, which bypasses the blacklist).
func HighChurnKeys(md map[string]string) []string {
	var out []string
	for k := range md {
		if _, ok := highChurn[k]; ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
