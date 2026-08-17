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
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

const (
	maxTextRunes          = 8192
	maxJSDateMs           = 8.64e15 // JS Date valid range is +/- this
	serverDiagnosticsPath = "Root.Objects.Server"
)

var (
	reVersionSuffix = regexp.MustCompile(`_v\d+$`)
	reContract      = regexp.MustCompile(`^[a-z0-9_]+$`)
	reNonLtreeLabel = regexp.MustCompile(`[^A-Za-z0-9_-]`)
)

// ValueType is the umh.value_type SQL enum domain.
type ValueType string

const (
	ValueNumeric ValueType = "numeric"
	ValueText    ValueType = "text"
)

// CanonicalLtreePath mirrors the SQL umh.to_ltree_path() so the in-process dedup key
// shares the DB's topic identity. PostgreSQL 16+ ltree labels accept hyphens, so "line-1"
// and "line_1" are kept distinct; only characters outside [A-Za-z0-9_-] fold to "_".
func CanonicalLtreePath(loc string) string {
	segs := strings.Split(loc, ".")
	out := make([]string, 0, len(segs))
	for _, s := range segs {
		s = reNonLtreeLabel.ReplaceAllString(s, "_")
		if r := []rune(s); len(r) > 255 {
			s = string(r[:255])
		}
		if s != "" {
			out = append(out, s)
		}
	}
	return strings.Join(out, ".")
}

// NormalizeContract strips a trailing _vN (all versions share one table).
func NormalizeContract(metaContract string) string {
	return reVersionSuffix.ReplaceAllString(metaContract, "")
}

// ValidateContract checks that data_contract_name is a bare lowercase name (letters, digits,
// underscores) with no leading underscore and no _vN version suffix.
func ValidateContract(c string) error {
	if !reContract.MatchString(c) {
		return fmt.Errorf("data_contract_name %q invalid: use a bare lowercase name (letters, digits, underscores), e.g. \"pump\"", c)
	}
	if c[0] == '_' {
		return fmt.Errorf("data_contract_name %q must not have a leading underscore (\"pump\", not \"_pump\")", c)
	}
	if reVersionSuffix.MatchString(c) {
		return fmt.Errorf("data_contract_name %q must not carry a version suffix (\"pump\", not \"pump_v1\")", c)
	}
	return nil
}

// ClassifyValue routes a value to value_num or value_text. A non-finite number is dropped
// (ok=false). Exactly one of num/text is non-nil. truncated is true when a text value was
// clipped to maxTextRunes (silent corruption otherwise -- the caller surfaces it).
func ClassifyValue(v any) (ValueType, *float64, *string, bool, bool) {
	switch tv := v.(type) {
	case bool:
		n := 0.0
		if tv {
			n = 1.0
		}
		return ValueNumeric, &n, nil, true, false
	case float64:
		if !isFinite(tv) {
			return "", nil, nil, false, false
		}
		return ValueNumeric, &tv, nil, true, false
	case int64:
		f := float64(tv)
		return ValueNumeric, &f, nil, true, false
	case int:
		f := float64(tv)
		return ValueNumeric, &f, nil, true, false
	case json.Number:
		f, err := tv.Float64()
		if err != nil || !isFinite(f) {
			return "", nil, nil, false, false
		}
		return ValueNumeric, &f, nil, true, false
	case string:
		text, truncated := truncateRunes(tv)
		return ValueText, nil, text, true, truncated
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", nil, nil, false, false
		}
		text, truncated := truncateRunes(string(b))
		return ValueText, nil, text, true, truncated
	}
}

func isFinite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

func truncateRunes(s string) (*string, bool) {
	r := []rune(s)
	if len(r) > maxTextRunes {
		s = string(r[:maxTextRunes])
		return &s, true
	}
	return &s, false
}

// ParseTimestampMs returns a UTC ISO-8601 string with milliseconds; ok=false when
// non-finite or out of range.
func ParseTimestampMs(v any) (string, bool) {
	var ms float64
	switch tv := v.(type) {
	case float64:
		ms = tv
	case int64:
		ms = float64(tv)
	case int:
		ms = float64(tv)
	case json.Number:
		f, err := tv.Float64()
		if err != nil {
			return "", false
		}
		ms = f
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(tv), 64)
		if err != nil {
			return "", false
		}
		ms = f
	default:
		return "", false
	}
	if !isFinite(ms) || ms < -maxJSDateMs || ms > maxJSDateMs {
		return "", false
	}
	// UnixMilli floors toward negative infinity, matching JS new Date(ms) for pre-1970 ms.
	return time.UnixMilli(int64(ms)).UTC().Format("2006-01-02T15:04:05.000Z"), true
}

// Row holds the values one message binds into the SQL queries.
type Row struct {
	RawLocation  string
	ContractName string
	VirtualPath  string
	TagName      string
	ValueType    ValueType
	TS           string
	ValueNum     *float64
	ValueText    *string
	Truncated    bool     // value_text was clipped to maxTextRunes (over-long string value)
	MetadataJSON string   // the metadata to write, set only when EmitMeta is true
	EmitMeta     bool     // write an attribute row (the metadata key set changed since last seen)
	churnKeys    []string // metadata keys that change nearly every message (defeat de-dup)
}

// DropReason labels a dropped message for the metric/log; "" (DropNone) means kept.
type DropReason string

const (
	DropNone                DropReason = ""
	DropInvalidTopic        DropReason = "invalid_topic"
	DropContractMismatch    DropReason = "contract_mismatch"
	DropServerVirtualPath   DropReason = "server_virtual_path"
	DropMissingValue        DropReason = "missing_value"
	DropMissingTimestamp    DropReason = "missing_timestamp"
	DropUnclassifiableValue DropReason = "unclassifiable_value"
	DropBadTimestamp        DropReason = "bad_timestamp"
	DropContractBypassed    DropReason = "contract_bypassed"
	DropNotTimeseries       DropReason = "not_timeseries"
	DropNotStructured       DropReason = "not_structured"
	DropNotObject           DropReason = "not_object"
)

var dropHints = map[DropReason]string{
	DropMissingValue:     ". The payload has no value field; the historian needs {value, timestamp_ms}",
	DropMissingTimestamp: ". The historian needs a {value, timestamp_ms} payload; the tag processor sets timestamp_ms automatically, otherwise ensure the payload carries it",
	DropContractBypassed: ". This versioned data contract carries data_contract_bypassed=true, so its schema was never applied (the registry was unreachable, or no schema is registered for this version) and the payload is unchecked; register the schema or restore the registry, then redeploy. This cannot be overridden by allow_unvalidated_data",
	DropNotTimeseries:    ". The historian stores timeseries only, and this payload carries fields beyond {value, timestamp_ms}; route relational data to a different data contract",
}

func dropHint(reason DropReason) string { return dropHints[reason] }

// datatypeFlipHint names the flag that stores a tag whose datatype changed, for the poison-row log.
// P0001 at the resolve phase identifies the flip exactly: the P0001 invariant in errclass.go admits
// only two sources, and the other one -- raise_pk_conflict, an append-only conflict the flag cannot
// fix -- can only fire on the value and attribute inserts.
func datatypeFlipHint(phase string, sqlstate string) string {
	if phase != phaseResolve || sqlstate != sqlstateRaise {
		return ""
	}
	return ". This tag is stored as a different datatype; set allow_datatype_changes: true on this output to keep both types on it, or fix the source so the tag emits one type"
}

// Transform maps one UNS message to a Row, or returns a non-empty DropReason to drop it.
func Transform(payload map[string]any, meta map[string]string, contract string, allMeta bool, allowlist []string, excl *MetaExcluder, view *BatchView) (*Row, DropReason) {
	// Parse the canonical umh_topic via the shared parser rather than trusting separate
	// location/contract/tag meta: a pipeline may not have run the tag_processor, so a missing or
	// malformed topic is dropped here.
	ut, err := topic.NewUnsTopic(meta["umh_topic"])
	if err != nil {
		return nil, DropInvalidTopic
	}
	info := ut.Info()

	want := "_" + contract
	if NormalizeContract(info.DataContract) != want {
		return nil, DropContractMismatch
	}
	// the version check is load-bearing, not a narrowing: the uns output stamps
	// data_contract_bypassed=true on EVERY unversioned message (uns_plugin/schema_validation/
	// validator.go:198-210, "unversioned contract - bypassing validation"), so hoisting this out of
	// the version check would drop all _historian traffic as contract_bypassed. On a versioned
	// contract the same meta means something else entirely: a schema was expected and not applied.
	if reVersionSuffix.MatchString(info.DataContract) && meta["data_contract_bypassed"] == "true" {
		return nil, DropContractBypassed
	}
	// NewUnsTopic already rejects empty/dotted location and empty name, so no re-check here.
	loc := info.LocationPath()
	tag := info.Name
	vp := info.GetVirtualPath()
	if vp == serverDiagnosticsPath || strings.HasPrefix(vp, serverDiagnosticsPath+".") {
		return nil, DropServerVirtualPath
	}
	for k := range payload {
		if k != "value" && k != "timestamp_ms" {
			return nil, DropNotTimeseries
		}
	}
	value, hasValue := payload["value"]
	if !hasValue || value == nil {
		return nil, DropMissingValue
	}
	tsRaw, hasTS := payload["timestamp_ms"]
	if !hasTS || tsRaw == nil {
		return nil, DropMissingTimestamp
	}
	vt, num, text, ok, truncated := ClassifyValue(value)
	if !ok {
		return nil, DropUnclassifiableValue
	}
	ts, ok := ParseTimestampMs(tsRaw)
	if !ok {
		return nil, DropBadTimestamp
	}
	row := &Row{
		RawLocation:  loc,
		ContractName: want,
		VirtualPath:  vp,
		TagName:      tag,
		ValueType:    vt,
		TS:           ts,
		ValueNum:     num,
		ValueText:    text,
		Truncated:    truncated,
	}
	keys := SelectMetaKeys(meta, allMeta, allowlist, excl)
	md := BuildMetadata(meta, keys)
	// Skip when there is no eligible metadata, so a metadata-less tag never writes an
	// attribute='{}' row. "\x00" joins the key fields because it cannot occur in any of them.
	if len(md) > 0 {
		row.churnKeys = HighChurnKeys(md)
		fp := Fingerprint(md)
		cacheKey := strings.Join([]string{want, CanonicalLtreePath(loc), vp, tag}, "\x00")
		if view.ShouldEmit(cacheKey, fp) {
			row.EmitMeta = true
			row.MetadataJSON = fp
		}
	}
	return row, DropNone
}
