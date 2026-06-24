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
)

const (
	maxTextRunes = 8192
	maxJSDateMs  = 8.64e15 // JS Date valid range is +/- this
)

var (
	reVersionSuffix = regexp.MustCompile(`_v\d+$`)
	reContract      = regexp.MustCompile(`^[a-z0-9_]+$`)
	reNonLtreeLabel = regexp.MustCompile(`[^A-Za-z0-9_]`)
)

// ValueType is the umh.value_type SQL enum domain.
type ValueType string

const (
	ValueNumeric ValueType = "numeric"
	ValueText    ValueType = "text"
)

// CanonicalLtreePath mirrors the SQL umh.to_ltree_path() so the in-process dedup key
// shares the DB's topic identity (raw "line-1" and "line_1" both map to "line_1").
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

func ValidateContract(c string) error {
	if !reContract.MatchString(c) {
		return fmt.Errorf("data_contract %q invalid: use a bare lowercase name (letters, digits, underscores), e.g. \"pump\"", c)
	}
	if c[0] == '_' {
		return fmt.Errorf("data_contract %q must not have a leading underscore (\"pump\", not \"_pump\")", c)
	}
	if reVersionSuffix.MatchString(c) {
		return fmt.Errorf("data_contract %q must not carry a version suffix (\"pump\", not \"pump_v1\")", c)
	}
	return nil
}

// LocationNormalizesToEmpty reports whether umh.to_ltree_path() would return NULL. Only
// "." is dropped by the split; every other char survives as itself or "_", so the path is
// empty iff all "."-split segments were empty.
func LocationNormalizesToEmpty(locationPath string) bool {
	return strings.ReplaceAll(locationPath, ".", "") == ""
}

// ClassifyValue routes a value to value_num or value_text. A non-finite number is dropped
// (ok=false). Exactly one of num/text is non-nil.
func ClassifyValue(v any) (ValueType, *float64, *string, bool) {
	switch tv := v.(type) {
	case bool:
		n := 0.0
		if tv {
			n = 1.0
		}
		return ValueNumeric, &n, nil, true
	case float64:
		if !isFinite(tv) {
			return "", nil, nil, false
		}
		return ValueNumeric, &tv, nil, true
	case json.Number:
		f, err := tv.Float64()
		if err != nil || !isFinite(f) {
			return "", nil, nil, false
		}
		return ValueNumeric, &f, nil, true
	case string:
		return ValueText, nil, truncateRunes(tv), true
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", nil, nil, false
		}
		return ValueText, nil, truncateRunes(string(b)), true
	}
}

func isFinite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

func truncateRunes(s string) *string {
	r := []rune(s)
	if len(r) > maxTextRunes {
		s = string(r[:maxTextRunes])
	}
	return &s
}

// ParseTimestampMs returns a UTC ISO-8601 string with milliseconds; ok=false when
// non-finite or out of range.
func ParseTimestampMs(v any) (string, bool) {
	var ms float64
	switch tv := v.(type) {
	case float64:
		ms = tv
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
	MetadataJSON string
	EmitMeta     bool
	churnKeys    []string
}

// DropReason labels a dropped message for the metric/log; "" (DropNone) means kept.
type DropReason string

const (
	DropNone                    DropReason = ""
	DropContractMismatch        DropReason = "contract_mismatch"
	DropMissingLocationOrTag    DropReason = "missing_location_or_tag"
	DropServerVirtualPath       DropReason = "server_virtual_path"
	DropMissingValueOrTimestamp DropReason = "missing_value_or_timestamp"
	DropEmptyLocation           DropReason = "empty_location"
	DropUnclassifiableValue     DropReason = "unclassifiable_value"
	DropBadTimestamp            DropReason = "bad_timestamp"
)

// Transform maps one UNS message to a Row, or returns a non-empty DropReason to drop it.
func Transform(payload map[string]any, meta map[string]string, contract string, allMeta bool, allowlist []string, excl *MetaExcluder, view *BatchView) (*Row, DropReason) {
	want := "_" + contract
	if NormalizeContract(meta["data_contract"]) != want {
		return nil, DropContractMismatch
	}
	loc := meta["location_path"]
	tag := meta["tag_name"]
	if loc == "" || tag == "" {
		return nil, DropMissingLocationOrTag
	}
	if vp := meta["virtual_path"]; strings.HasPrefix(vp, "Root.Objects.Server") {
		return nil, DropServerVirtualPath
	}
	value, hasValue := payload["value"]
	tsRaw, hasTS := payload["timestamp_ms"]
	if !hasValue || value == nil || !hasTS || tsRaw == nil {
		return nil, DropMissingValueOrTimestamp
	}
	if LocationNormalizesToEmpty(loc) {
		return nil, DropEmptyLocation
	}
	vt, num, text, ok := ClassifyValue(value)
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
		VirtualPath:  meta["virtual_path"],
		TagName:      tag,
		ValueType:    vt,
		TS:           ts,
		ValueNum:     num,
		ValueText:    text,
	}
	keys := SelectMetaKeys(meta, allMeta, allowlist, excl)
	md := BuildMetadata(meta, keys)
	// Skip when there is no eligible metadata, so a metadata-less tag never writes an
	// attribute='{}' row. "\x00" joins the key fields because it cannot occur in any of them.
	if len(md) > 0 {
		row.churnKeys = HighChurnKeys(md)
		fp := Fingerprint(md)
		cacheKey := strings.Join([]string{want, CanonicalLtreePath(loc), meta["virtual_path"], tag}, "\x00")
		if view.ShouldEmit(cacheKey, fp) {
			row.EmitMeta = true
			row.MetadataJSON = fp
		}
	}
	return row, DropNone
}
