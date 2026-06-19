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

package timescaledb_historian_plugin

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
)

// NormalizeContract strips a trailing _vN (all versions share one table).
func NormalizeContract(metaContract string) string {
	return reVersionSuffix.ReplaceAllString(metaContract, "")
}

// ValidateContract enforces the bare-lowercase-name rule from the template.
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

// LocationNormalizesToEmpty reports whether to_ltree_path() would return NULL,
// i.e. every "."-split segment is empty. Non-word characters become "_" in SQL
// (they never vanish), so any non-empty segment survives; removing all "." thus
// leaves "" iff all segments were empty. Depends only on the ASCII "." split, so
// it cannot drift from the SQL function.
func LocationNormalizesToEmpty(locationPath string) bool {
	return strings.ReplaceAll(locationPath, ".", "") == ""
}

// ClassifyValue routes a payload value to value_num or value_text, mirroring the
// template's JS typing. Numbers arrive as float64 (benthos structured decoding);
// json.Number is tolerated. A non-finite number is dropped (ok=false), never coerced.
// Exactly one of num/text is non-nil; the unused one stays nil -> SQL NULL.
func ClassifyValue(v any) (valueType string, num *float64, text *string, ok bool) {
	switch tv := v.(type) {
	case bool:
		n := 0.0
		if tv {
			n = 1.0
		}
		return "numeric", &n, nil, true
	case float64:
		if !isFinite(tv) {
			return "", nil, nil, false
		}
		return "numeric", &tv, nil, true
	case json.Number:
		f, err := tv.Float64()
		if err != nil || !isFinite(f) {
			return "", nil, nil, false
		}
		return "numeric", &f, nil, true
	case string:
		return "text", nil, truncateRunes(tv), true
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", nil, nil, false
		}
		return "text", nil, truncateRunes(string(b)), true
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

// ParseTimestampMs mirrors the template's Number(timestamp_ms)+new Date() guards.
// Returns a UTC ISO-8601 string with milliseconds; ok=false when non-finite or out of range.
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
	msInt := int64(ms)
	sec := msInt / 1000
	nsec := (msInt % 1000) * int64(time.Millisecond)
	return time.Unix(sec, nsec).UTC().Format("2006-01-02T15:04:05.000Z"), true
}

// Row is the result of transforming one UNS message into the values the SQL
// queries bind. RawLocation is bound at $1 and the SQL wraps it in to_ltree_path().
type Row struct {
	RawLocation  string
	ContractName string
	VirtualPath  string
	TagName      string
	ValueType    string
	TS           string
	ValueNum     *float64
	ValueText    *string
	MetadataJSON string
	EmitMeta     bool
	churnKeys    []string
}

// Transform maps one UNS message to a Row, or returns ok=false to drop it
// silently. Order matches the spec: contract -> presence -> empty-path ->
// typing -> timestamp -> metadata.
func Transform(payload map[string]any, meta map[string]string, contract string, allMeta bool, allowlist []string, view *BatchView) (*Row, bool) {
	// 1. contract match (incoming data_contract carries a leading "_")
	want := "_" + contract
	if NormalizeContract(meta["data_contract"]) != want {
		return nil, false
	}
	// 2. presence guards (null/absent only; 0/false/"" are valid)
	loc := meta["location_path"]
	tag := meta["tag_name"]
	if loc == "" || tag == "" {
		return nil, false
	}
	if vp := meta["virtual_path"]; strings.HasPrefix(vp, "Root.Objects.Server") {
		return nil, false
	}
	value, hasValue := payload["value"]
	tsRaw, hasTS := payload["timestamp_ms"]
	if !hasValue || value == nil || !hasTS || tsRaw == nil {
		return nil, false
	}
	// 3. empty-path drop (canonicalization itself happens in SQL via to_ltree_path)
	if LocationNormalizesToEmpty(loc) {
		return nil, false
	}
	// 4. typing
	vt, num, text, ok := ClassifyValue(value)
	if !ok {
		return nil, false
	}
	// 5. timestamp
	ts, ok := ParseTimestampMs(tsRaw)
	if !ok {
		return nil, false
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
	// 6. metadata: select -> build -> fingerprint -> dedup
	keys := SelectMetaKeys(meta, allMeta, allowlist)
	md := BuildMetadata(meta, keys)
	row.churnKeys = HighChurnKeys(md)
	fp := Fingerprint(md)
	cacheKey := "md:" + want + ":" + loc + ":" + meta["virtual_path"] + ":" + tag
	if view.ShouldEmit(cacheKey, fp) {
		row.EmitMeta = true
		row.MetadataJSON = fp
	}
	return row, true
}
