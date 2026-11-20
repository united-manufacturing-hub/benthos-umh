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

// uns_beta_selective_micro_bench_test.go (ENG-5094): isolates the per-record
// processing-cost delta between the LEGACY uns input and the NEW uns_beta input
// for a HIGHLY SELECTIVE consumer (100k distinct topics, wants 1 -> ~all records
// dropped).
//
// WHY: legacy uns (uns_input_processor.go:68) filters on the raw record.Key
// BEFORE building any message, so a dropped record costs ~one regex match. uns_beta
// delegates to the official redpanda input, which builds the FULL *service.Message
// for EVERY record (value-wrap + all-headers materialized + native metas) BEFORE
// uns_beta's filter runs (betaKeyFilter.matches), so a dropped record pays full
// message construction then discards it. This benchmark measures exactly that delta.
// Network/fetch/decompression are IDENTICAL for both and are NOT in the benchmark.
//
// Scope: these benchmarks are uns_beta-specific. uns_beta pre-filters at
// the Kafka consumer via connect's key_pattern, so a dropped record never reaches
// the build-then-filter path measured here (it is dropped before a service.Message
// is constructed). uns_beta's dropped-record cost is therefore a single
// regex match at the source, structurally identical to legacy uns's pre-filter,
// and its end-to-end drain cost is covered by the {select_one, uns_beta}
// case in uns_beta_e2e_benchmark_test.go.
//
// All symbols are prefixed `micro` to avoid in-package redeclaration collisions.
package uns_plugin

import (
	"regexp"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Package-level sinks defeat compiler dead-store elision.
var (
	microSinkBool bool
	microSinkMsg  *service.Message
)

// microFranzRecordToMessageV1 is a VERBATIM copy of connect v4.94.1's
// internal/impl/kafka.FranzRecordToMessageV1 (internal/, not importable; it uses
// only the public service API so the copy is exact). This is the per-record build
// uns_beta's delegated OwnedInput runs before uns_beta's filter sees the record.
func microFranzRecordToMessageV1(record *kgo.Record) *service.Message {
	msg := service.NewMessage(record.Value)
	msg.MetaSetMut("kafka_key", record.Key)
	msg.MetaSetMut("kafka_topic", record.Topic)
	msg.MetaSetMut("kafka_partition", int(record.Partition))
	msg.MetaSetMut("kafka_offset", int(record.Offset))
	msg.MetaSetMut("kafka_timestamp_unix", record.Timestamp.Unix())
	msg.MetaSetMut("kafka_timestamp_ms", record.Timestamp.UnixMilli())
	msg.MetaSetMut("kafka_tombstone_message", record.Value == nil)
	microAddHeaders(msg, record.Headers)
	return msg
}

// microAddHeaders is a VERBATIM copy of connect v4.94.1's internal addHeaders.
func microAddHeaders(msg *service.Message, headers []kgo.RecordHeader) {
	if len(headers) == 0 {
		return
	}
	for _, h := range headers {
		if h.Value == nil {
			msg.MetaSetMut(h.Key, nil)
		} else if n := len(h.Value); n == 0 {
			msg.MetaSetMut(h.Key, "")
		} else {
			msg.MetaSetMut(h.Key, string(h.Value))
		}
	}
	msg.MetaSetMut("__rpcn_kafka_headers", headers)
}

// microKey is the umh_topic Kafka key of a representative umh.messages tag record
// (~58 bytes).
const microKey = "umh.v1.acme.berlin.line1.cell2._historian.axis.temperature"

// microValue is a representative time-series tag payload (~45 bytes JSON).
const microValue = `{"value":42.5,"timestamp_ms":1718380800000}`

// microHeaderPool holds realistic UMH-metadata kv pairs. Header count is a
// benchmark parameter (4/8/16); counts beyond the named entries pad with
// realistic-length values (~10-25 bytes).
var microHeaderPool = []kgo.RecordHeader{
	{Key: "location_path", Value: []byte("acme.berlin.line1.cell2")},
	{Key: "data_contract", Value: []byte("_historian")},
	{Key: "tag_name", Value: []byte("temperature")},
	{Key: "virtual_path", Value: []byte("axis")},
	{Key: "umh_topic", Value: []byte(microKey)},
	{Key: "serializer", Value: []byte("timeseries-number")},
	{Key: "bridged_by", Value: []byte("umh-core-protocolconverter")},
	{Key: "x-trace", Value: []byte("a1b2c3d4e5f60718")},
	{Key: "x-span-id", Value: []byte("0718f6e5d4c3b2a1")},
	{Key: "produced_at", Value: []byte("1718380800000")},
	{Key: "schema_version", Value: []byte("v1.2.0")},
	{Key: "source_node", Value: []byte("plc-cell2-axis")},
	{Key: "data_type", Value: []byte("float64")},
	{Key: "unit", Value: []byte("degC")},
	{Key: "quality", Value: []byte("good")},
	{Key: "asset_id", Value: []byte("axis-0042")},
}

// microHeaders returns the first n headers from the pool (n <= len(pool)).
func microHeaders(n int) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, n)
	copy(out, microHeaderPool[:n])
	return out
}

// microRecord builds a representative umh.messages tag record with n headers.
func microRecord(n int) *kgo.Record {
	return &kgo.Record{
		Key:       []byte(microKey),
		Value:     []byte(microValue),
		Topic:     "umh.messages",
		Partition: 0,
		Offset:    12345,
		Timestamp: time.Unix(1718380800, 0),
		Headers:   microHeaders(n),
	}
}

// microHeaderCounts is the header-count sweep (sensitivity to header volume).
var microHeaderCounts = []int{4, 8, 16}

// microNonMatchPattern does NOT match microKey (highly selective consumer that
// drops this record). Anchored, like the real umh_topics patterns.
const microNonMatchPattern = `^umh\.v1\.acme\.berlin\.line9\.cell9\._historian\.zone\.pressure$`

// microMatchPattern MATCHES microKey exactly (kept-record benchmarks).
const microMatchPattern = `^umh\.v1\.acme\.berlin\.line1\.cell2\._historian\.axis\.temperature$`

// =============================================================================
// HEADLINE: dropped-record cost (the decision).
// =============================================================================

// BenchmarkMicroDropped_LegacyUns is the ENTIRE legacy dropped-record cost: one
// regex match on the raw key, then return nil (mirrors uns_input_processor.go:68).
func BenchmarkMicroDropped_LegacyUns(b *testing.B) {
	legacyRe := regexp.MustCompile(microNonMatchPattern)
	for _, n := range microHeaderCounts {
		rec := microRecord(n)
		b.Run(microHeaderLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				microSinkBool = legacyRe.Match(rec.Key)
			}
		})
	}
}

// BenchmarkMicroDropped_UnsBeta mirrors the delegated build + uns_beta ReadBatch
// filter on a dropped record: the OwnedInput builds the full message, then
// uns_beta filters on the kafka_key meta and discards.
func BenchmarkMicroDropped_UnsBeta(b *testing.B) {
	betaFilter, err := newBetaKeyFilter([]string{microNonMatchPattern})
	if err != nil {
		b.Fatalf("newBetaKeyFilter: %v", err)
	}
	for _, n := range microHeaderCounts {
		rec := microRecord(n)
		b.Run(microHeaderLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg := microFranzRecordToMessageV1(rec)
				key, _ := msg.MetaGet("kafka_key")
				microSinkBool = betaFilter.matches(key)
			}
		})
	}
}

// =============================================================================
// CONTEXT: kept-record cost (records build in BOTH arms -> ~parity, proving
// non-selective consumers are unaffected).
// =============================================================================

// BenchmarkMicroKept_LegacyUns mirrors the legacy kept path: match, then build
// the legacy way (uns_input_processor.go:72-98).
func BenchmarkMicroKept_LegacyUns(b *testing.B) {
	legacyRe := regexp.MustCompile(microMatchPattern)
	for _, n := range microHeaderCounts {
		rec := microRecord(n)
		b.Run(microHeaderLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				microSinkBool = legacyRe.Match(rec.Key)
				msg := service.NewMessage(rec.Value)
				// Default metadataFormat is MetadataFormatString -> string(h.Value).
				for _, h := range rec.Headers {
					msg.MetaSetMut(h.Key, string(h.Value))
				}
				msg.MetaSetMut("kafka_msg_key", rec.Key)
				msg.MetaSetMut("kafka_topic", rec.Topic)
				msg.MetaSetMut("umh_topic", string(rec.Key))
				msg.MetaSetMut("kafka_timestamp_ms", microItoa(rec.Timestamp.UnixMilli()))
				microSinkMsg = msg
			}
		})
	}
}

// BenchmarkMicroKept_UnsBeta mirrors the kept path on uns_beta: delegated build,
// filter passes, then the kept-record meta fixups uns_beta applies.
func BenchmarkMicroKept_UnsBeta(b *testing.B) {
	betaFilter, err := newBetaKeyFilter([]string{microMatchPattern})
	if err != nil {
		b.Fatalf("newBetaKeyFilter: %v", err)
	}
	for _, n := range microHeaderCounts {
		rec := microRecord(n)
		b.Run(microHeaderLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg := microFranzRecordToMessageV1(rec)
				key, _ := msg.MetaGet("kafka_key")
				if betaFilter.matches(key) {
					msg.MetaSetMut("umh_topic", key)
					msg.MetaSetMut("kafka_msg_key", key)
					msg.MetaDelete("__rpcn_kafka_headers")
				}
				microSinkMsg = msg
			}
		})
	}
}

// microHeaderLabel names a subtest by its header count.
func microHeaderLabel(n int) string {
	switch n {
	case 4:
		return "headers=4"
	case 8:
		return "headers=8"
	case 16:
		return "headers=16"
	default:
		return "headers=" + microItoa(int64(n))
	}
}

// microItoa renders the legacy fmt.Sprintf("%d", ...) timestamp meta without
// pulling fmt into the hot loop; matches uns_input_processor.go:98 output.
func microItoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
