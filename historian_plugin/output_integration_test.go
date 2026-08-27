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

package historian_plugin_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	tsh "github.com/united-manufacturing-hub/benthos-umh/historian_plugin"
)

var (
	sharedDSN   string
	pgContainer *postgres.PostgresContainer
)

func mkMsg(value any, tsMs float64, contract string, loc string, tag string, extraMeta map[string]string) *service.Message {
	m := service.NewMessage(nil)
	m.SetStructured(map[string]any{"value": value, "timestamp_ms": tsMs})
	m.MetaSet("data_contract", contract)
	m.MetaSet("location_path", loc)
	m.MetaSet("tag_name", tag)
	m.MetaSet("virtual_path", "vibration")
	// The historian now derives location/contract/virtual_path/tag from the canonical
	// umh_topic, so set it (as the tag_processor would) rather than the loose fields above.
	m.MetaSet("umh_topic", "umh.v1."+loc+"."+contract+".vibration."+tag)
	for k, v := range extraMeta {
		m.MetaSet(k, v)
	}
	return m
}

func mkMsgNoTimestamp(value any, contract string, loc string, tag string, extraMeta map[string]string) *service.Message {
	m := mkMsg(value, 0, contract, loc, tag, extraMeta)
	m.SetStructured(map[string]any{"value": value})
	return m
}

var _ = Describe("TimescaleDB integration", Ordered, Label("postgres"), func() {
	var ctx context.Context

	BeforeAll(func() {
		if os.Getenv("TEST_HISTORIAN") == "" {
			Skip("set TEST_HISTORIAN=true to run TimescaleDB integration tests")
		}
		ctx = context.Background()
		c, err := postgres.Run(ctx, "timescale/timescaledb:latest-pg18",
			postgres.WithDatabase("umh"),
			postgres.WithUsername("umh_owner"),
			postgres.WithPassword("secret"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(90*time.Second)),
		)
		Expect(err).NotTo(HaveOccurred())
		pgContainer = c
		sharedDSN, err = c.ConnectionString(ctx, "sslmode=disable")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if pgContainer != nil {
			_ = pgContainer.Terminate(context.Background())
		}
	})

	connected := func(contract string) *tsh.HistorianTestHandle {
		h := tsh.NewHistorianTestHandle(sharedDSN, contract)
		Expect(h.Connect(ctx)).To(Succeed())
		return h
	}

	seconds := func(d time.Duration) int64 { return int64(d.Seconds()) }

	It("creates each hypertable with its own configured chunk interval", func() {
		valueChunk := 24 * time.Hour
		attributeChunk := 720 * time.Hour

		h := tsh.NewHistorianTestHandle(sharedDSN, "chunky")
		h.SetChunkIntervals(valueChunk, attributeChunk)
		Expect(h.Connect(ctx)).To(Succeed())
		defer h.Close(ctx)

		Expect(h.AppliedChunkInterval(ctx, "value_chunky")).To(HaveValue(Equal(seconds(valueChunk))))
		Expect(h.AppliedChunkInterval(ctx, "attribute_chunky")).To(HaveValue(Equal(seconds(attributeChunk))))
	})

	It("warns on restart when the configured chunk interval no longer matches the table", func() {
		createdChunk := 168 * time.Hour
		reconfiguredChunk := 24 * time.Hour
		unchangedAttributeChunk := 168 * time.Hour

		first := tsh.NewHistorianTestHandle(sharedDSN, "chunkdrift")
		first.SetChunkIntervals(createdChunk, unchangedAttributeChunk)
		Expect(first.Connect(ctx)).To(Succeed())
		first.Close(ctx)

		restarted := tsh.NewHistorianTestHandle(sharedDSN, "chunkdrift")
		restarted.SetChunkIntervals(reconfiguredChunk, unchangedAttributeChunk)
		logs := restarted.CaptureLogs()
		Expect(restarted.Connect(ctx)).To(Succeed())
		defer restarted.Close(ctx)

		Expect(logs()).To(ContainSubstring(fmt.Sprintf("configured value_chunk_interval (%ds)", seconds(reconfiguredChunk))))
		Expect(logs()).To(ContainSubstring(fmt.Sprintf("created with (%ds)", seconds(createdChunk))))
		Expect(logs()).To(ContainSubstring("stays in force"))
		Expect(logs()).NotTo(ContainSubstring("attribute_chunk_interval"))
		Expect(restarted.AppliedChunkInterval(ctx, "value_chunkdrift")).To(HaveValue(Equal(seconds(createdChunk))))
	})

	It("names the table when the applied chunk interval cannot be read", func() {
		h := connected("chunkunread")
		defer h.Close(ctx)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		logs := h.CaptureLogs()
		h.WarnChunkDrift(cancelled)

		Expect(logs()).To(ContainSubstring("cannot read the chunk interval of umh.value_chunkunread"))
		Expect(logs()).NotTo(ContainSubstring("stays in force"))
	})

	It("warns on restart when the applied compression policy no longer matches config", func() {
		appliedCompress := 24 * time.Hour
		handleDefaultCompress := 168 * time.Hour

		first := connected("poldrift")
		Expect(first.ExecSQL(ctx, "ALTER TABLE umh.value_poldrift SET (timescaledb.compress, timescaledb.compress_segmentby = 'topic_id', timescaledb.compress_orderby = 'ts DESC')")).To(Succeed())
		Expect(first.ExecSQL(ctx, "SELECT remove_compression_policy('umh.value_poldrift', if_exists => TRUE)")).To(Succeed())
		Expect(first.ExecSQL(ctx, fmt.Sprintf("SELECT add_compression_policy('umh.value_poldrift', INTERVAL '%d seconds')", seconds(appliedCompress)))).To(Succeed())
		first.Close(ctx)

		restarted := tsh.NewHistorianTestHandle(sharedDSN, "poldrift")
		logs := restarted.CaptureLogs()
		Expect(restarted.Connect(ctx)).To(Succeed())
		defer restarted.Close(ctx)

		Expect(logs()).To(ContainSubstring(fmt.Sprintf("configured compress_after (%ds)", seconds(handleDefaultCompress))))
		Expect(logs()).To(ContainSubstring(fmt.Sprintf("applied in the database (%ds)", seconds(appliedCompress))))
		Expect(logs()).To(ContainSubstring("stays in force"))
	})

	It("bootstraps idempotently (Connect twice)", func() {
		h := connected("pump")
		defer h.Close(ctx)
		Expect(h.Connect(ctx)).To(Succeed())
	})

	It("a fresh handle connects to an already-bootstrapped database and reads/writes (restart path)", func() {
		// First handle bootstraps the schema and writes one point.
		h1 := connected("recon")
		defer h1.Close(ctx)
		Expect(h1.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_recon_v1", "acme.line1", "x", nil),
		})).To(Succeed())

		// A second, independent handle (bootstrapped == false) connects to the SAME database --
		// the real restart path, not the same-handle early return. Bootstrap runs again and must
		// be idempotent; the ledger-gated policy block is skipped rather than re-applied.
		h2 := tsh.NewHistorianTestHandle(sharedDSN, "recon")
		Expect(h2.Connect(ctx)).To(Succeed())
		defer h2.Close(ctx)

		// h2 sees the recorded schema version and h1's data through the documented read path...
		Expect(h2.SchemaVersion(ctx)).To(Equal(1))
		Expect(h2.CountValueRows(ctx, "recon")).To(Equal(1))
		id, ok := h2.GetTopicID(ctx, "acme.line1", "vibration", "recon", "x")
		Expect(ok).To(BeTrue())
		Expect(h2.ValueWindow(ctx, "recon", id, 0, 2000)).To(Equal([]float64{1.0}))

		// ...and can write further points itself.
		Expect(h2.WriteBatch(ctx, service.MessageBatch{
			mkMsg(2.0, 2000, "_recon_v1", "acme.line1", "x", nil),
		})).To(Succeed())
		Expect(h2.CountValueRows(ctx, "recon")).To(Equal(2))
	})

	It("records the baseline schema version in the ledger after bootstrap", func() {
		h := connected("pump")
		defer h.Close(ctx)
		// Greenfield baseline is version 1; max(version) reflects it.
		Expect(h.SchemaVersion(ctx)).To(Equal(1))
		Expect(h.SchemaVersion(ctx)).To(Equal(tsh.SchemaVersionForTest()))
		// Re-bootstrapping does not re-apply or duplicate the recorded version.
		Expect(h.Connect(ctx)).To(Succeed())
		Expect(h.SchemaVersion(ctx)).To(Equal(1))
	})

	It("does not advance the topic sequence when re-resolving an existing topic", func() {
		h := connected("noburn")
		defer h.Close(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_noburn_v1", "acme.line1", "x", nil),
		})).To(Succeed())
		seq := h.TopicSeqValue(ctx) // topic now exists; sequence at its id
		// Re-resolve the same topic across several further batches (distinct ts) -> all lookup
		// hits -> no sequence burn.
		for i := 2; i <= 6; i++ {
			Expect(h.WriteBatch(ctx, service.MessageBatch{
				mkMsg(float64(i), float64(i*1000), "_noburn_v1", "acme.line1", "x", nil),
			})).To(Succeed())
		}
		Expect(h.TopicSeqValue(ctx)).To(Equal(seq), "re-resolving an existing topic must not burn the sequence")
		Expect(h.CountValueRows(ctx, "noburn")).To(Equal(6))
	})

	It("serves a repeat topic from the process cache with no further DB resolve", func() {
		h := connected("tcache")
		defer h.Close(ctx)
		// First write resolves the new topic and caches its id.
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_tcache_v1", "acme.line1", "x", nil),
		})).To(Succeed())
		hits, misses := h.LookupHits(), h.LookupMisses()
		// Subsequent batches for the SAME topic are served from the process cache: no read-first
		// lookup and no fall-through to the upsert, so neither DB-resolve counter advances.
		for i := 2; i <= 5; i++ {
			Expect(h.WriteBatch(ctx, service.MessageBatch{
				mkMsg(float64(i), float64(i*1000), "_tcache_v1", "acme.line1", "x", nil),
			})).To(Succeed())
		}
		Expect(h.LookupHits()).To(Equal(hits), "a cached topic must not issue a read-first DB lookup")
		Expect(h.LookupMisses()).To(Equal(misses), "a cached topic must not fall through to the upsert")
		Expect(h.CountValueRows(ctx, "tcache")).To(Equal(5))
	})

	It("purges the topic cache on reconnect so a cached topic is re-resolved against the DB", func() {
		h := connected("tcpurge")
		defer h.Close(ctx)
		// First write resolves the new topic (one miss -> upsert) and caches its id.
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_tcpurge_v1", "acme.line1", "x", nil),
		})).To(Succeed())
		hits, misses := h.LookupHits(), h.LookupMisses()

		// A reconnect can land on a restored/recreated DB, so Connect purges the caches. After it,
		// the SAME topic must go back to the DB (a read-first lookup hit) rather than be served from
		// the now-empty process cache -- otherwise a stale id could misroute writes (no FK catches it).
		Expect(h.Connect(ctx)).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(2.0, 2000, "_tcpurge_v1", "acme.line1", "x", nil),
		})).To(Succeed())
		Expect(h.LookupHits()).To(Equal(hits+1), "after a reconnect the cached topic must be re-resolved via a DB lookup")
		Expect(h.LookupMisses()).To(Equal(misses), "re-resolving an existing topic hits the lookup, it must not fall through to the upsert")
		Expect(h.CountValueRows(ctx, "tcpurge")).To(Equal(2))
	})

	It("re-warms after restart (fresh handle) without bumping the sequence", func() {
		h1 := connected("restart2")
		defer h1.Close(ctx)
		Expect(h1.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_restart2_v1", "acme.line1", "a", nil),
			mkMsg(2.0, 1000, "_restart2_v1", "acme.line1", "b", nil),
		})).To(Succeed())
		seq := h1.TopicSeqValue(ctx)

		// A fresh handle == the restart path (no in-process state). Writing the SAME, existing
		// topics resolves them via lookup with no sequence bump.
		h2 := tsh.NewHistorianTestHandle(sharedDSN, "restart2")
		Expect(h2.Connect(ctx)).To(Succeed())
		defer h2.Close(ctx)
		Expect(h2.WriteBatch(ctx, service.MessageBatch{
			mkMsg(3.0, 2000, "_restart2_v1", "acme.line1", "a", nil),
			mkMsg(4.0, 2000, "_restart2_v1", "acme.line1", "b", nil),
		})).To(Succeed())
		Expect(h2.TopicSeqValue(ctx)).To(Equal(seq), "existing topics must resolve via lookup after restart with no sequence bump")
		Expect(h2.LookupMisses()).To(Equal(int64(0)), "both topics already existed -> no misses")
	})

	It("advances the topic sequence by exactly one per new topic (serial writer)", func() {
		h := connected("newone")
		defer h.Close(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_newone_v1", "acme.line1", "first", nil),
		})).To(Succeed())
		seq := h.TopicSeqValue(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_newone_v1", "acme.line1", "second", nil),
		})).To(Succeed())
		Expect(h.TopicSeqValue(ctx)).To(Equal(seq+1), "a new topic (serial writer) advances the sequence by exactly one")
	})

	It("a datatype flip goes through the lookup-miss path and is dropped as poison", func() {
		h := connected("flip2")
		defer h.Close(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_flip2_v1", "l.a", "t", nil),
		})).To(Succeed())
		misses := h.LookupMisses()
		// Same tag, now text: the value_type-aware lookup MISSES -> guarded upsert -> tag guard
		// RAISEs P0001, which is classified as poison and the row is dropped (ACK). A value_type-
		// agnostic lookup would HIT and silently skip the guard, so this asserts the resolve went
		// through the miss path. LookupMisses increases by >=1 (the fast path and the isolated
		// retry each re-resolve the flip).
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg("now-text", 2000, "_flip2_v1", "l.a", "t", nil),
		})).To(Succeed())
		Expect(h.LookupMisses()).To(BeNumerically(">=", misses+1), "the flip must miss the value_type-aware lookup and fall to the upsert")
		Expect(h.CountValueRows(ctx, "flip2")).To(Equal(1)) // numeric kept, flip dropped
	})

	It("names the flag that would have stored the flip in the poison log", func() {
		h := connected("fliphint")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_fliphint_v1", "l.a", "t", nil),
		})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg("now-text", 2000, "_fliphint_v1", "l.a", "t", nil),
		})).To(Succeed())
		Expect(logs()).To(ContainSubstring("level=error"))
		Expect(logs()).To(ContainSubstring("dropped poison row at resolve"))
		Expect(logs()).To(ContainSubstring("allow_datatype_changes: true"), "the operator must learn the fix from the log, not from the docs")
	})

	It("leaves an append-only value conflict unhinted, since the flag cannot fix it", func() {
		h := connected("conflicthint")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_conflicthint_v1", "l.a", "t", nil),
		})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(2.0, 1000, "_conflicthint_v1", "l.a", "t", nil),
		})).To(Succeed())
		Expect(logs()).To(ContainSubstring("dropped poison row at value"))
		Expect(logs()).NotTo(ContainSubstring("allow_datatype_changes"), "two values at one timestamp is not a datatype problem")
	})

	It("keeps both datatypes for one tag when datatype changes are allowed", func() {
		h := connected("flipok")
		defer h.SetAllowDatatypeChanges(false)
		defer h.Close(ctx)
		h.SetAllowDatatypeChanges(true)
		logs := h.CaptureLogs()
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_flipok", "l.a", "t", nil),
		})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg("now-text", 2000, "_flipok", "l.a", "t", nil),
		})).To(Succeed())
		Expect(h.CountValueRows(ctx, "flipok")).To(Equal(2), "the flag stores both types on one tag")
		Expect(logs()).NotTo(ContainSubstring("dropped poison row"))
		id, ok := h.GetTopicID(ctx, "l.a", "vibration", "flipok", "t")
		Expect(ok).To(BeTrue())
		num, text := h.ValueRow(ctx, "flipok", id)
		Expect(num).NotTo(BeNil(), "the first row keeps its numeric column")
		Expect(text).To(BeNil())
	})

	It("two handles concurrently creating the same new topic converge on one id, no dropped rows", func() {
		hA := tsh.NewHistorianTestHandle(sharedDSN, "multi")
		Expect(hA.Connect(ctx)).To(Succeed())
		defer hA.Close(ctx)
		hB := tsh.NewHistorianTestHandle(sharedDSN, "multi")
		Expect(hB.Connect(ctx)).To(Succeed())
		defer hB.Close(ctx)

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			errs[0] = hA.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 1000, "_multi_v1", "l.a", "shared", nil)})
		}()
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			errs[1] = hB.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 1000, "_multi_v1", "l.a", "shared", nil)})
		}()
		wg.Wait()
		Expect(errs[0]).NotTo(HaveOccurred())
		Expect(errs[1]).NotTo(HaveOccurred())
		// Both resolve to one topic; the identical (topic_id, ts, value) write is absorbed once.
		Expect(hA.CountValueRows(ctx, "multi")).To(Equal(1))
		_, ok := hA.GetTopicID(ctx, "l.a", "vibration", "multi", "shared")
		Expect(ok).To(BeTrue())
	})

	It("batched writes scale with workers and beat per-message (no batching)", Label("load"), func() {
		const totalRows = 4000
		poolDSN := sharedDSN + "&pool_max_conns=16"

		// concurrently runs `work` across `workers` goroutines and returns rows/s. sharedTag
		// makes every worker hit the SAME tag (max dimension-row contention); false gives each
		// its own tag. work(w, tag, base) writes totalRows/workers rows for worker w.
		measure := func(h *tsh.HistorianTestHandle, contract string, workers int, sharedTag bool,
			work func(w int, tag string, base int),
		) float64 {
			perWorker := totalRows / workers
			var wg sync.WaitGroup
			start := time.Now()
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer GinkgoRecover()
					defer wg.Done()
					tag := "x"
					if !sharedTag {
						tag = fmt.Sprintf("t%d", w)
					}
					work(w, tag, w*perWorker)
				}(w)
			}
			wg.Wait()
			elapsed := time.Since(start)
			Expect(h.CountValueRows(ctx, contract)).To(Equal(totalRows))
			return float64(totalRows) / elapsed.Seconds()
		}

		// Plugin: the real two-phase WriteBatch, batches of 1000.
		runPlugin := func(contract string, workers int, sharedTag bool) float64 {
			h := tsh.NewHistorianTestHandle(poolDSN, contract)
			Expect(h.Connect(ctx)).To(Succeed())
			defer h.Close(ctx)
			full := "_" + contract + "_v1"
			perWorker := totalRows / workers
			return measure(h, contract, workers, sharedTag, func(_ int, tag string, base int) {
				const B = 1000
				for i := 0; i < perWorker; i += B {
					bsz := B
					if i+bsz > perWorker {
						bsz = perWorker - i
					}
					batch := make(service.MessageBatch, bsz)
					for j := 0; j < bsz; j++ {
						batch[j] = mkMsg(float64(i+j), float64(base+i+j+1), full, "acme.line1", tag, nil)
					}
					Expect(h.WriteBatch(ctx, batch)).To(Succeed())
				}
			})
		}

		// No batching: the same write as one combined statement per row, autocommit.
		runPerMessage := func(contract string, workers int, sharedTag bool) float64 {
			h := tsh.NewHistorianTestHandle(poolDSN, contract)
			Expect(h.Connect(ctx)).To(Succeed())
			defer h.Close(ctx)
			cn := "_" + contract
			perWorker := totalRows / workers
			return measure(h, contract, workers, sharedTag, func(_ int, tag string, base int) {
				for i := 0; i < perWorker; i++ {
					num := float64(i)
					ts := time.UnixMilli(int64(base + i + 1)).UTC().Format("2006-01-02T15:04:05.000Z")
					Expect(h.WritePerMessageValue(ctx, "acme.line1", cn, "vibration", tag, "numeric", ts, &num, nil)).To(Succeed())
				}
			})
		}

		var batchedW1, batchedW8, perMsgW8 float64
		for di, sharedTag := range []bool{false, true} {
			label := "distinct-tags"
			if sharedTag {
				label = "shared-tag  "
			}
			GinkgoWriter.Printf("\n%s (%d rows):\n", label, totalRows)
			for _, w := range []int{1, 4, 8} {
				batched := runPlugin(fmt.Sprintf("plug%dw%d", di, w), w, sharedTag)
				perMsg := runPerMessage(fmt.Sprintf("perm%dw%d", di, w), w, sharedTag)
				GinkgoWriter.Printf("  workers=%d  batched=%.0f rows/s  per-message=%.0f rows/s  ratio=%.2fx\n",
					w, batched, perMsg, batched/perMsg)
				if !sharedTag { // record the distinct-tags case for the guards below
					switch w {
					case 1:
						batchedW1 = batched
					case 8:
						batchedW8, perMsgW8 = batched, perMsg
					}
				}
			}
		}
		// Regression guards (generous margins below the ~2.9x batched-vs-per-message and ~1.4x
		// worker-scaling observed on CI, so they assert the structural property -- batching scales
		// with max_in_flight and beats per-message -- without being flaky on slower CI hardware,
		// where worker scaling plateaus past 4 workers).
		Expect(batchedW8).To(BeNumerically(">", perMsgW8*1.2),
			"batched must be at least as performant as per-message (no batching) at max_in_flight=8")
		Expect(batchedW8).To(BeNumerically(">", batchedW1*1.25),
			"batched throughput must scale with worker count (regression: it was flat before the two-phase write)")
	})

	It("resolves a high-cardinality batch, then serves the warm batch entirely from the topic cache", Label("load"), func() {
		h := connected("hicard")
		defer h.Close(ctx)
		const topics = 1000
		mkBatch := func(baseTS int) service.MessageBatch {
			b := make(service.MessageBatch, topics)
			for i := 0; i < topics; i++ {
				b[i] = mkMsg(float64(i), float64(baseTS+i), "_hicard_v1", "acme.line1", fmt.Sprintf("tag%d", i), nil)
			}
			return b
		}
		// Cold: every one of the 1000 topics is new -> read-first lookup misses -> guarded upsert.
		t0 := time.Now()
		Expect(h.WriteBatch(ctx, mkBatch(1_000_000))).To(Succeed())
		cold := time.Since(t0)
		Expect(h.CountValueRows(ctx, "hicard")).To(Equal(topics))
		misses, hits := h.LookupMisses(), h.LookupHits()
		Expect(misses).To(Equal(int64(topics)), "cold batch resolves every distinct topic via the DB")

		// Warm: the same 1000 topics at a new ts. Every resolve is served from the process cache, so
		// Phase 1 issues zero DB round-trips -- neither the miss nor the read-first-hit counter moves.
		t1 := time.Now()
		Expect(h.WriteBatch(ctx, mkBatch(2_000_000))).To(Succeed())
		warm := time.Since(t1)
		Expect(h.CountValueRows(ctx, "hicard")).To(Equal(2 * topics))
		Expect(h.LookupMisses()).To(Equal(misses), "warm batch must not fall through to the upsert")
		Expect(h.LookupHits()).To(Equal(hits), "warm batch must not issue a read-first DB lookup (served from cache)")
		GinkgoWriter.Printf("\nhigh-cardinality %d topics: cold=%s  warm=%s\n", topics, cold, warm)
	})

	It("fails Connect on wrong credentials, then retries cleanly", func() {
		h := tsh.NewHistorianTestHandle("postgres://umh_owner:wrong@"+hostPort()+"/umh?sslmode=disable", "pump")
		Expect(h.Connect(ctx)).NotTo(Succeed())
		h.SetDSN(sharedDSN)
		Expect(h.Connect(ctx)).To(Succeed())
		_ = h.Close(ctx)
	})

	It("verifies the to_ltree_path SQL port (value + NULL boundary)", func() {
		h := connected("pump")
		defer h.Close(ctx)
		type tc struct {
			in       string
			wantSQL  string
			wantNull bool
		}
		corpus := []tc{
			{"acme.line1", "acme.line1", false},
			{"acme.line-1", "acme.line-1", false}, // PG16+ ltree keeps hyphens; not folded to _
			{"acme@line/1", "acme_line_1", false},
			{"a.b/c", "a.b_c", false},
			{"a...b", "a.b", false},
			{"...", "", true},
			{".", "", true},
		}
		for _, c := range corpus {
			val, isNull := h.SQLToLtree(ctx, c.in)
			Expect(isNull).To(Equal(c.wantNull), "SQL null for %q", c.in)
			if !c.wantNull {
				Expect(val).To(Equal(c.wantSQL), "SQL value for %q", c.in)
			}
		}
	})

	It("writes a numeric row (value_text NULL) and absorbs an identical replay", func() {
		h := connected("flow")
		defer h.Close(ctx)
		msg := mkMsg(3.5, 1000, "_flow_v1", "acme.line1", "x", nil)
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		Expect(h.CountValueRows(ctx, "flow")).To(Equal(1))
		// identical replay -> absorbed, no error, still one row
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(3.5, 1000, "_flow_v1", "acme.line1", "x", nil)})).To(Succeed())
		Expect(h.CountValueRows(ctx, "flow")).To(Equal(1))
	})

	It("routes a string to value_text and a bool to value_num (the other column NULL)", func() {
		h := connected("vland")
		defer h.Close(ctx)
		// string -> value_text populated, value_num NULL
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg("hello", 1000, "_vland_v1", "acme.line1", "s", nil)})).To(Succeed())
		idS, ok := h.GetTopicID(ctx, "acme.line1", "vibration", "vland", "s")
		Expect(ok).To(BeTrue())
		num, text := h.ValueRow(ctx, "vland", idS)
		Expect(num).To(BeNil(), "a string must not populate value_num")
		Expect(text).NotTo(BeNil())
		Expect(*text).To(Equal("hello"))
		// bool -> value_num 1, value_text NULL
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(true, 1000, "_vland_v1", "acme.line1", "b", nil)})).To(Succeed())
		idB, ok := h.GetTopicID(ctx, "acme.line1", "vibration", "vland", "b")
		Expect(ok).To(BeTrue())
		numB, textB := h.ValueRow(ctx, "vland", idB)
		Expect(textB).To(BeNil(), "a bool must not populate value_text")
		Expect(numB).NotTo(BeNil())
		Expect(*numB).To(Equal(1.0))
	})

	It("writes a mixed numeric+text batch through the unnest fast path (interleaved NULL arrays)", func() {
		h := connected("mixed")
		defer h.Close(ctx)
		f := func(v float64) *float64 { return &v }
		s := func(v string) *string { return &v }
		logs := h.CaptureLogs()
		// One batch, four distinct tags alternating numeric/text, so the unnest value insert gets
		// value_num = {1.5, NULL, 3.5, NULL} and value_text = {NULL, "a", NULL, "b"} -- NULLs
		// interleaved at several positions, the pgx array-encoding case a single-type batch (all
		// numeric or all text) never reaches under the simple protocol.
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.5, 1000, "_mixed_v1", "acme.line1", "n1", nil),
			mkMsg("a", 1000, "_mixed_v1", "acme.line1", "s1", nil),
			mkMsg(3.5, 1000, "_mixed_v1", "acme.line1", "n2", nil),
			mkMsg("b", 1000, "_mixed_v1", "acme.line1", "s2", nil),
		})).To(Succeed())
		Expect(h.CountValueRows(ctx, "mixed")).To(Equal(4))
		Expect(logs()).NotTo(ContainSubstring("isolating good rows"), "a mixed-type batch must stay on the fast path")

		for _, tc := range []struct {
			tag  string
			num  *float64
			text *string
		}{
			{"n1", f(1.5), nil},
			{"s1", nil, s("a")},
			{"n2", f(3.5), nil},
			{"s2", nil, s("b")},
		} {
			id, ok := h.GetTopicID(ctx, "acme.line1", "vibration", "mixed", tc.tag)
			Expect(ok).To(BeTrue(), "topic %s", tc.tag)
			num, text := h.ValueRow(ctx, "mixed", id)
			if tc.num == nil {
				Expect(num).To(BeNil(), "%s value_num", tc.tag)
			} else {
				Expect(num).NotTo(BeNil(), "%s value_num", tc.tag)
				Expect(*num).To(Equal(*tc.num))
			}
			if tc.text == nil {
				Expect(text).To(BeNil(), "%s value_text", tc.tag)
			} else {
				Expect(text).NotTo(BeNil(), "%s value_text", tc.tag)
				Expect(*text).To(Equal(*tc.text))
			}
		}
	})

	It("keeps hyphen and underscore location variants as distinct topics", func() {
		h := connected("vcol")
		defer h.Close(ctx)
		// PG16+ ltree accepts hyphens, so '-' is preserved rather than folded to '_'. The topic
		// parser restricts write-path location chars to [a-zA-Z0-9._-], so line-1 and line_1 are
		// the two writable variants -- and they now resolve to TWO distinct topics, each its own series.
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 1000, "_vcol_v1", "acme.line-1", "t", nil)})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(2.0, 2000, "_vcol_v1", "acme.line_1", "t", nil)})).To(Succeed())

		id1, ok := h.GetTopicID(ctx, "acme.line-1", "vibration", "vcol", "t")
		Expect(ok).To(BeTrue())
		id2, ok := h.GetTopicID(ctx, "acme.line_1", "vibration", "vcol", "t")
		Expect(ok).To(BeTrue())
		Expect(id2).NotTo(Equal(id1), "line-1 and line_1 are distinct ltree paths -> distinct topics")

		// Two separate single-point series, not one merged series.
		Expect(h.CountValueRows(ctx, "vcol")).To(Equal(2))
		Expect(h.ValueWindow(ctx, "vcol", id1, 0, 4000)).To(Equal([]float64{1.0}))
		Expect(h.ValueWindow(ctx, "vcol", id2, 0, 4000)).To(Equal([]float64{2.0}))

		// Read side: get_topic_id accepts arbitrary strings, so a character outside [A-Za-z0-9_-]
		// still folds. A Grafana user typing "line@1" canonicalizes '@' -> '_', matching line_1.
		id3, ok := h.GetTopicID(ctx, "acme.line@1", "vibration", "vcol", "t")
		Expect(ok).To(BeTrue())
		Expect(id3).To(Equal(id2), "get_topic_id folds line@1's '@' to '_', matching line_1")
	})

	It("Go CanonicalLtreePath agrees with SQL to_ltree_path over a shared corpus", func() {
		h := connected("parity")
		defer h.Close(ctx)
		// The Go function is the dedup cache key; the SQL function is storage identity. They must
		// not drift, or dedup keys stop matching DB identity. Feed the same inputs to both.
		corpus := []string{
			"acme.line1", "acme.line-1", "acme.line_1", "acme@line/1", "a.b/c",
			"a...b", "ENTERPRISE.Site.Area", "x-y_z.1-2",
			"...", ".", "",
		}
		for _, in := range corpus {
			goVal := tsh.CanonicalLtreePath(in)
			sqlVal, isNull := h.SQLToLtree(ctx, in)
			if isNull {
				Expect(goVal).To(Equal(""), "SQL to_ltree_path(%q) is NULL; Go must be empty", in)
			} else {
				Expect(goVal).To(Equal(sqlVal), "Go and SQL canonicalization must agree for %q", in)
			}
		}
	})

	It("locks the documented read query (get_topic_id + time-window select)", func() {
		h := connected("readq")
		defer h.Close(ctx)
		// three numeric points for one tag at 1s, 2s, 3s
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_readq_v1", "acme.line1", "x", nil),
			mkMsg(2.0, 2000, "_readq_v1", "acme.line1", "x", nil),
			mkMsg(3.0, 3000, "_readq_v1", "acme.line1", "x", nil),
		})).To(Succeed())

		// get_topic_id resolves the tag -- the Grafana / ad-hoc entry point documented in the README.
		id, ok := h.GetTopicID(ctx, "acme.line1", "vibration", "readq", "x")
		Expect(ok).To(BeTrue())

		// a [1.5s, 3.5s) window returns only the in-range points, in ts order.
		Expect(h.ValueWindow(ctx, "readq", id, 1500, 3500)).To(Equal([]float64{2.0, 3.0}))

		// an unknown tag resolves to no topic_id.
		_, ok = h.GetTopicID(ctx, "acme.line1", "vibration", "readq", "nope")
		Expect(ok).To(BeFalse())
	})

	It("drops a different value at the same (topic_id, ts) as poison", func() {
		h := connected("conf")
		defer h.Close(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 2000, "_conf_v1", "l.a", "t", nil)})).To(Succeed())
		logs := h.CaptureLogs()
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(2.0, 2000, "_conf_v1", "l.a", "t", nil)})).To(Succeed())
		Expect(h.CountValueRows(ctx, "conf")).To(Equal(1)) // original kept, conflicting value dropped
		// The runbook signal: the drop is named in the error log, not just silently reflected in the row count.
		Expect(logs()).To(And(ContainSubstring("dropped poison row"), ContainSubstring(`tag="t"`)))
	})

	It("drops a datatype flip for the same tag as poison", func() {
		h := connected("flip")
		defer h.Close(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 3000, "_flip_v1", "l.a", "t", nil)})).To(Succeed())
		logs := h.CaptureLogs()
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg("now-text", 4000, "_flip_v1", "l.a", "t", nil)})).To(Succeed())
		Expect(h.CountValueRows(ctx, "flip")).To(Equal(1)) // numeric kept, flip dropped
		Expect(logs()).To(And(ContainSubstring("dropped poison row"), ContainSubstring(`tag="t"`)))
	})

	It("drops a poison flip row but lands the good co-batched rows (ACK)", func() {
		h := connected("iso")
		defer h.Close(ctx)
		// establish tag t as numeric
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 1000, "_iso_v1", "l.a", "t", nil)})).To(Succeed())
		Expect(h.CountValueRows(ctx, "iso")).To(Equal(1))
		// batch: a brand-new good tag y, plus a datatype flip on t (poison)
		good := mkMsg(5.0, 2000, "_iso_v1", "l.a", "y", nil)
		poison := mkMsg("now-text", 3000, "_iso_v1", "l.a", "t", nil)
		logs := h.CaptureLogs()
		Expect(h.WriteBatch(ctx, service.MessageBatch{good, poison})).To(Succeed()) // ACK: poison isolated
		// y landed; the flip on t was dropped -> two rows total, t still numeric
		Expect(h.CountValueRows(ctx, "iso")).To(Equal(2))
		// Only the poison row (t) is named in the log; the co-batched good row (y) is not dropped.
		Expect(logs()).To(And(ContainSubstring("dropped poison row"), ContainSubstring(`tag="t"`)))
		Expect(logs()).NotTo(ContainSubstring(`tag="y"`))
	})

	It("drops a poison same-ts conflict row within a batch, keeps the rest (ACK)", func() {
		h := connected("iso2")
		defer h.Close(ctx)
		good := mkMsg(9.0, 1000, "_iso2_v1", "l.a", "keep", nil)
		a := mkMsg(1.0, 2000, "_iso2_v1", "l.a", "x", nil)
		b := mkMsg(2.0, 2000, "_iso2_v1", "l.a", "x", nil) // same (topic,ts), different value -> poison
		logs := h.CaptureLogs()
		Expect(h.WriteBatch(ctx, service.MessageBatch{good, a, b})).To(Succeed())
		Expect(h.CountValueRows(ctx, "iso2")).To(Equal(2)) // keep + x@2000(=1.0) land; b dropped
		// Only the conflicting row (x) is named as poison; the surviving rows are not.
		Expect(logs()).To(And(ContainSubstring("dropped poison row"), ContainSubstring(`tag="x"`)))
		Expect(logs()).NotTo(ContainSubstring(`tag="keep"`))
	})

	It("absorbs a byte-identical duplicate within a batch on the fast path (no isolation)", func() {
		h := connected("dup")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		// Two byte-identical points in one batch. They are collapsed before the batched insert, so
		// this stays on the fast path (no 21000, no isolation) and lands exactly one row -- unlike a
		// same-(topic,ts) row with a DIFFERENT value, which is the poison case above.
		a := mkMsg(7.0, 1000, "_dup_v1", "l.a", "t", nil)
		b := mkMsg(7.0, 1000, "_dup_v1", "l.a", "t", nil)
		Expect(h.WriteBatch(ctx, service.MessageBatch{a, b})).To(Succeed())
		Expect(h.CountValueRows(ctx, "dup")).To(Equal(1))
		Expect(logs()).NotTo(ContainSubstring("isolating good rows"), "an identical in-batch duplicate must not fall to the isolated path")
	})

	It("Connect succeeds for the owner role (has INSERT)", func() {
		h := connected("probe") // connected() asserts Connect succeeded; the owner can write
		defer h.Close(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 1000, "_probe_v1", "l.a", "t", nil)})).To(Succeed())
	})

	It("Connect fails visibly for a role lacking INSERT (write probe)", func() {
		owner := connected("probe") // ensures umh.value_probe / umh.attribute_probe exist
		defer owner.Close(ctx)

		admin, err := pgx.Connect(ctx, sharedDSN)
		Expect(err).NotTo(HaveOccurred())
		defer admin.Close(ctx)
		for _, stmt := range []string{
			"DROP ROLE IF EXISTS probe_norights",
			"CREATE ROLE probe_norights LOGIN PASSWORD 'norights'",
			"GRANT CONNECT ON DATABASE umh TO probe_norights",
			"GRANT USAGE ON SCHEMA umh TO probe_norights", // can resolve the table name, still no INSERT
		} {
			_, execErr := admin.Exec(ctx, stmt)
			Expect(execErr).NotTo(HaveOccurred())
		}

		// MarkBootstrapped models the reconnect / already-provisioned path (bootstrapped already true
		// in-process), where the DDL is skipped and probeWritable is the front-line check -- so this
		// pins the exact "lacks INSERT" diagnostic. On a cold first connect a no-INSERT role that also
		// lacks DDL rights instead fails at the bootstrap step; either way Connect fails visibly.
		lim := tsh.NewHistorianTestHandle("postgres://probe_norights:norights@"+hostPort()+"/umh?sslmode=disable", "probe")
		lim.MarkBootstrapped()
		defer lim.Close(ctx)
		err = lim.Connect(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("INSERT"))
		Expect(err.Error()).To(ContainSubstring("umh.value_probe"))
	})

	It("NACKs and drops zero rows on a standing write fault (never a silent drop)", func() {
		// The core safety property: an error that is NOT poison (here a permission revoke, a class-42
		// standing fault) must hold the batch for retry -- return an error so benthos NACKs -- and
		// drop no good rows, unlike the poison path which drops the offending row and ACKs.
		owner := connected("standing")
		defer owner.Close(ctx)
		Expect(owner.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_standing_v1", "l.a", "t", nil),
		})).To(Succeed())
		Expect(owner.CountValueRows(ctx, "standing")).To(Equal(1))

		admin, err := pgx.Connect(ctx, sharedDSN)
		Expect(err).NotTo(HaveOccurred())
		defer admin.Close(ctx)
		for _, stmt := range []string{
			"DROP ROLE IF EXISTS standing_rw",
			"CREATE ROLE standing_rw LOGIN PASSWORD 'rw'",
			"GRANT CONNECT ON DATABASE umh TO standing_rw",
			"GRANT USAGE ON SCHEMA umh TO standing_rw",
			"GRANT SELECT ON ALL TABLES IN SCHEMA umh TO standing_rw", // resolve the topic via lookup
			"GRANT INSERT, UPDATE ON umh.value_standing, umh.attribute_standing TO standing_rw",
		} {
			_, execErr := admin.Exec(ctx, stmt)
			Expect(execErr).NotTo(HaveOccurred())
		}

		rw := tsh.NewHistorianTestHandle("postgres://standing_rw:rw@"+hostPort()+"/umh?sslmode=disable", "standing")
		rw.MarkBootstrapped() // no CREATE; skip DDL, it can still resolve + INSERT
		Expect(rw.Connect(ctx)).To(Succeed())
		defer rw.Close(ctx)
		// Warm the process topic cache with a successful write, so the failing write below fails on
		// the value INSERT (the standing fault) rather than on an earlier resolve.
		Expect(rw.WriteBatch(ctx, service.MessageBatch{
			mkMsg(2.0, 2000, "_standing_v1", "l.a", "t", nil),
		})).To(Succeed())
		Expect(owner.CountValueRows(ctx, "standing")).To(Equal(2))

		// Standing fault: the role can no longer INSERT. The next (good) row cannot land.
		_, err = admin.Exec(ctx, "REVOKE INSERT ON umh.value_standing FROM standing_rw")
		Expect(err).NotTo(HaveOccurred())

		logs := rw.CaptureLogs()
		err = rw.WriteBatch(ctx, service.MessageBatch{
			mkMsg(3.0, 3000, "_standing_v1", "l.a", "t", nil),
		})
		Expect(err).To(HaveOccurred(), "a standing (non-poison) fault must NACK, not ACK")
		Expect(logs()).To(ContainSubstring("standing fault"), "a permission fault is classified standing (loud), not poison")
		Expect(logs()).NotTo(ContainSubstring("dropped poison row"), "a standing fault must never drop a row")
		Expect(owner.CountValueRows(ctx, "standing")).To(Equal(2), "the held row must not be dropped; the table is unchanged")
	})

	It("dedups unchanged metadata across batches", func() {
		h := connected("meta")
		defer h.Close(ctx)
		m1 := mkMsg(1.0, 5000, "_meta_v1", "l.a", "t", map[string]string{"serialNumber": "abc"})
		m2 := mkMsg(2.0, 6000, "_meta_v1", "l.a", "t", map[string]string{"serialNumber": "abc"})
		Expect(h.WriteBatch(ctx, service.MessageBatch{m1})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{m2})).To(Succeed())
		// same key set -> only the first emits an attribute row
		Expect(h.CountAttributeRows(ctx, "meta")).To(Equal(1))
		// stored as a JSON object, readable via attribute->>'key' (not an array-of-pairs)
		v, ok := h.AttributeValue(ctx, "meta", "serialNumber")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("abc"))
	})

	It("stores the data contract version, which no column holds, in the attribute row", func() {
		h := connected("ver")
		defer h.Close(ctx)
		msg := mkMsg(1.0, 1000, "_ver_v3", "l.a", "t", map[string]string{
			"data_contract_name":    "_ver",
			"data_contract_version": "3",
			"serialNumber":          "abc",
		})
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		v, ok := h.AttributeValue(ctx, "ver", "data_contract_version")
		Expect(ok).To(BeTrue(), "all versions share one umh.tag row, so metadata is the only place the version survives")
		Expect(v).To(Equal("3"))
		_, ok = h.AttributeValue(ctx, "ver", "data_contract_name")
		Expect(ok).To(BeFalse(), "the name is already a column on umh.tag")
	})

	It("writes no version key for a contract that never carried one", func() {
		h := connected("nover")
		defer h.Close(ctx)
		msg := mkMsg(1.0, 1000, "_nover", "l.a", "t", map[string]string{"serialNumber": "abc"})
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		_, ok := h.AttributeValue(ctx, "nover", "data_contract_version")
		Expect(ok).To(BeFalse(), "the uns output sets the version only on the validated path, and an absent key is not stored blank")
	})

	It("emits a fresh attribute row when the contract version changes", func() {
		h := connected("verbump")
		defer h.Close(ctx)
		v1 := mkMsg(1.0, 1000, "_verbump_v1", "l.a", "t", map[string]string{"data_contract_version": "1"})
		v2 := mkMsg(2.0, 2000, "_verbump_v2", "l.a", "t", map[string]string{"data_contract_version": "2"})
		Expect(h.WriteBatch(ctx, service.MessageBatch{v1})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{v2})).To(Succeed())
		Expect(h.CountAttributeRows(ctx, "verbump")).To(Equal(2), "the version is part of the dedup fingerprint, so a bump is recorded rather than swallowed")
	})

	It("omits blacklisted metadata keys from the stored attribute row", func() {
		h := connected("excl")
		defer h.Close(ctx)
		h.SetMetaExclude([]string{"secret_token", "opcua_*"})
		msg := mkMsg(1.0, 1000, "_excl_v1", "l.a", "t", map[string]string{
			"serialNumber": "keep-me",
			"secret_token": "drop-me",
			"opcua_vendor": "drop-me-too",
		})
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		Expect(h.CountAttributeRows(ctx, "excl")).To(Equal(1))
		// non-blacklisted key survives
		v, ok := h.AttributeValue(ctx, "excl", "serialNumber")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("keep-me"))
		// exact-match and prefix-match blacklist entries are both dropped
		_, ok = h.AttributeValue(ctx, "excl", "secret_token")
		Expect(ok).To(BeFalse())
		_, ok = h.AttributeValue(ctx, "excl", "opcua_vendor")
		Expect(ok).To(BeFalse())
	})

	It("intra-batch: same tag+ts with different metadata isolates the conflicting row (ACK)", func() {
		h := connected("intra")
		defer h.Close(ctx)
		a := mkMsg(1.0, 7000, "_intra_v1", "l.a", "t", map[string]string{"serialNumber": "A"})
		b := mkMsg(1.0, 7000, "_intra_v1", "l.a", "t", map[string]string{"serialNumber": "B"})
		Expect(h.WriteBatch(ctx, service.MessageBatch{a, b})).To(Succeed())
		Expect(h.CountValueRows(ctx, "intra")).To(Equal(1)) // one row lands, the conflicting twin drops
	})

	It("re-emits metadata after a rolled-back batch (dedup view discarded on failure)", func() {
		h := connected("reemit")
		defer h.Close(ctx)
		// 1. baseline value + metadata B at ts=8000
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 8000, "_reemit_v1", "l.a", "t", map[string]string{"serialNumber": "A"})})).To(Succeed())
		Expect(h.CountAttributeRows(ctx, "reemit")).To(Equal(1))
		// 2. a conflicting value at the SAME ts carrying NEW metadata B -> the value write is
		//    poison. The batch is isolated (ACK): the conflicting value is dropped and, because
		//    the isolated path never promotes the dedup view, B is not committed to the cache.
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(2.0, 8000, "_reemit_v1", "l.a", "t", map[string]string{"serialNumber": "B"})})).To(Succeed())
		Expect(h.CountAttributeRows(ctx, "reemit")).To(Equal(1))
		// 3. a valid write for the same tag with metadata B at a fresh ts must RE-EMIT B,
		//    because the prior view was discarded on rollback (not silently suppressed).
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(3.0, 9000, "_reemit_v1", "l.a", "t", map[string]string{"serialNumber": "B"})})).To(Succeed())
		Expect(h.CountAttributeRows(ctx, "reemit")).To(Equal(2))
	})

	It("nacks and errors when data arrives but nothing matches the contract", func() {
		h := connected("drops")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		bad := mkMsg(1.0, 1000, "_other_v1", "acme.line1", "t", nil)
		err := h.WriteBatch(ctx, service.MessageBatch{bad})
		Expect(err).To(HaveOccurred(), "a contract mismatch nacks the batch, so throughput cannot count it as sent")
		Expect(err.Error()).To(ContainSubstring("batch refused"))
		Expect(h.CountValueRows(ctx, "drops")).To(Equal(0))
		Expect(logs()).To(ContainSubstring("reason=contract_mismatch"), "the error must name the drop reason")
		Expect(logs()).NotTo(ContainSubstring("(reason=contract_mismatch, example"), "a refused batch reports through the throttled mismatch error only; the drop tally is discarded with it")
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: no message carries data contract _drops"), "a wrong contract must error, so umh-core degrades the bridge")
		Expect(logs()).To(ContainSubstring("_other_v1"), "the error must name the contract that actually arrived")
		Expect(logs()).To(ContainSubstring(`^umh\.v1(?:\.[^._][^.]*)+\._drops(_v\d+)?\..+$`), "the error must carry the regex to paste")
	})

	It("confirms the first stored message, then nacks a later other-contract batch", func() {
		h := connected("stored")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		good := mkMsg(1.0, 1000, "_stored_v1", "acme.line1", "t", nil)
		Expect(h.WriteBatch(ctx, service.MessageBatch{good})).To(Succeed())
		Expect(h.CountValueRows(ctx, "stored")).To(Equal(1))
		Expect(logs()).To(ContainSubstring("level=info msg=TimescaleDB historian: first message stored for data contract _stored"), "a successful first store must be confirmed at info")
		other := mkMsg(2.0, 2000, "_other_v1", "acme.line1", "t", nil)
		Expect(h.WriteBatch(ctx, service.MessageBatch{other})).To(HaveOccurred())
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: subscription is over-broad"), "once rows are landing, a foreign contract means the subscription is too wide")
	})

	It("nacks a mixed batch whole, so the matching message is not written either", func() {
		h := connected("overbroad")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		batch := service.MessageBatch{
			mkMsg(1.0, 1000, "_overbroad_v1", "acme.line1", "t", nil),
			mkMsg(2.0, 2000, "_other_v1", "acme.line1", "t", nil),
		}
		Expect(h.WriteBatch(ctx, batch)).To(HaveOccurred())
		Expect(h.CountValueRows(ctx, "overbroad")).To(Equal(0), "nacking the batch costs the matching row: it is refused with the rest and only returns if the offset is replayed")
		Expect(logs()).To(ContainSubstring("reason=contract_mismatch"))
		Expect(logs()).NotTo(ContainSubstring("first message stored for data contract _overbroad"), "nothing was written, so there is no first store to confirm")
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: subscription is over-broad"), "a mixed batch is the case that made throughput lie, so it must error")
		Expect(logs()).To(ContainSubstring("1 of 2 message(s)"))
	})

	It("blames the subscription when the matching message is dropped for its payload, not its contract", func() {
		h := connected("matchdrop")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		batch := service.MessageBatch{
			mkMsg(nil, 1000, "_matchdrop_v1", "acme.line1", "t", nil),
			mkMsg(2.0, 2000, "_other_v1", "acme.line1", "t", nil),
		}
		Expect(h.WriteBatch(ctx, batch)).To(HaveOccurred())
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: subscription is over-broad"), "the contract did arrive, it just carried no value; sending the operator to data_contract_name would be the wrong fix")
		Expect(logs()).NotTo(ContainSubstring("no message carries"))
	})

	It("nacks and reports the first batch without waiting out any startup hold", func() {
		h := connected("grace")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		bad := mkMsg(1.0, 1000, "_other_v1", "acme.line1", "t", nil)
		Expect(h.WriteBatch(ctx, service.MessageBatch{bad})).To(HaveOccurred(), "the very first batch must nack, so a wrong subscription never reports throughput")
		Expect(h.CountValueRows(ctx, "grace")).To(Equal(0))
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: no message carries data contract _grace"), "the actionable reason must arrive with the nack, not 30s later")
		Expect(logs()).To(ContainSubstring(`^umh\.v1(?:\.[^._][^.]*)+\._grace(_v\d+)?\..+$`))
	})

	It("stores an unversioned contract with no configuration at all", func() {
		h := connected("unver")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		msg := mkMsg(1.0, 1000, "_unver", "acme.line1", "t", nil)
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		Expect(h.CountValueRows(ctx, "unver")).To(Equal(1), "a default _historian bridge must store without any opt-in")
		Expect(logs()).NotTo(ContainSubstring("reason=contract_"))
	})

	It("stores an unversioned contract carrying the bypass flag the uns output stamps on all of them", func() {
		h := connected("unverbypass")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		msg := mkMsg(1.0, 1000, "_unverbypass", "acme.line1", "t", map[string]string{"data_contract_bypassed": "true"})
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		Expect(h.CountValueRows(ctx, "unverbypass")).To(Equal(1), "honoring the flag here would drop every _historian message")
		Expect(logs()).NotTo(ContainSubstring("reason=contract_bypassed"))
	})

	It("refuses a versioned contract whose schema was bypassed, even with datatype changes allowed", func() {
		h := connected("bypassed")
		defer h.Close(ctx)
		h.SetAllowDatatypeChanges(true)
		logs := h.CaptureLogs()
		msg := mkMsg(1.0, 1000, "_bypassed_v1", "acme.line1", "t", map[string]string{"data_contract_bypassed": "true"})
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		Expect(h.CountValueRows(ctx, "bypassed")).To(Equal(0), "a versioned contract with an unapplied schema is never stored")
		Expect(logs()).To(ContainSubstring("reason=contract_bypassed"))
	})

	It("refuses a relational payload carrying extra fields alongside value and timestamp_ms", func() {
		h := connected("relational")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		msg := service.NewMessage(nil)
		msg.SetStructured(map[string]any{"value": 1.0, "timestamp_ms": float64(1000), "orderId": "WO-42"})
		msg.MetaSet("umh_topic", "umh.v1.acme.line1._relational_v1.vibration.t")
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		Expect(h.CountValueRows(ctx, "relational")).To(Equal(0), "relational data must not land in a timeseries table")
		Expect(logs()).To(ContainSubstring("reason=not_timeseries"))
	})

	It("reports a body that is not JSON, naming the topic that carried it", func() {
		h := connected("notjson")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		msg := service.NewMessage([]byte("<xml>not json</xml>"))
		msg.MetaSet("umh_topic", "umh.v1.acme.line1._notjson_v1.vibration.t")
		Expect(h.WriteBatch(ctx, service.MessageBatch{msg})).To(Succeed())
		Expect(h.CountValueRows(ctx, "notjson")).To(Equal(0))
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: dropped 1 of 1 message(s) (reason=not_structured"), "an unparseable body must name itself, not fail silently")
		Expect(logs()).To(ContainSubstring(`example umh_topic="umh.v1.acme.line1._notjson_v1.vibration.t"`), "the topic is the only handle an operator has on a body that cannot be parsed")
	})

	It("reports a JSON body that is not an object", func() {
		h := connected("notobj")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		arr := service.NewMessage(nil)
		arr.SetStructured([]any{map[string]any{"value": 1.0, "timestamp_ms": float64(1000)}})
		arr.MetaSet("umh_topic", "umh.v1.acme.line1._notobj_v1.vibration.t")
		scalar := service.NewMessage(nil)
		scalar.SetStructured(42.0)
		scalar.MetaSet("umh_topic", "umh.v1.acme.line1._notobj_v1.vibration.s")
		Expect(h.WriteBatch(ctx, service.MessageBatch{arr, scalar})).To(Succeed())
		Expect(h.CountValueRows(ctx, "notobj")).To(Equal(0), "an array of points and a bare scalar are both outside the {value, timestamp_ms} contract")
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: dropped 2 of 2 message(s) (reason=not_object"))
	})

	It("nacks every mismatching batch while the error log is throttled", func() {
		h := connected("throttle")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		bad := func() service.MessageBatch {
			return service.MessageBatch{mkMsg(1.0, 1000, "_other_v1", "acme.line1", "t", nil)}
		}
		for i := 0; i < 3; i++ {
			Expect(h.WriteBatch(ctx, bad())).To(HaveOccurred(), "the throttle governs the log only; a refused batch must never be acked")
		}
		Expect(strings.Count(logs(), "level=error msg=TimescaleDB historian: no message carries")).To(Equal(1), "three batches inside one interval report once")
		Expect(h.CountValueRows(ctx, "throttle")).To(Equal(0))
	})

	It("errors with the reason and the fix when a whole batch is dropped for a real fault", func() {
		h := connected("drops")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		bad := mkMsg(nil, 1000, "_drops_v1", "acme.line1", "t", nil) // matching contract, missing value
		Expect(h.WriteBatch(ctx, service.MessageBatch{bad})).To(Succeed())
		Expect(h.CountValueRows(ctx, "drops")).To(Equal(0))
		Expect(logs()).To(ContainSubstring("level=error msg=TimescaleDB historian: dropped 1 of 1 message(s) (reason=missing_value"), "a malformed message must log at error level so umh-core degrades the bridge")
		Expect(logs()).To(ContainSubstring("payload has no value field"), "the drop log must name what was wrong with the message")
		Expect(logs()).NotTo(ContainSubstring("level=warning"), "the per-message error already carries the reason and the fix; a reasonless batch summary on top of it is noise")
	})

	It("reports each reason once per batch with its share, and still writes the good rows", func() {
		h := connected("tally")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		batch := service.MessageBatch{
			mkMsg(1.0, 1000, "_tally_v1", "acme.line1", "good", nil),
			mkMsg(nil, 2000, "_tally_v1", "acme.line1", "noval1", nil),
			mkMsg(nil, 3000, "_tally_v1", "acme.line1", "noval2", nil),
			mkMsgNoTimestamp(4.0, "_tally_v1", "acme.line1", "nots", nil),
		}
		Expect(h.WriteBatch(ctx, batch)).To(Succeed())
		Expect(h.CountValueRows(ctx, "tally")).To(Equal(1), "a partly-dropped batch still writes what it can")
		Expect(logs()).To(ContainSubstring("dropped 2 of 4 message(s) (reason=missing_value"), "two messages, one line, with the share of the batch")
		Expect(logs()).To(ContainSubstring("dropped 1 of 4 message(s) (reason=missing_timestamp"), "a second reason gets its own line, not a merged one")
		Expect(strings.Count(logs(), "level=error")).To(Equal(2), "one line per reason, never one per message")
		Expect(logs()).To(ContainSubstring("tag processor sets timestamp_ms"), "each line keeps the fix for its own reason")
	})

	It("truncates an over-long value_text and warns exactly once", func() {
		h := connected("trunc")
		defer h.Close(ctx)
		logs := h.CaptureLogs()
		long := strings.Repeat("x", 9000)
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(long, 1000, "_trunc_v1", "acme.line1", "t", nil)})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(long, 2000, "_trunc_v1", "acme.line1", "t2", nil)})).To(Succeed())
		Expect(h.CountValueRows(ctx, "trunc")).To(Equal(2))
		id, ok := h.GetTopicID(ctx, "acme.line1", "vibration", "trunc", "t")
		Expect(ok).To(BeTrue())
		_, text := h.ValueRow(ctx, "trunc", id)
		Expect(text).NotTo(BeNil())
		Expect([]rune(*text)).To(HaveLen(8192), "clipped to maxTextRunes")
		Expect(strings.Count(logs(), "was truncated")).To(Equal(1), "truncation warns once per process, not per message")
	})

	It("warns about a stored high-churn metadata key (allowlist mode)", func() {
		h := connected("churn")
		defer h.Close(ctx)
		h.SetMetadataAllowlist([]string{"opcua_source_timestamp"}) // known high-churn key, explicitly allowed
		logs := h.CaptureLogs()
		Expect(h.WriteBatch(ctx, service.MessageBatch{
			mkMsg(1.0, 1000, "_churn_v1", "acme.line1", "t", map[string]string{"opcua_source_timestamp": "123"}),
		})).To(Succeed())
		Expect(logs()).To(ContainSubstring("high-churn metadata key"))
	})
})

func hostPort() string {
	ep, err := pgContainer.Endpoint(context.Background(), "")
	Expect(err).NotTo(HaveOccurred())
	return ep
}
