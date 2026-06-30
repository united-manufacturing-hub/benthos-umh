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
	"sync"
	"time"

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

	It("bootstraps idempotently (Connect twice)", func() {
		h := connected("pump")
		defer h.Close(ctx)
		Expect(h.Connect(ctx)).To(Succeed())
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

	It("batched writes scale with workers and beat per-message (no batching)", func() {
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

	It("RAISEs on a different value at the same (topic_id, ts)", func() {
		h := connected("conf")
		defer h.Close(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 2000, "_conf_v1", "l.a", "t", nil)})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(2.0, 2000, "_conf_v1", "l.a", "t", nil)})).NotTo(Succeed())
	})

	It("RAISEs on a datatype flip for the same tag", func() {
		h := connected("flip")
		defer h.Close(ctx)
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 3000, "_flip_v1", "l.a", "t", nil)})).To(Succeed())
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg("now-text", 4000, "_flip_v1", "l.a", "t", nil)})).NotTo(Succeed())
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

	It("intra-batch: same tag+ts with different metadata RAISEs (real conflict)", func() {
		h := connected("intra")
		defer h.Close(ctx)
		a := mkMsg(1.0, 7000, "_intra_v1", "l.a", "t", map[string]string{"serialNumber": "A"})
		b := mkMsg(1.0, 7000, "_intra_v1", "l.a", "t", map[string]string{"serialNumber": "B"})
		Expect(h.WriteBatch(ctx, service.MessageBatch{a, b})).NotTo(Succeed())
	})

	It("re-emits metadata after a rolled-back batch (dedup view discarded on failure)", func() {
		h := connected("reemit")
		defer h.Close(ctx)
		// 1. baseline value + metadata B at ts=8000
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(1.0, 8000, "_reemit_v1", "l.a", "t", map[string]string{"serialNumber": "A"})})).To(Succeed())
		Expect(h.CountAttributeRows(ctx, "reemit")).To(Equal(1))
		// 2. a conflicting value at the SAME ts carrying NEW metadata B -> the value write
		//    RAISEs -> the whole batch nacks and rolls back, so B is never committed to the
		//    dedup cache (and never written).
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(2.0, 8000, "_reemit_v1", "l.a", "t", map[string]string{"serialNumber": "B"})})).NotTo(Succeed())
		Expect(h.CountAttributeRows(ctx, "reemit")).To(Equal(1))
		// 3. a valid write for the same tag with metadata B at a fresh ts must RE-EMIT B,
		//    because the prior view was discarded on rollback (not silently suppressed).
		Expect(h.WriteBatch(ctx, service.MessageBatch{mkMsg(3.0, 9000, "_reemit_v1", "l.a", "t", map[string]string{"serialNumber": "B"})})).To(Succeed())
		Expect(h.CountAttributeRows(ctx, "reemit")).To(Equal(2))
	})
})

func hostPort() string {
	ep, err := pgContainer.Endpoint(context.Background(), "")
	Expect(err).NotTo(HaveOccurred())
	return ep
}
