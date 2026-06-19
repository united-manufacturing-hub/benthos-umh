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

package timescaledb_historian_plugin_test

import (
	"context"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	tsh "github.com/united-manufacturing-hub/benthos-umh/timescaledb_historian_plugin"
)

var (
	sharedDSN   string
	pgContainer *postgres.PostgresContainer
)

func mkMsg(value any, tsMs float64, contract, loc, tag string, extraMeta map[string]string) *service.Message {
	m := service.NewMessage(nil)
	m.SetStructured(map[string]any{"value": value, "timestamp_ms": tsMs})
	m.MetaSet("data_contract", contract)
	m.MetaSet("location_path", loc)
	m.MetaSet("tag_name", tag)
	m.MetaSet("virtual_path", "vibration")
	for k, v := range extraMeta {
		m.MetaSet(k, v)
	}
	return m
}

var _ = Describe("TimescaleDB integration", Ordered, Label("postgres"), func() {
	var ctx context.Context

	BeforeAll(func() {
		if os.Getenv("TEST_TIMESCALEDB_HISTORIAN") == "" {
			Skip("set TEST_TIMESCALEDB_HISTORIAN=true to run TimescaleDB integration tests")
		}
		ctx = context.Background()
		c, err := postgres.Run(ctx, "timescale/timescaledb:latest-pg16",
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

	It("fails Connect on wrong credentials, then retries cleanly", func() {
		h := tsh.NewHistorianTestHandle("postgres://umh_owner:wrong@"+hostPort()+"/umh?sslmode=disable", "pump")
		Expect(h.Connect(ctx)).NotTo(Succeed())
		h.SetDSN(sharedDSN)
		Expect(h.Connect(ctx)).To(Succeed())
		_ = h.Close(ctx)
	})

	It("verifies to_ltree_path port + Go empty-boundary agreement", func() {
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
			Expect(tsh.LocationNormalizesToEmpty(c.in)).To(Equal(c.wantNull), "Go empty-check for %q", c.in)
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

	It("intra-batch: same tag+ts with different metadata RAISEs (real conflict)", func() {
		h := connected("intra")
		defer h.Close(ctx)
		a := mkMsg(1.0, 7000, "_intra_v1", "l.a", "t", map[string]string{"serialNumber": "A"})
		b := mkMsg(1.0, 7000, "_intra_v1", "l.a", "t", map[string]string{"serialNumber": "B"})
		Expect(h.WriteBatch(ctx, service.MessageBatch{a, b})).NotTo(Succeed())
	})
})

func hostPort() string {
	ep, err := pgContainer.Endpoint(context.Background(), "")
	Expect(err).NotTo(HaveOccurred())
	return ep
}
