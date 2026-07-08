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
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This benchmark compares three ways the fast path can write a batch of value rows, all against a
// real TimescaleDB (PG18) testcontainer and under the same QueryExecModeExec the plugin uses in
// production (so pgx.Batch pipelining is measured under the mode that actually ships):
//
//   - perrow:   one tx.Exec per row (the pre-batching implementation)
//   - pgxbatch: the same per-row SQL queued into a pgx.Batch and pipelined in one SendBatch
//   - unnest:   one multi-row INSERT ... SELECT unnest(...) (the current implementation)
//
// Run: TEST_HISTORIAN=true go test ./historian_plugin/ -run '^$' -bench BenchmarkValueInserts -benchmem
//
// Caveat: each (strategy, size) writes into its own contract table so it grows from empty, but the
// table still grows across a sub-benchmark's b.N iterations, so ns/op is an average over a slowly
// growing hypertable. That bias is identical in shape across strategies, so the comparison holds.

const benchBaseMs = 1_600_000_000_000 // fixed epoch base so ts strings are valid timestamptz

var (
	benchOnce sync.Once
	benchDSNv string
	benchTerm func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	if benchTerm != nil {
		benchTerm()
	}
	os.Exit(code)
}

func benchDSN(tb testing.TB) string {
	tb.Helper()
	benchOnce.Do(func() {
		ctx := context.Background()
		c, err := postgres.Run(ctx, "timescale/timescaledb:latest-pg18",
			postgres.WithDatabase("umh"),
			postgres.WithUsername("umh_owner"),
			postgres.WithPassword("secret"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(90*time.Second)),
		)
		if err != nil {
			panic(err)
		}
		dsn, err := c.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			panic(err)
		}
		benchDSNv = dsn
		benchTerm = func() { _ = c.Terminate(context.Background()) }
	})
	return benchDSNv
}

func benchPool(tb testing.TB, dsn string) *pgxpool.Pool {
	tb.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		tb.Fatal(err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec // match production Connect
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatal(err)
	}
	return pool
}

// benchSetup bootstraps a contract's tables and resolves `topics` topic_ids to write against.
func benchSetup(tb testing.TB, ctx context.Context, pool *pgxpool.Pool, contract string, topics int) []int64 {
	tb.Helper()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, bootstrapSQL(contract, 168*time.Hour, 0, false)); err != nil {
		tb.Fatal(err)
	}
	ids := make([]int64, topics)
	for k := 0; k < topics; k++ {
		loc := fmt.Sprintf("bench.line%d", k)
		if err := conn.QueryRow(ctx, topicResolveSQL, loc, "_"+contract, "vp", fmt.Sprintf("tag%d", k), "numeric").Scan(&ids[k]); err != nil {
			tb.Fatal(err)
		}
	}
	return ids
}

func benchWrite(ctx context.Context, pool *pgxpool.Pool, strat, vq, vbq string, ids []int64, tss []string, nums []*float64, texts []*string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	switch strat {
	case "perrow":
		for j := range ids {
			if _, err := tx.Exec(ctx, vq, ids[j], tss[j], nums[j], texts[j]); err != nil {
				return err
			}
		}
	case "pgxbatch":
		batch := &pgx.Batch{}
		for j := range ids {
			batch.Queue(vq, ids[j], tss[j], nums[j], texts[j])
		}
		br := tx.SendBatch(ctx, batch)
		for range ids {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return err
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	case "unnest":
		if _, err := tx.Exec(ctx, vbq, ids, tss, nums, texts); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func BenchmarkValueInserts(b *testing.B) {
	if os.Getenv("TEST_HISTORIAN") == "" {
		b.Skip("set TEST_HISTORIAN=true to run TimescaleDB benchmarks")
	}
	ctx := context.Background()
	pool := benchPool(b, benchDSN(b))
	defer pool.Close()

	const topics = 50
	for _, n := range []int{100, 1000, 5000} {
		for _, strat := range []string{"perrow", "pgxbatch", "unnest"} {
			contract := fmt.Sprintf("bench%s%d", strat, n)
			ids := benchSetup(b, ctx, pool, contract, topics)
			vq := valueQueryFor(contract)
			vbq := valueBatchQueryFor(contract)
			b.Run(fmt.Sprintf("%s/n=%d", strat, n), func(b *testing.B) {
				var counter int64
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					vIDs := make([]int64, n)
					vTS := make([]string, n)
					vNum := make([]*float64, n)
					vText := make([]*string, n)
					for j := 0; j < n; j++ {
						counter++
						v := float64(j)
						vIDs[j] = ids[j%topics]
						vTS[j] = time.UnixMilli(benchBaseMs + counter*1000).UTC().Format(time.RFC3339Nano)
						vNum[j] = &v
						vText[j] = nil
					}
					b.StartTimer()
					if err := benchWrite(ctx, pool, strat, vq, vbq, vIDs, vTS, vNum, vText); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(n), "rows/op")
			})
		}
	}
}
