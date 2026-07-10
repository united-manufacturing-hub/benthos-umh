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
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("classify", func() {
	pg := func(code string) error { return &pgconn.PgError{Code: code} }

	DescribeTable("maps a write error to a disposition",
		func(err error, want disposition) {
			Expect(classify(err)).To(Equal(want))
		},
		// payload-caused -> drop
		Entry("plugin RAISE guards (P0001)", pg("P0001"), dispDropPoison),
		Entry("check constraint (23514)", pg("23514"), dispDropPoison),
		Entry("unique violation (23505)", pg("23505"), dispDropPoison),
		Entry("numeric out of range (22003)", pg("22003"), dispDropPoison),
		Entry("invalid text representation (22P02)", pg("22P02"), dispDropPoison),
		// transient -> retry quiet
		Entry("connection failure (08006)", pg("08006"), dispRetryTransient),
		Entry("serialization failure (40001)", pg("40001"), dispRetryTransient),
		Entry("deadlock (40P01)", pg("40P01"), dispRetryTransient),
		Entry("lock not available (55P03)", pg("55P03"), dispRetryTransient),
		Entry("admin shutdown (57P01)", pg("57P01"), dispRetryTransient),
		// standing fault -> retry loud, never drop
		Entry("disk full (53100)", pg("53100"), dispRetryStanding),
		Entry("too many connections (53300)", pg("53300"), dispRetryStanding),
		Entry("insufficient privilege (42501)", pg("42501"), dispRetryStanding),
		Entry("undefined table (42P01)", pg("42P01"), dispRetryStanding),
		Entry("invalid schema name (3F000)", pg("3F000"), dispRetryStanding),
		Entry("unrecognized code defaults to standing", pg("XX000"), dispRetryStanding),
		// no SQLSTATE -> transient (network / ctx / pool)
		Entry("context deadline", context.DeadlineExceeded, dispRetryTransient),
		Entry("io.EOF", io.EOF, dispRetryTransient),
		Entry("wrapped pg error still unwraps", fmt.Errorf("value write failed: %w", pg("P0001")), dispDropPoison),
	)

	It("extracts the SQLSTATE, or none", func() {
		Expect(pgSQLState(pg("P0001"))).To(Equal("P0001"))
		Expect(pgSQLState(fmt.Errorf("wrap: %w", pg("23514")))).To(Equal("23514"))
		Expect(pgSQLState(errors.New("plain"))).To(Equal("none"))
	})
})
