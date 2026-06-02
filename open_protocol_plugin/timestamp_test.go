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

package open_protocol_plugin_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

var _ = Describe("ParseControllerTime", func() {
	It("parses a well-formed timestamp in UTC", func() {
		t, err := op.ParseControllerTime("2026-06-02:14:30:15", time.UTC)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.UTC()).To(Equal(time.Date(2026, 6, 2, 14, 30, 15, 0, time.UTC)))
	})

	It("interprets the wall-clock in the supplied zone", func() {
		berlin, _ := time.LoadLocation("Europe/Berlin")
		t, err := op.ParseControllerTime("2026-06-02:14:30:15", berlin)
		Expect(err).NotTo(HaveOccurred())
		// 14:30 CEST (summer, +02:00) == 12:30 UTC
		Expect(t.UTC()).To(Equal(time.Date(2026, 6, 2, 12, 30, 15, 0, time.UTC)))
	})

	It("returns an error on a malformed timestamp", func() {
		_, err := op.ParseControllerTime("not-a-timestamp", time.UTC)
		Expect(err).To(HaveOccurred())
	})

	It("returns an error on the wrong separator/shape", func() {
		_, err := op.ParseControllerTime("2026-06-02 14:30:15", time.UTC)
		Expect(err).To(HaveOccurred())
	})
})
