//go:build !connect_patched

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

package uns_plugin

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Runs in the unpatched lane (make test-uns), unlike the specs labeled uns_beta.
var _ = Describe("uns_beta in an unpatched build", func() {
	It("fails at construction instead of on every Connect", func() {
		parsed, err := unsBetaConfigSpec().ParseYAML("consumer_group: \"g\"\numh_topics: [\".*\"]\n", nil)
		Expect(err).NotTo(HaveOccurred())

		in, err := newUnsBeta(parsed, nil)
		Expect(err).To(MatchError(errUnsBetaUnavailable))
		Expect(in).To(BeNil())
	})
})
