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

package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("JSON Schema Generator", func() {
	Context("when validating format flag", func() {
		It("should accept 'benthos' format", func() {
			err := validateFormat("benthos")
			Expect(err).ToNot(HaveOccurred())
		})

		It("should accept 'json-schema' format", func() {
			err := validateFormat("json-schema")
			Expect(err).ToNot(HaveOccurred())
		})

		It("should reject invalid format", func() {
			err := validateFormat("invalid-format")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid format"))
		})

		It("should reject empty format", func() {
			err := validateFormat("")
			Expect(err).To(HaveOccurred())
		})
	})
})
