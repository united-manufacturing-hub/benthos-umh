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

package modbus_plugin

import (
	"hash/maphash"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("tagIDWithSlave", func() {
	var seed maphash.Seed

	BeforeEach(func() {
		seed = maphash.MakeSeed()
	})

	base := func() ModbusDataItemWithAddress {
		return ModbusDataItemWithAddress{Name: "reg", Register: "holding", Address: 23, Type: "INT16"}
	}

	It("should treat differently ordered lists as the same tag", func() {
		a, b := base(), base()
		a.SlaveIDs = []byte{3, 5}
		b.SlaveIDs = []byte{5, 3}
		Expect(tagIDWithSlave(seed, a)).To(Equal(tagIDWithSlave(seed, b)))
	})

	It("should treat different lists as different tags", func() {
		a, b := base(), base()
		a.SlaveIDs = []byte{3, 5}
		b.SlaveIDs = []byte{3, 6}
		Expect(tagIDWithSlave(seed, a)).NotTo(Equal(tagIDWithSlave(seed, b)))
	})

	It("should treat a list and a single slave as different tags", func() {
		a, b := base(), base()
		a.SlaveIDs = []byte{3, 5}
		b.SlaveID = 3
		Expect(tagIDWithSlave(seed, a)).NotTo(Equal(tagIDWithSlave(seed, b)))
	})

	It("should not change the hash of an item without a list", func() {
		a, b := base(), base()
		a.SlaveID = 2
		b.SlaveID = 2
		Expect(tagIDWithSlave(seed, a)).To(Equal(tagIDWithSlave(seed, b)))
	})

	It("should not mutate the caller's slice while sorting", func() {
		a := base()
		a.SlaveIDs = []byte{6, 3, 5}
		_ = tagIDWithSlave(seed, a)
		Expect(a.SlaveIDs).To(Equal([]byte{6, 3, 5}))
	})
})
