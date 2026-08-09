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
	"github.com/redpanda-data/benthos/v4/public/service"
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

var _ = Describe("validateAndAppend with slave lists", func() {
	var (
		m          *ModbusInput
		seed       maphash.Seed
		seenFields map[uint64]struct{}
	)

	BeforeEach(func() {
		m = &ModbusInput{
			Log:      service.MockResources().Logger(),
			SlaveIDs: []byte{1, 2, 3, 4, 5, 6},
		}
		seed = maphash.MakeSeed()
		seenFields = make(map[uint64]struct{})
	})

	item := func(ids ...byte) ModbusDataItemWithAddress {
		return ModbusDataItemWithAddress{
			Name: "reg", Register: "holding", Address: 23, Type: "INT16", SlaveIDs: ids,
		}
	}

	It("should accept a subset of the configured slaves", func() {
		out, err := m.validateAndAppend(nil, item(3, 5, 6), seed, seenFields)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(HaveLen(1))
		Expect(out[0].SlaveIDs).To(Equal([]byte{3, 5, 6}))
	})

	It("should reject an ID missing from the top-level slaveIDs", func() {
		_, err := m.validateAndAppend(nil, item(3, 7), seed, seenFields)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("slaveID 7"))
		Expect(err.Error()).To(ContainSubstring("top-level slaveIDs list"))
	})

	It("should drop a second identical entry as a duplicate", func() {
		out, err := m.validateAndAppend(nil, item(3, 5), seed, seenFields)
		Expect(err).NotTo(HaveOccurred())

		out, err = m.validateAndAppend(out, item(5, 3), seed, seenFields)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(HaveLen(1))
	})

	It("should keep entries with different slave lists", func() {
		out, err := m.validateAndAppend(nil, item(3, 5), seed, seenFields)
		Expect(err).NotTo(HaveOccurred())

		out, err = m.validateAndAppend(out, item(4, 6), seed, seenFields)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(HaveLen(2))
	})
})

var _ = Describe("targetSlaves", func() {
	configured := []byte{1, 2, 3, 4, 5, 6}

	It("should return the list when one is set", func() {
		item := ModbusDataItemWithAddress{SlaveIDs: []byte{3, 5, 6}}
		Expect(item.targetSlaves(configured)).To(Equal([]byte{3, 5, 6}))
	})

	It("should prefer the list over a single ID", func() {
		item := ModbusDataItemWithAddress{SlaveID: 2, SlaveIDs: []byte{3, 5}}
		Expect(item.targetSlaves(configured)).To(Equal([]byte{3, 5}))
	})

	It("should return the single ID when no list is set", func() {
		item := ModbusDataItemWithAddress{SlaveID: 2}
		Expect(item.targetSlaves(configured)).To(Equal([]byte{2}))
	})

	It("should return every configured slave when nothing is set", func() {
		item := ModbusDataItemWithAddress{}
		Expect(item.targetSlaves(configured)).To(Equal(configured))
	})
})

var _ = Describe("buildPerSlaveAddresses", func() {
	newInput := func(addrs ...ModbusDataItemWithAddress) *ModbusInput {
		return &ModbusInput{
			Log:       service.MockResources().Logger(),
			SlaveIDs:  []byte{1, 2, 3, 4, 5, 6},
			Addresses: addrs,
		}
	}

	named := func(name string, ids ...byte) ModbusDataItemWithAddress {
		return ModbusDataItemWithAddress{
			Name: name, Register: "holding", Address: 23, Type: "INT16", SlaveIDs: ids,
		}
	}

	It("should assign a subset address to exactly its slaves", func() {
		perSlave := newInput(named("reg", 3, 5, 6)).buildPerSlaveAddresses()
		Expect(perSlave).To(HaveLen(3))
		Expect(perSlave).To(HaveKey(byte(3)))
		Expect(perSlave).To(HaveKey(byte(5)))
		Expect(perSlave).To(HaveKey(byte(6)))
		Expect(perSlave[byte(3)]).To(HaveLen(1))
		Expect(perSlave[byte(3)][0].Name).To(Equal("reg"))
	})

	It("should not assign a subset address to unlisted slaves", func() {
		perSlave := newInput(named("reg", 3, 5, 6)).buildPerSlaveAddresses()
		Expect(perSlave).NotTo(HaveKey(byte(1)))
		Expect(perSlave).NotTo(HaveKey(byte(2)))
		Expect(perSlave).NotTo(HaveKey(byte(4)))
	})

	It("should assign a single-slave address to that slave only", func() {
		item := ModbusDataItemWithAddress{Name: "one", Register: "holding", Address: 10, Type: "INT16", SlaveID: 2}
		perSlave := newInput(item).buildPerSlaveAddresses()
		Expect(perSlave).To(HaveLen(1))
		Expect(perSlave).To(HaveKey(byte(2)))
	})

	It("should assign an unrestricted address to every configured slave", func() {
		item := ModbusDataItemWithAddress{Name: "all", Register: "holding", Address: 11, Type: "INT16"}
		perSlave := newInput(item).buildPerSlaveAddresses()
		Expect(perSlave).To(HaveLen(6))
	})

	It("should mix subset, single and unrestricted addresses", func() {
		perSlave := newInput(
			named("sub", 3, 5),
			ModbusDataItemWithAddress{Name: "one", Register: "holding", Address: 10, Type: "INT16", SlaveID: 1},
			ModbusDataItemWithAddress{Name: "all", Register: "holding", Address: 11, Type: "INT16"},
		).buildPerSlaveAddresses()

		Expect(perSlave[byte(1)]).To(HaveLen(2)) // one + all
		Expect(perSlave[byte(3)]).To(HaveLen(2)) // sub + all
		Expect(perSlave[byte(2)]).To(HaveLen(1)) // all
	})
})

var _ = Describe("dedupPerSlave", func() {
	newInput := func(addrs ...ModbusDataItemWithAddress) *ModbusInput {
		return &ModbusInput{
			Log:       service.MockResources().Logger(),
			SlaveIDs:  []byte{1, 2, 3, 4, 5, 6},
			Addresses: addrs,
		}
	}

	// Same register+address, different tag names: which one survives on a given slave
	// is decided by config order.
	named := func(name string, ids ...byte) ModbusDataItemWithAddress {
		return ModbusDataItemWithAddress{
			Name: name, Register: "holding", Address: 23, Type: "INT16", SlaveIDs: ids,
		}
	}

	unrestricted := func(name string) ModbusDataItemWithAddress {
		return ModbusDataItemWithAddress{Name: name, Register: "holding", Address: 23, Type: "INT16"}
	}

	namesFor := func(perSlave map[byte][]ModbusDataItemWithAddress, sid byte) []string {
		names := make([]string, 0, len(perSlave[sid]))
		for _, item := range perSlave[sid] {
			names = append(names, item.Name)
		}
		return names
	}

	It("should keep the subset entry on its slaves and the unrestricted entry elsewhere", func() {
		m := newInput(named("a", 3, 5), unrestricted("b"))
		perSlave := m.dedupPerSlave(m.buildPerSlaveAddresses())

		Expect(namesFor(perSlave, 3)).To(Equal([]string{"a"}))
		Expect(namesFor(perSlave, 5)).To(Equal([]string{"a"}))
		Expect(namesFor(perSlave, 1)).To(Equal([]string{"b"}))
		Expect(namesFor(perSlave, 2)).To(Equal([]string{"b"}))
		Expect(namesFor(perSlave, 4)).To(Equal([]string{"b"}))
		Expect(namesFor(perSlave, 6)).To(Equal([]string{"b"}))
	})

	It("should let config order decide, so a leading unrestricted entry wins everywhere", func() {
		m := newInput(unrestricted("b"), named("a", 3, 5))
		perSlave := m.dedupPerSlave(m.buildPerSlaveAddresses())

		for _, sid := range m.SlaveIDs {
			Expect(namesFor(perSlave, sid)).To(Equal([]string{"b"}), "slave %d", sid)
		}
	})

	It("should keep entries that differ in register or address", func() {
		other := ModbusDataItemWithAddress{Name: "c", Register: "input", Address: 23, Type: "INT16"}
		m := newInput(named("a", 3), other)
		perSlave := m.dedupPerSlave(m.buildPerSlaveAddresses())

		Expect(namesFor(perSlave, 3)).To(ConsistOf("a", "c"))
		Expect(namesFor(perSlave, 1)).To(Equal([]string{"c"}))
	})

	It("should leave a map without collisions untouched", func() {
		m := newInput(named("a", 3), ModbusDataItemWithAddress{
			Name: "d", Register: "holding", Address: 24, Type: "INT16", SlaveIDs: []byte{5},
		})
		perSlave := m.dedupPerSlave(m.buildPerSlaveAddresses())

		Expect(perSlave).To(HaveLen(2))
		Expect(namesFor(perSlave, 3)).To(Equal([]string{"a"}))
		Expect(namesFor(perSlave, 5)).To(Equal([]string{"d"}))
	})
})
