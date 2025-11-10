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

package opcua_plugin

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ServerCapabilities", func() {
	Context("ServerCapabilities struct", func() {
		It("should have expected operation limit fields", func() {
			caps := ServerCapabilities{
				MaxNodesPerBrowse:           100,
				MaxMonitoredItemsPerCall:    1000,
				MaxNodesPerRead:             500,
				MaxNodesPerWrite:            500,
				MaxBrowseContinuationPoints: 10,
			}

			Expect(caps.MaxNodesPerBrowse).To(Equal(uint32(100)))
			Expect(caps.MaxMonitoredItemsPerCall).To(Equal(uint32(1000)))
			Expect(caps.MaxNodesPerRead).To(Equal(uint32(500)))
			Expect(caps.MaxNodesPerWrite).To(Equal(uint32(500)))
			Expect(caps.MaxBrowseContinuationPoints).To(Equal(uint32(10)))
		})

		It("should have SupportsDataChangeFilter field for profile-based capability detection", func() {
			// Test for new field that tracks DataChangeFilter support
			// Per OPC UA Part 7, Section 6.4.3: Profile-based capability detection
			caps := ServerCapabilities{
				SupportsDataChangeFilter: true,
			}

			Expect(caps.SupportsDataChangeFilter).To(BeTrue())

			// Test default false value
			capsDefault := ServerCapabilities{}
			Expect(capsDefault.SupportsDataChangeFilter).To(BeFalse())
		})
	})

	Context("queryOperationLimits", func() {
		var (
			g   *OPCUAInput
			ctx context.Context
		)

		BeforeEach(func() {
			g = &OPCUAInput{}
			ctx = context.Background()
		})

		It("should return nil when client is nil", func() {
			caps, err := g.queryOperationLimits(ctx)
			Expect(err).To(HaveOccurred())
			Expect(caps).To(BeNil())
		})

		It("should return error with descriptive message when client is nil", func() {
			_, err := g.queryOperationLimits(ctx)
			Expect(err).To(MatchError(ContainSubstring("client is nil")))
		})
	})

	Context("logServerCapabilities", func() {
		It("should handle nil capabilities without panic", func() {
			g := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{},
			}
			g.ServerProfile = GetProfileByName(ProfileAuto)

			// Should not panic with nil caps
			Expect(func() {
				g.logServerCapabilities(nil)
			}).NotTo(Panic())
		})

		It("should handle zero server limits gracefully", func() {
			g := &OPCUAInput{
				OPCUAConnection: &OPCUAConnection{},
			}
			g.ServerProfile = GetProfileByName(ProfileAuto)

			capsZero := &ServerCapabilities{
				MaxNodesPerBrowse:        0,
				MaxMonitoredItemsPerCall: 0,
				MaxNodesPerRead:          0,
			}

			// Should not panic and should early return (no logs since all zeros)
			Expect(func() {
				g.logServerCapabilities(capsZero)
			}).NotTo(Panic())
		})
	})

	Context("NodeID constants", func() {
		It("should define ServerCapabilities node ID", func() {
			// ns=0;i=2268
			Expect(ServerCapabilitiesNodeID).NotTo(BeNil())
		})

		It("should define OperationLimits node ID", func() {
			// ns=0;i=11704
			Expect(OperationLimitsNodeID).NotTo(BeNil())
		})

		It("should define MaxNodesPerBrowse node ID", func() {
			// ns=0;i=11712
			Expect(MaxNodesPerBrowseNodeID).NotTo(BeNil())
		})

		It("should define MaxMonitoredItemsPerCall node ID", func() {
			// ns=0;i=11714
			Expect(MaxMonitoredItemsPerCallNodeID).NotTo(BeNil())
		})

		It("should define MaxNodesPerRead node ID", func() {
			// ns=0;i=11705
			Expect(MaxNodesPerReadNodeID).NotTo(BeNil())
		})

		It("should define MaxNodesPerWrite node ID", func() {
			// ns=0;i=11708
			Expect(MaxNodesPerWriteNodeID).NotTo(BeNil())
		})
	})
})
