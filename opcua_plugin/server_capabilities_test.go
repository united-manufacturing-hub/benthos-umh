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

	Context("Profile-based DataChangeFilter detection", func() {
		// OPC UA Part 7, Section 6.4.3: Profile-based capability detection
		// Standard DataChange Subscription Server Facet URI indicates DataChangeFilter support
		const StandardDataChangeSubscriptionFacetURI = "http://opcfoundation.org/UA-Profile/Server/StandardDataChangeSubscription"

		Context("detectDataChangeFilterSupport helper function", func() {
			It("should return true when Standard DataChange Subscription facet URI is present", func() {
				// Kepware/Ignition/S7-1500 scenario - supports DataChangeFilter
				profileArray := []string{
					"http://opcfoundation.org/UA-Profile/Server/StandardDataChangeSubscription",
					"http://opcfoundation.org/UA-Profile/Server/StandardUA2017",
				}

				// Call helper function to detect support
				supports := detectDataChangeFilterSupport(profileArray)

				// Expect true because Standard facet is present
				Expect(supports).To(BeTrue(),
					"Should detect DataChangeFilter support when Standard DataChange Subscription facet is in profile array")
			})

			It("should return false when Standard DataChange Subscription facet URI is absent", func() {
				// S7-1200 scenario - Micro Embedded Device profile without Standard facet
				profileArray := []string{
					"http://opcfoundation.org/UA-Profile/Server/MicroEmbeddedDevice2017",
				}

				// Call helper function to detect support
				supports := detectDataChangeFilterSupport(profileArray)

				// Expect false because Standard facet is NOT present
				Expect(supports).To(BeFalse(),
					"Should NOT detect DataChangeFilter support when Standard DataChange Subscription facet is absent")
			})

			It("should return false for empty profile array", func() {
				// Server declares no conformance profiles
				profileArray := []string{}

				// Call helper function to detect support
				supports := detectDataChangeFilterSupport(profileArray)

				// Expect false (safe default)
				Expect(supports).To(BeFalse(),
					"Should default to false when profile array is empty")
			})

			It("should return false for nil profile array", func() {
				// Server doesn't support ServerProfileArray node
				var profileArray []string = nil

				// Call helper function to detect support
				supports := detectDataChangeFilterSupport(profileArray)

				// Expect false (safe default)
				Expect(supports).To(BeFalse(),
					"Should default to false when profile array is nil")
			})

			It("should perform case-sensitive URI matching", func() {
				// Profile URIs are case-sensitive per OPC UA spec
				profileArray := []string{
					"http://opcfoundation.org/UA-PROFILE/SERVER/STANDARDDATACHANGESUBSCRIPTION", // Wrong case
				}

				// Call helper function to detect support
				supports := detectDataChangeFilterSupport(profileArray)

				// Expect false because URI doesn't match exactly (case matters)
				Expect(supports).To(BeFalse(),
					"Should perform case-sensitive URI matching - wrong case should not match")
			})

			It("should handle multiple profiles and only match exact URI", func() {
				// Mix of profiles, only Standard DataChange facet indicates filter support
				profileArray := []string{
					"http://opcfoundation.org/UA-Profile/Server/MicroEmbeddedDevice2017",
					"http://opcfoundation.org/UA-Profile/Server/StandardUA2017",
					"http://opcfoundation.org/UA-Profile/Server/StandardDataChangeSubscription", // This one!
					"http://opcfoundation.org/UA-Profile/Server/SomeOtherProfile",
				}

				// Call helper function to detect support
				supports := detectDataChangeFilterSupport(profileArray)

				// Expect true because Standard facet is present (even among other profiles)
				Expect(supports).To(BeTrue(),
					"Should detect support when Standard facet is present among multiple profiles")
			})
		})
	})
})
