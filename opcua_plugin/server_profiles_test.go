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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DetectServerProfile", func() {
	Context("when detecting known servers", func() {
		It("should detect Ignition Gateway", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Inductive Automation",
				ProductName:      "Ignition Gateway v8.1.35",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileIgnition))
		})

		It("should detect Kepware KEPServerEX", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "PTC",
				ProductName:      "KEPServerEX v6.11",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileKepware))
		})

		It("should detect Siemens S7-1200", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Siemens AG",
				ProductName:      "SIMATIC S7-1200",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileS71200))
		})

		It("should detect Siemens S7-1500", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Siemens AG",
				ProductName:      "SIMATIC S7-1500",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileS71500))
		})

		It("should detect Prosys Simulation Server", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Prosys OPC",
				ProductName:      "Simulation Server",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileProsys))
		})

		It("should detect unknown server as fallback", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Acme Corp",
				ProductName:      "Custom OPC Server v1.0",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileUnknown))
		})

		It("should handle nil ServerInfo", func() {
			profile := DetectServerProfile(nil)

			Expect(profile.Name).To(Equal(ProfileUnknown))
		})
	})

	Context("when detecting by product name only", func() {
		It("should detect Ignition from product name", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Unknown",
				ProductName:      "Ignition v8.1",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileIgnition))
		})

		It("should detect Eclipse Milo as Ignition", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Eclipse Foundation",
				ProductName:      "Eclipse Milo Server",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileIgnition))
		})

		It("should detect Kepware from product name", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Unknown",
				ProductName:      "KEPServerEX",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileKepware))
		})
	})

	Context("when detecting Siemens models", func() {
		It("should prioritize S7-1200 over S7-1500 when both match", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Siemens AG",
				ProductName:      "S7-1200 Advanced",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileS71200))
		})

		It("should detect S7-1500 by model number only", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Siemens",
				ProductName:      "CPU 1500",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileS71500))
		})

		It("should detect S7-1200 by model number only", func() {
			serverInfo := &ServerInfo{
				ManufacturerName: "Siemens",
				ProductName:      "CPU 1200",
			}

			profile := DetectServerProfile(serverInfo)

			Expect(profile.Name).To(Equal(ProfileS71200))
		})
	})
})

var _ = Describe("GetProfileByName", func() {
	Context("when looking up valid profile names", func() {
		It("should return Auto profile", func() {
			profile := GetProfileByName(ProfileAuto)

			Expect(profile.Name).To(Equal(ProfileAuto))
		})

		It("should return High-Performance profile", func() {
			profile := GetProfileByName(ProfileHighPerformance)

			Expect(profile.Name).To(Equal(ProfileHighPerformance))
		})

		It("should return Ignition profile", func() {
			profile := GetProfileByName(ProfileIgnition)

			Expect(profile.Name).To(Equal(ProfileIgnition))
		})

		It("should return Kepware profile", func() {
			profile := GetProfileByName(ProfileKepware)

			Expect(profile.Name).To(Equal(ProfileKepware))
		})

		It("should return S7-1200 profile", func() {
			profile := GetProfileByName(ProfileS71200)

			Expect(profile.Name).To(Equal(ProfileS71200))
		})

		It("should return S7-1500 profile", func() {
			profile := GetProfileByName(ProfileS71500)

			Expect(profile.Name).To(Equal(ProfileS71500))
		})

		It("should return Prosys profile", func() {
			profile := GetProfileByName(ProfileProsys)

			Expect(profile.Name).To(Equal(ProfileProsys))
		})
	})

	Context("when looking up invalid profile names", func() {
		It("should return unknown fallback for invalid name", func() {
			profile := GetProfileByName("invalid-profile-name")

			Expect(profile.Name).To(Equal(ProfileUnknown))
		})

		It("should return unknown fallback for empty string", func() {
			profile := GetProfileByName("")

			Expect(profile.Name).To(Equal(ProfileUnknown))
		})
	})
})

var _ = Describe("Profile Value Validation", func() {
	Context("when validating all profile values", func() {
		It("should have reasonable MaxBatchSize values", func() {
			profiles := []ServerProfile{
				GetProfileByName(ProfileAuto),
				GetProfileByName(ProfileHighPerformance),
				GetProfileByName(ProfileIgnition),
				GetProfileByName(ProfileKepware),
				GetProfileByName(ProfileS71200),
				GetProfileByName(ProfileS71500),
				GetProfileByName(ProfileProsys),
			}

			for _, profile := range profiles {
				Expect(profile.MaxBatchSize).To(BeNumerically(">=", 50),
					"Profile %s MaxBatchSize should be >= 50", profile.Name)
				Expect(profile.MaxBatchSize).To(BeNumerically("<=", 1000),
					"Profile %s MaxBatchSize should be <= 1000", profile.Name)
			}
		})

		It("should have MaxWorkers greater than MinWorkers", func() {
			profiles := []ServerProfile{
				GetProfileByName(ProfileAuto),
				GetProfileByName(ProfileHighPerformance),
				GetProfileByName(ProfileIgnition),
				GetProfileByName(ProfileKepware),
				GetProfileByName(ProfileS71200),
				GetProfileByName(ProfileS71500),
				GetProfileByName(ProfileProsys),
			}

			for _, profile := range profiles {
				Expect(profile.MaxWorkers).To(BeNumerically(">", profile.MinWorkers),
					"Profile %s MaxWorkers (%d) should be > MinWorkers (%d)",
					profile.Name, profile.MaxWorkers, profile.MinWorkers)
			}
		})

		It("should have MinWorkers >= 1", func() {
			profiles := []ServerProfile{
				GetProfileByName(ProfileAuto),
				GetProfileByName(ProfileHighPerformance),
				GetProfileByName(ProfileIgnition),
				GetProfileByName(ProfileKepware),
				GetProfileByName(ProfileS71200),
				GetProfileByName(ProfileS71500),
				GetProfileByName(ProfileProsys),
			}

			for _, profile := range profiles {
				Expect(profile.MinWorkers).To(BeNumerically(">=", 1),
					"Profile %s MinWorkers should be >= 1", profile.Name)
			}
		})

		It("should have non-empty DisplayName for all profiles", func() {
			profiles := []ServerProfile{
				GetProfileByName(ProfileAuto),
				GetProfileByName(ProfileHighPerformance),
				GetProfileByName(ProfileIgnition),
				GetProfileByName(ProfileKepware),
				GetProfileByName(ProfileS71200),
				GetProfileByName(ProfileS71500),
				GetProfileByName(ProfileProsys),
			}

			for _, profile := range profiles {
				Expect(profile.DisplayName).NotTo(BeEmpty(),
					"Profile %s should have a DisplayName", profile.Name)
			}
		})

		It("should have non-empty Description for all profiles", func() {
			profiles := []ServerProfile{
				GetProfileByName(ProfileAuto),
				GetProfileByName(ProfileHighPerformance),
				GetProfileByName(ProfileIgnition),
				GetProfileByName(ProfileKepware),
				GetProfileByName(ProfileS71200),
				GetProfileByName(ProfileS71500),
				GetProfileByName(ProfileProsys),
			}

			for _, profile := range profiles {
				Expect(profile.Description).NotTo(BeEmpty(),
					"Profile %s should have a Description", profile.Name)
			}
		})
	})

	Context("when validating specific profile values", func() {
		It("should have correct values for Auto profile", func() {
			profile := GetProfileByName(ProfileAuto)

			Expect(profile.MaxBatchSize).To(Equal(50))
			Expect(profile.MaxWorkers).To(Equal(5))
			Expect(profile.MinWorkers).To(Equal(1))
		})

		It("should have correct values for Ignition profile", func() {
			profile := GetProfileByName(ProfileIgnition)

			Expect(profile.MaxBatchSize).To(Equal(100))
			Expect(profile.MaxWorkers).To(Equal(20))
			Expect(profile.MinWorkers).To(Equal(5))
		})

		It("should have correct values for Kepware profile", func() {
			profile := GetProfileByName(ProfileKepware)

			Expect(profile.MaxBatchSize).To(Equal(1000))
			Expect(profile.MaxWorkers).To(Equal(40))
			Expect(profile.MinWorkers).To(Equal(5))
		})

		It("should have correct values for S7-1200 profile", func() {
			profile := GetProfileByName(ProfileS71200)

			Expect(profile.MaxBatchSize).To(Equal(100))
			Expect(profile.MaxWorkers).To(Equal(10))
			Expect(profile.MinWorkers).To(Equal(3))
		})

		It("should have correct values for S7-1500 profile", func() {
			profile := GetProfileByName(ProfileS71500)

			Expect(profile.MaxBatchSize).To(Equal(500))
			Expect(profile.MaxWorkers).To(Equal(20))
			Expect(profile.MinWorkers).To(Equal(5))
		})

		It("should have correct values for Prosys profile", func() {
			profile := GetProfileByName(ProfileProsys)

			Expect(profile.MaxBatchSize).To(Equal(800))
			Expect(profile.MaxWorkers).To(Equal(60))
			Expect(profile.MinWorkers).To(Equal(5))
		})

		It("should have correct values for High-Performance profile", func() {
			profile := GetProfileByName(ProfileHighPerformance)

			Expect(profile.MaxBatchSize).To(Equal(1000))
			Expect(profile.MaxWorkers).To(Equal(50))
			Expect(profile.MinWorkers).To(Equal(10))
		})
	})
})

var _ = Describe("ServerProfile MaxMonitoredItems", func() {
	Context("Profile Definitions", func() {
		It("should have MaxMonitoredItems=1000 for S7-1200 profile", func() {
			profile := GetProfileByName(ProfileS71200)
			Expect(profile.MaxMonitoredItems).To(Equal(1000))
		})

		It("should have MaxMonitoredItems=10000 for S7-1500 profile", func() {
			profile := GetProfileByName(ProfileS71500)
			Expect(profile.MaxMonitoredItems).To(Equal(10000))
		})

		It("should have MaxMonitoredItems=0 (unlimited) for Kepware profile", func() {
			profile := GetProfileByName(ProfileKepware)
			Expect(profile.MaxMonitoredItems).To(Equal(0))
		})

		It("should have MaxMonitoredItems=0 (unlimited) for Ignition profile", func() {
			profile := GetProfileByName(ProfileIgnition)
			Expect(profile.MaxMonitoredItems).To(Equal(0))
		})

		It("should have MaxMonitoredItems=0 (unlimited) for Prosys profile", func() {
			profile := GetProfileByName(ProfileProsys)
			Expect(profile.MaxMonitoredItems).To(Equal(0))
		})

		It("should have MaxMonitoredItems=0 (unlimited) for High-Performance profile", func() {
			profile := GetProfileByName(ProfileHighPerformance)
			Expect(profile.MaxMonitoredItems).To(Equal(0))
		})

		It("should have MaxMonitoredItems=0 (unlimited) for Auto profile", func() {
			profile := GetProfileByName(ProfileAuto)
			Expect(profile.MaxMonitoredItems).To(Equal(0))
		})
	})
})

var _ = Describe("ServerProfile DataChangeFilter Support", func() {
	Context("when checking profile filter support", func() {
		It("should have SupportsDataChangeFilter field in ServerProfile struct", func() {
			profile := ServerProfile{
				Name:                     "test",
				DisplayName:              "Test Profile",
				Description:              "Test description",
				MaxBatchSize:             100,
				MaxWorkers:               10,
				MinWorkers:               1,
				MaxMonitoredItems:        0,
				SupportsDataChangeFilter: true,
			}
			Expect(profile.SupportsDataChangeFilter).To(BeTrue())
		})

		It("should set S7-1200 profile to NOT support DataChangeFilter", func() {
			profile := GetProfileByName(ProfileS71200)
			Expect(profile.SupportsDataChangeFilter).To(BeFalse(),
				"S7-1200 implements Micro Embedded Device 2017 profile without Standard DataChange Subscription Server Facet")
		})

		It("should set S7-1500 profile to support DataChangeFilter", func() {
			profile := GetProfileByName(ProfileS71500)
			Expect(profile.SupportsDataChangeFilter).To(BeTrue(),
				"S7-1500 implements Standard facet with DataChangeFilter support")
		})

		It("should set Kepware profile to support DataChangeFilter", func() {
			profile := GetProfileByName(ProfileKepware)
			Expect(profile.SupportsDataChangeFilter).To(BeTrue(),
				"Kepware is a standard OPC UA server with full DataChangeFilter support")
		})

		It("should set Ignition profile to support DataChangeFilter", func() {
			profile := GetProfileByName(ProfileIgnition)
			Expect(profile.SupportsDataChangeFilter).To(BeTrue(),
				"Ignition (Eclipse Milo) implements Standard facet with DataChangeFilter support")
		})

		It("should set Prosys profile to support DataChangeFilter", func() {
			profile := GetProfileByName(ProfileProsys)
			Expect(profile.SupportsDataChangeFilter).To(BeTrue(),
				"Prosys Simulation Server has full Standard DataChangeFilter support")
		})

		It("should set High-Performance profile to support DataChangeFilter", func() {
			profile := GetProfileByName(ProfileHighPerformance)
			Expect(profile.SupportsDataChangeFilter).To(BeTrue(),
				"High-performance VM servers typically support Standard facet")
		})

		It("should set Auto profile to NOT support DataChangeFilter by default", func() {
			profile := GetProfileByName(ProfileAuto)
			Expect(profile.SupportsDataChangeFilter).To(BeFalse(),
				"Auto profile uses defensive default (false) for unknown servers")
		})

		It("should set Unknown profile to NOT support DataChangeFilter", func() {
			profile := GetProfileByName("invalid-profile-name")
			Expect(profile.SupportsDataChangeFilter).To(BeFalse(),
				"Unknown profile uses conservative fallback (false)")
		})
	})

	Context("when validating all profiles have filter support setting", func() {
		It("should have SupportsDataChangeFilter set for all profiles", func() {
			profiles := []struct {
				name     string
				expected bool
			}{
				{ProfileAuto, false},
				{ProfileHighPerformance, true},
				{ProfileIgnition, true},
				{ProfileKepware, true},
				{ProfileS71200, false}, // Critical fix - Micro Embedded Device profile
				{ProfileS71500, true},
				{ProfileProsys, true},
				{"unknown", false},
			}

			for _, tc := range profiles {
				profile := GetProfileByName(tc.name)
				Expect(profile.SupportsDataChangeFilter).To(Equal(tc.expected),
					"Profile %s should have SupportsDataChangeFilter=%v", tc.name, tc.expected)
			}
		})
	})
})

var _ = Describe("validateProfile", func() {
	Context("when validating profile constraints", func() {
		It("should panic if MinWorkers > MaxWorkers", func() {
			invalidProfile := ServerProfile{
				Name:       "test-invalid",
				MaxWorkers: 5,
				MinWorkers: 10, // Programming mistake!
			}

			Expect(func() {
				validateProfile(invalidProfile)
			}).To(PanicWith(MatchRegexp(
				"PROGRAMMING ERROR in profile test-invalid: MinWorkers \\(10\\) > MaxWorkers \\(5\\)",
			)))
		})

		It("should not panic when MinWorkers < MaxWorkers", func() {
			validProfile := ServerProfile{
				Name:         "test-valid",
				MaxWorkers:   10,
				MinWorkers:   5,
				MaxBatchSize: 100,
			}

			Expect(func() {
				validateProfile(validProfile)
			}).NotTo(Panic())
		})

		It("should not panic when MinWorkers == MaxWorkers", func() {
			edgeCaseProfile := ServerProfile{
				Name:         "test-edge",
				MaxWorkers:   5,
				MinWorkers:   5,
				MaxBatchSize: 100,
			}

			Expect(func() {
				validateProfile(edgeCaseProfile)
			}).NotTo(Panic())
		})

		It("should panic if MinWorkers < 1", func() {
			invalidProfile := ServerProfile{
				Name:         "test-negative-minworkers",
				MaxWorkers:   10,
				MinWorkers:   0, // Invalid!
				MaxBatchSize: 100,
			}

			Expect(func() {
				validateProfile(invalidProfile)
			}).To(PanicWith(MatchRegexp(
				"PROGRAMMING ERROR in profile test-negative-minworkers: MinWorkers \\(0\\) must be >= 1",
			)))
		})

		It("should panic if MaxWorkers = 0", func() {
			invalidProfile := ServerProfile{
				Name:         "test-zero-maxworkers",
				MaxWorkers:   0, // Invalid!
				MinWorkers:   1,
				MaxBatchSize: 100,
			}

			Expect(func() {
				validateProfile(invalidProfile)
			}).To(PanicWith(MatchRegexp(
				"PROGRAMMING ERROR in profile test-zero-maxworkers: MaxWorkers \\(0\\) must be >= 1",
			)))
		})

		It("should panic if MaxWorkers < 0", func() {
			invalidProfile := ServerProfile{
				Name:         "test-negative-maxworkers",
				MaxWorkers:   -5, // Invalid!
				MinWorkers:   1,
				MaxBatchSize: 100,
			}

			Expect(func() {
				validateProfile(invalidProfile)
			}).To(PanicWith(MatchRegexp(
				"PROGRAMMING ERROR in profile test-negative-maxworkers: MaxWorkers \\(-5\\) must be >= 1",
			)))
		})
	})
})
