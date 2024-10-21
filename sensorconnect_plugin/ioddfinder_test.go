package sensorconnect_plugin_test

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/v2/sensorconnect_plugin"
)

var _ = Describe("IODD File Tests", func() {
	var (
		ctx context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("GetIoddFile", func() {
		It("should retrieve IODD file for Siemens AG | SIRIUS ACT Electronic Module 4DI/4DQ for IO-Link", func() {
			err := AssertIoddFileGetter(ctx, 42, 278531)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should retrieve IODD file for Bosch Rexroth AG | 4WRPEH10-3X", func() {
			err := AssertIoddFileGetter(ctx, 287, 2228227)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should retrieve IODD file for ifm electronic gmbh | DTI410", func() {
			err := AssertIoddFileGetter(ctx, 310, 967)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})

// AssertIoddFileGetter is a helper function to assert the correctness of GetIoddFile
func AssertIoddFileGetter(ctx context.Context, vendorId int64, deviceId int) error {
	input := sensorconnect_plugin.SensorConnectInput{}

	err := input.FetchAndStoreIoDDFile(ctx, vendorId, deviceId)
	if err != nil {
		return err
	}

	ioddFilemapKey := sensorconnect_plugin.IoddFilemapKey{
		VendorId: vendorId,
		DeviceId: deviceId,
	}

	_, ok := input.IoDeviceMap.Load(ioddFilemapKey)
	if !ok {
		return fmt.Errorf("filemap not found for vendorId: %d, deviceId: %d", vendorId, deviceId)
	}

	return nil
}
