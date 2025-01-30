package opcua_plugin_test

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/redpanda-data/benthos/v4/public/service"
	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These are tests which only use the KepServer itself and none of the underlying
// PLC's, which are connected via OPC-UA. We will check on connectivity and verify
// some static and dynamic data exchange.
var _ = Describe("Test against KepServer EX6", func() {
	var (
		endpoint string
		username string
		password string
		input    *OPCUAInput
		ctx      context.Context
		cancel   context.CancelFunc
	)

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_KEPWARE_ENDPOINT")
		username = os.Getenv("TEST_KEPWARE_USERNAME")
		password = os.Getenv("TEST_KEPWARE_PASSWORD")

		if endpoint == "" || username == "" || password == "" {
			Skip("Skipping test: environmental variables are not set")
		}

		ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	})

	AfterEach(func() {
		if input.Client != nil {
			err := input.Client.Close(ctx)
			Expect(err).NotTo(HaveOccurred())
		}

		if cancel != nil {
			cancel()
		}
	})

	DescribeTable("Connect and Read", func(opcInput *OPCUAInput, errorExpected bool, expectedValue any, isChangingValue bool) {

		input = opcInput
		input.Endpoint = endpoint

		err := input.Connect(ctx)
		if errorExpected {
			Expect(err).To(HaveOccurred())
			return
		}
		Expect(err).NotTo(HaveOccurred())

		// early return since we only want to check for connectivity in some test-cases
		if input.NodeIDs == nil {
			return
		}

		// validate the data coming from kepware itself (static and dynamic)
		validateStaticAndChangingData(ctx, input, expectedValue, isChangingValue)

	},
		Entry("should connect", &OPCUAInput{
			NodeIDs:                    nil,
			SubscribeEnabled:           false,
			AutoReconnect:              true,
			ReconnectIntervalInSeconds: 5,
		}, false, nil, false),
		Entry("should connect in no security mode", &OPCUAInput{
			NodeIDs:          nil,
			SubscribeEnabled: false,
			SecurityMode:     "None",
			SecurityPolicy:   "None",
		}, false, nil, false),
		Entry("should connect with correct credentials", &OPCUAInput{
			Username: username,
			Password: password,
			NodeIDs:  nil,
		}, false, nil, false),
		Entry("should fail to connect using incorrect credentials", &OPCUAInput{
			Username: "123",
			Password: "123",
			NodeIDs:  nil,
		}, true, nil, false),
		Entry("should check if message-value is 123", &OPCUAInput{
			NodeIDs:                    ParseNodeIDs([]string{"ns=2;s=Tests.TestDevice.testConstData"}),
			AutoReconnect:              true,
			ReconnectIntervalInSeconds: 5,
		}, false, json.Number("123"), false),
		Entry("should return data changes on subscribe", &OPCUAInput{
			NodeIDs:          ParseNodeIDs([]string{"ns=2;s=Tests.TestDevice.testChangingData"}),
			SubscribeEnabled: true,
		}, false, nil, true),
	)
})

// Here we are testing the underlying opc-clients, which are siemens s7 / wago
// they're connected via opc-ua as clients
// We verify that we are able to find their namespaceArrays and check for the
// correct namespace. On top of that we are reading static and changing data
// from the underlying S7-1200.
var _ = Describe("Test underlying OPC-clients", func() {
	var (
		endpoint string
		username string
		password string
		input    *OPCUAInput
		ctx      context.Context
		cancel   context.CancelFunc
	)

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_KEPWARE_ENDPOINT")
		username = os.Getenv("TEST_KEPWARE_USERNAME")
		password = os.Getenv("TEST_KEPWARE_PASSWORD")

		if endpoint == "" || username == "" || password == "" {
			Skip("Skipping test: environmental variables are not set")
		}

		ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	})

	AfterEach(func() {
		if input.Client != nil {
			err := input.Client.Close(ctx)
			Expect(err).NotTo(HaveOccurred())
		}

		if cancel != nil {
			cancel()
		}
	})

	// Testing for the PLC-Namespaces which are included in the KepServer.
	// Therefore we fetch the namespaceArray and check if the correct namespace
	// exists here.
	DescribeTable("Test if PLC-Namespaces are available", func(namespace string, nodeID *ua.NodeID, isNamespaceAvailable bool) {
		input = &OPCUAInput{
			Endpoint:                   endpoint,
			Username:                   username,
			Password:                   password,
			AutoReconnect:              true,
			ReconnectIntervalInSeconds: 5,
		}

		err := input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		req := &ua.ReadRequest{
			NodesToRead: []*ua.ReadValueID{
				{
					NodeID:      nodeID,
					AttributeID: ua.AttributeIDValue,
				},
			},
		}

		resp, err := input.Read(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Results[0].Status).To(Equal(ua.StatusOK))

		namespaces, ok := resp.Results[0].Value.Value().([]string)
		Expect(ok).To(Equal(true))

		if !isNamespaceAvailable {
			Expect(namespaces).NotTo(ContainElement(namespace))
			return
		}
		Expect(namespaces).To(ContainElement(namespace))
	},
		Entry(
			"should contain siemens-namespace",
			"http://Server _interface_1",
			ua.NewStringNodeID(2, "SiemensPLC_main.main.Server.NamespaceArray"),
			true,
		),
		Entry(
			"should fail due to incorrect namespace",
			"totally wrong namespace",
			ua.NewStringNodeID(2, "SiemensPLC_main.main.Server.NamespaceArray"),
			false,
		),
		Entry(
			"should contain wago-namespace",
			"urn:wago-com:codesys-provider",
			ua.NewStringNodeID(2, "Wago.play.Server.NamespaceArray"),
			true,
		),
		Entry(
			"should fail due to incorrect namespace",
			"totally wrong namespace",
			ua.NewStringNodeID(2, "Wago.play.Server.NamespaceArray"),
			false,
		),
	)

	// Read static and dynamic data from the underlying S7-1200 (connected via OPC-UA)
	// and verify it's type and values.
	DescribeTable("check for correct values", func(opcInput *OPCUAInput, expectedValue any, isChangingValue bool) {

		input = opcInput
		input.Endpoint = endpoint

		err := input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		// validate on the static and dynamic data from underlying s7-1200
		validateStaticAndChangingData(ctx, input, expectedValue, isChangingValue)
	},
		Entry("should check if message-value is true", &OPCUAInput{
			NodeIDs:                    ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.ServerInterfaces.Server _interface_1.test"}),
			AutoReconnect:              true,
			ReconnectIntervalInSeconds: 5,
		}, true, false),
		Entry("should return data changes on subscribe", &OPCUAInput{
			NodeIDs:          ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.ServerInterfaces.Server _interface_1.counter"}),
			SubscribeEnabled: true,
		}, nil, true),
	)

})

func validateStaticAndChangingData(ctx context.Context, input *OPCUAInput, expectedValue any, isChangingValue bool) {
	var (
		messageBatch     service.MessageBatch
		messageBatch2    service.MessageBatch
		storedMessage    any
		assignableNumber json.Number = "10.0"
	)
	// read the first message batch
	Eventually(func() (int, error) {
		messageBatch, _, err := input.ReadBatch(ctx)
		return len(messageBatch), err
	}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs)))

	for _, message := range messageBatch {
		message, err := message.AsStructuredMut()
		Expect(err).NotTo(HaveOccurred())

		// if we expect a specific Value here, check if it equals
		if expectedValue != nil {
			Expect(message).To(BeAssignableToTypeOf(expectedValue))
			Expect(message).To(Equal(expectedValue))
			return
		}
		// if not we just check if the type matches since its a dynamic value
		Expect(message).To(BeAssignableToTypeOf(assignableNumber))

		storedMessage = message
	}

	// read a second message batch if we want to check on data changes
	if isChangingValue {
		Eventually(func() (int, error) {
			messageBatch2, _, err := input.ReadBatch(ctx)
			return len(messageBatch2), err
		}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs)))

		for _, message := range messageBatch2 {
			message, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())

			Expect(message).To(BeAssignableToTypeOf(assignableNumber))
			Expect(message).NotTo(Equal(storedMessage))
		}
	}
}
