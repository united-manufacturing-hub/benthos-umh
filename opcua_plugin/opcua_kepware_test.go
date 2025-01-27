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

var _ = Describe("Test against KepServer EX6", func() {
	var endpoint string
	var username string
	var password string
	var input *OPCUAInput
	var ctx context.Context
	var cancel context.CancelFunc

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_KEPWARE_ENDPOINT")
		username = os.Getenv("TEST_KEPWARE_USERNAME")
		password = os.Getenv("TEST_KEPWARE_PASSWORD")

		if endpoint == "" || username == "" || password == "" {
			Skip("Skipping test: environmental variables are not set")
			return
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

	DescribeTable("Connect", func(opcInput *OPCUAInput, errorExpected bool) {

		input = opcInput
		input.Endpoint = endpoint

		err := input.Connect(ctx)
		if errorExpected {
			Expect(err).To(HaveOccurred())
			return
		}
		Expect(err).NotTo(HaveOccurred())

		var messageBatch service.MessageBatch

		switch len(input.NodeIDs) {
		case 1:
			Eventually(func() (int, error) {
				messageBatch, _, err = input.ReadBatch(ctx)
				return len(messageBatch), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs)))

			for _, message := range messageBatch {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())
				var assignableNumber json.Number = "10.0"

				Expect(message).To(BeAssignableToTypeOf(assignableNumber))
			}
			return
		case 2:
			Eventually(func() (int, error) {
				messageBatch, _, err = input.ReadBatch(ctx)
				return len(messageBatch), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs)))

			var storedMessage any

			for _, message := range messageBatch {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())
				var assignableNumber json.Number = "10.0"

				Expect(message).To(BeAssignableToTypeOf(assignableNumber))
				storedMessage = message
			}

			var messageBatch2 service.MessageBatch
			Eventually(func() (int, error) {
				messageBatch2, _, err = input.ReadBatch(ctx)
				return len(messageBatch2), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs) - 1))

			for _, message := range messageBatch2 {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())
				var assignableNumber json.Number = "10.0"

				Expect(message).To(BeAssignableToTypeOf(assignableNumber))
				Expect(message).NotTo(Equal(storedMessage))
			}
			return
		default:
			return
		}

	},
		Entry("should connect", &OPCUAInput{
			Username:                   "",
			Password:                   "",
			NodeIDs:                    nil,
			SubscribeEnabled:           false,
			AutoReconnect:              true,
			ReconnectIntervalInSeconds: 5,
		}, false),
		Entry("should connect in no security mode", &OPCUAInput{
			Username:         "",
			Password:         "",
			NodeIDs:          nil,
			SubscribeEnabled: false,
			SecurityMode:     "None",
			SecurityPolicy:   "None",
		}, false),
		Entry("should connect with correct credentials", &OPCUAInput{
			Username: username,
			Password: password,
			NodeIDs:  nil,
		}, false),
		Entry("should fail to connect using incorrect credentials", &OPCUAInput{
			Username: "123",
			Password: "123",
			NodeIDs:  nil,
		}, true),
		Entry("should return a batch of messages", &OPCUAInput{
			Username:                     "",
			Password:                     "",
			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=Tests.TestDevice.testChangingData"}),
			BrowseHierarchicalReferences: true,
			AutoReconnect:                true,
			ReconnectIntervalInSeconds:   5,
		}, false),
		Entry("should return data changes on subscribe", &OPCUAInput{
			Username:                     "",
			Password:                     "",
			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=Tests.TestDevice.testChangingData", "ns=2;s=Tests.TestDevice.testConstData"}),
			BrowseHierarchicalReferences: true,
			SubscribeEnabled:             true,
		}, false),
	)
})

var _ = Describe("Test underlying OPC-clients", func() {
	var endpoint string
	var username string
	var password string
	var input *OPCUAInput
	var ctx context.Context
	var cancel context.CancelFunc

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_KEPWARE_ENDPOINT")
		username = os.Getenv("TEST_KEPWARE_USERNAME")
		password = os.Getenv("TEST_KEPWARE_PASSWORD")

		if endpoint == "" || username == "" || password == "" {
			Skip("Skipping test: environmental variables are not set")
			return
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

	DescribeTable("Test if Siemens-Namespace is available", func(namespace string, contains bool) {
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
					NodeID:      ua.NewStringNodeID(2, "SiemensPLC_main.main.Server.NamespaceArray"),
					AttributeID: ua.AttributeIDValue,
				},
			},
		}

		resp, err := input.Read(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		namespaces, ok := resp.Results[0].Value.Value().([]string)
		Expect(ok).To(Equal(true))

		if !contains {
			Expect(namespaces).NotTo(ContainElement(namespace))
			return
		}
		Expect(namespaces).To(ContainElement(namespace))
	},
		Entry("should contain siemens-namespace", "http://Server _interface_1", true),
		Entry("should fail due to incorrect namespace", "totally wrong namespace", false),
	)

	DescribeTable("check for correct values", func(opcInput *OPCUAInput, expectedValue interface{}, dynamic bool) {

		input = opcInput
		input.Endpoint = endpoint

		err := input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		var messageBatch service.MessageBatch

		if !dynamic {
			Eventually(func() (int, error) {
				messageBatch, _, err = input.ReadBatch(ctx)
				return len(messageBatch), err
			}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs)))

			for _, message := range messageBatch {
				message, err := message.AsStructuredMut()
				Expect(err).NotTo(HaveOccurred())

				Expect(message).To(BeAssignableToTypeOf(expectedValue))
				Expect(message).To(Equal(expectedValue))
			}
			return
		}
		Eventually(func() (int, error) {
			messageBatch, _, err = input.ReadBatch(ctx)
			return len(messageBatch), err
		}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs)))

		var storedMessage any

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())
			var assignableNumber json.Number = "10.0"

			Expect(message).To(BeAssignableToTypeOf(assignableNumber))
			storedMessage = message
		}

		var messageBatch2 service.MessageBatch
		Eventually(func() (int, error) {
			messageBatch2, _, err = input.ReadBatch(ctx)
			return len(messageBatch2), err
		}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs)))

		for _, message := range messageBatch2 {
			message, err := message.AsStructuredMut()
			Expect(err).NotTo(HaveOccurred())
			var assignableNumber json.Number = "10.0"

			Expect(message).To(BeAssignableToTypeOf(assignableNumber))
			Expect(message).NotTo(Equal(storedMessage))
		}

	},
		Entry("should return true", &OPCUAInput{
			Username:                     "",
			Password:                     "",
			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.ServerInterfaces.Server _interface_1.test"}),
			BrowseHierarchicalReferences: true,
			AutoReconnect:                true,
			ReconnectIntervalInSeconds:   5,
		}, true, false),
		Entry("should return data changes on subscribe", &OPCUAInput{
			Username:                     "",
			Password:                     "",
			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.ServerInterfaces.Server _interface_1.counter"}),
			BrowseHierarchicalReferences: true,
			SubscribeEnabled:             true,
		}, nil, true),
	)

})
