package opcua_plugin_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
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

	type KepwareWrapper struct {
		Username                     string
		Password                     string
		NodeIDs                      []*ua.NodeID
		SecurityMode                 string
		SecurityPolicy               string
		BrowseHierarchicalReferences bool
		AutoReconnect                bool
		ReconnectIntervalInSeconds   int
		SubscribeEnabled             bool
		ErrorOccured                 bool
		ExpectedValue                interface{}
	}

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

	//TODO: proper var handling for tableEntries since this wouldn't work
	DescribeTable("Connect", func(wrapper KepwareWrapper) {

		input = &OPCUAInput{
			Endpoint:                     endpoint,
			Username:                     wrapper.Username,
			Password:                     wrapper.Password,
			NodeIDs:                      wrapper.NodeIDs,
			SecurityMode:                 wrapper.SecurityMode,
			SecurityPolicy:               wrapper.SecurityPolicy,
			BrowseHierarchicalReferences: wrapper.BrowseHierarchicalReferences,
			AutoReconnect:                wrapper.AutoReconnect,
			ReconnectIntervalInSeconds:   wrapper.ReconnectIntervalInSeconds,
			SubscribeEnabled:             wrapper.SubscribeEnabled,
		}

		err := input.Connect(ctx)
		if wrapper.ErrorOccured {
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
		Entry("should connect", KepwareWrapper{
			Username:                   "",
			Password:                   "",
			NodeIDs:                    nil,
			SubscribeEnabled:           false,
			AutoReconnect:              true,
			ReconnectIntervalInSeconds: 5,
			ErrorOccured:               false,
		}),
		Entry("should connect in no security mode", KepwareWrapper{
			Username:         "",
			Password:         "",
			NodeIDs:          nil,
			SubscribeEnabled: false,
			SecurityMode:     "None",
			SecurityPolicy:   "None",
			ErrorOccured:     false,
		}),
		Entry("should connect with correct credentials", KepwareWrapper{
			Username:     username,
			Password:     password,
			NodeIDs:      nil,
			ErrorOccured: false,
		}),
		Entry("should fail to connect using incorrect credentials", KepwareWrapper{
			Username:     "123",
			Password:     "123",
			NodeIDs:      nil,
			ErrorOccured: true,
		}),
		Entry("should return a batch of messages", KepwareWrapper{
			Username:                     "",
			Password:                     "",
			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=Tests.TestDevice.testChangingData"}),
			BrowseHierarchicalReferences: true,
			AutoReconnect:                true,
			ReconnectIntervalInSeconds:   5,
			ErrorOccured:                 false,
		}),
		Entry("should return data changes on subscribe", KepwareWrapper{
			Username:                     "",
			Password:                     "",
			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=Tests.TestDevice.testChangingData", "ns=2;s=Tests.TestDevice.testConstData"}),
			BrowseHierarchicalReferences: true,
			SubscribeEnabled:             true,
			ErrorOccured:                 false,
		}),
	)

	//TODO: iterate over the message to check for the correct namespace here
	DescribeTable("Check PLC-data", func(wrapper KepwareWrapper) {
		//NOTE: namespaceArray consists at least of http://Server_interface_1
		// check on isDiederikCool == true
		// check on counter getting data changes

		input = &OPCUAInput{
			Endpoint:                     endpoint,
			Username:                     wrapper.Username,
			Password:                     wrapper.Password,
			NodeIDs:                      wrapper.NodeIDs,
			BrowseHierarchicalReferences: wrapper.BrowseHierarchicalReferences,
			AutoReconnect:                wrapper.AutoReconnect,
			ReconnectIntervalInSeconds:   wrapper.ReconnectIntervalInSeconds,
		}

		err := input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		var messageBatch service.MessageBatch

		Eventually(func() (int, error) {
			messageBatch, _, err = input.ReadBatch(ctx)
			return len(messageBatch), err
		}, 30*time.Second, 100*time.Millisecond).WithContext(ctx).Should(Equal(len(input.NodeIDs)))

		for _, message := range messageBatch {
			message, err := message.AsStructuredMut()
			test := reflect.TypeOf(message)

			GinkgoWriter.Printf("type of message: %v\n", test)
			GinkgoWriter.Printf("message from messageBatch:  %v\n", message)

			Expect(err).NotTo(HaveOccurred())

			//	if reflect.TypeOf(wrapper.ExpectedValue).Kind() == reflect.Slice {
			//		for _, messageEntry := range message {
			//			if messageEntry == wrapper.ExpectedValue {
			//				err = fmt.Errorf("namespace not found")
			//			}
			//		}

			//	}
			value := reflect.ValueOf(message)
			GinkgoWriter.Printf("value of message: %v\n", value)

			typeOf := reflect.TypeOf(wrapper.ExpectedValue).Kind()
			GinkgoWriter.Printf("type of ExpectedValue: %v\n", typeOf)
			Expect(message).To(BeAssignableToTypeOf(wrapper.ExpectedValue))
			Expect(message).To(Equal(wrapper.ExpectedValue))
		}

	},
		Entry("should return true for data-value", KepwareWrapper{
			Username:                     "",
			Password:                     "",
			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.ServerInterfaces.Server _interface_1.custom_struct.Is_Diederik_Cool"}),
			BrowseHierarchicalReferences: true,
			AutoReconnect:                true,
			ReconnectIntervalInSeconds:   5,
			ErrorOccured:                 false,
			ExpectedValue:                true,
		}),
		//		Entry("should receive data changes", KepwareWrapper{
		//			Username:                     "",
		//			Password:                     "",
		//			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.ServerInterfaces.Server_interface_1.counter"}),
		//			BrowseHierarchicalReferences: true,
		//			AutoReconnect:                true,
		//			ReconnectIntervalInSeconds:   5,
		//			ErrorOccured:                 false,
		//		}),
		Entry("namespaceArray should consist of expected namespace", KepwareWrapper{
			Username:                     "",
			Password:                     "",
			NodeIDs:                      ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.Server.NamespaceArray"}),
			BrowseHierarchicalReferences: true,
			AutoReconnect:                true,
			ReconnectIntervalInSeconds:   5,
			ErrorOccured:                 false,
			ExpectedValue:                "http://Server _interface_1",
		}),
	)
})
