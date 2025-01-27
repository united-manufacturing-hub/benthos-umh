package opcua_plugin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gopcua/opcua/ua"

	. "github.com/united-manufacturing-hub/benthos-umh/opcua_plugin"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// opc-plc is what is running when starting up the devcontainer
// other simulators are not running when starting up the devcontainer and are a rather manual process to start
var _ = Describe("Test Against Microsoft OPC UA simulator (opc-plc)", Serial, func() {

	BeforeEach(func() {
		testActivated := os.Getenv("TEST_OPCUA_SIMULATOR")

		// Check if environment variables are set
		if testActivated == "" {
			Skip("Skipping unit tests against simulator: TEST_OPCUA_SIMULATOR not set")
			return
		}
	})

	Describe("Connect Anonymous", func() {
		It("should connect anonymously", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var nodeIDStrings = []string{"ns=3;s=Basic"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: false,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			var messageBatch service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch, _, err = input.ReadBatch(ctx)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(4))

			// Close connection
			if input.Client != nil {
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	Describe("Connect with Username and Password", func() {
		Context("when using invalid credentials", func() {
			It("should fail to connect", func() {
				Skip("Skipping due to OPC-UA simulator limitations on logging in multiple times")
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				input := &OPCUAInput{
					Endpoint: "opc.tcp://localhost:50000",
					Username: "sysadmin_bad", // Incorrect username and password
					Password: "demo",
					NodeIDs:  nil,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).To(HaveOccurred())

				// Close connection
				if input.Client != nil {
					err := input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		Context("when using valid credentials", func() {
			It("should successfully connect", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				input := &OPCUAInput{
					Endpoint: "opc.tcp://localhost:50000",
					Username: "sysadmin", // Correct username and password
					Password: "demo",
					NodeIDs:  nil,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Close connection
				if input.Client != nil {
					err := input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})
	})

	Describe("Subscribe", func() {
		Context("when connecting to subscribe to Fast data changes", func() {
			It("should connect and receive data changes", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=Fast"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				// ns=3;s=Fast consists out of 6 nodes
				// FastUInt1 - 5 and BadFastUInt1
				// BadFastUInt1 can sometimes be null, and then it will not report anything
				// Therefore, the expected messageBatch is between 5 and 6
				// However, sometimes the OPC UA server sends back the values for multiple seconds in the same batch, so it could also be 10 or 12
				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 5))

				for _, message := range messageBatch {
					message, err := message.AsStructuredMut()
					Expect(err).NotTo(HaveOccurred())
					Expect(message).To(BeAssignableToTypeOf(json.Number("22.565684")))
					//GinkgoT().Log("Received message: ", message)
				}

				// Close connection
				if input.Client != nil {
					err := input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		Context("when connecting to subscribe to Boolean with Properties", func() {
			It("should connect and confirm properties are not browsed by default", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=6;s=DataAccess_AnalogType_Byte"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

				for _, message := range messageBatch {
					message, err := message.AsStructuredMut()
					Expect(err).NotTo(HaveOccurred())
					Expect(message).To(Equal(json.Number("0")))
					//GinkgoT().Log("Received message: ", message)
				}

				// Close connection
				if input.Client != nil {
					err := input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})
	})

	Describe("Subscribe to different datatypes", func() {
		When("Subscribing to AnalogTypes (simple datatypes)", func() {
			It("should connect and subscribe to AnalogTypes", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{
					"ns=6;s=DataAccess_AnalogType_Byte",
					"ns=6;s=DataAccess_AnalogType_Double",
					"ns=6;s=DataAccess_AnalogType_Float",
					"ns=6;s=DataAccess_AnalogType_Int16",
					"ns=6;s=DataAccess_AnalogType_Int32",
					"ns=6;s=DataAccess_AnalogType_Int64",
					"ns=6;s=DataAccess_AnalogType_SByte",
					"ns=6;s=DataAccess_AnalogType_UInt16",
					"ns=6;s=DataAccess_AnalogType_UInt32",
					"ns=6;s=DataAccess_AnalogType_UInt64",
				}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(10))

				for _, message := range messageBatch {
					message, err := message.AsStructuredMut()
					Expect(err).NotTo(HaveOccurred())
					Expect(message).To(BeAssignableToTypeOf(json.Number("22.565684")))
					//GinkgoT().Log("Received message: ", message)
				}

				// Close connection
				if input.Client != nil {
					err := input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to null datatype", func() {
			It("should not subscribe to null datatype", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=6;s=DataAccess_DataItem_Null"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1 // return -1 to indicate that the messageBatch is empty
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(0)) // should never subscribe to null datatype

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to AnalogTypeArray", func() {
			It("should connect and subscribe to AnalogTypeArray and validate data types", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=6;s=DataAccess_AnalogType_Array"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(26))

				for _, message := range messageBatch {
					messageParsed, err := message.AsStructuredMut()
					if err != nil {
						// This might happen if an empty string is returned from OPC-UA
						continue
					}

					opcuapath, found := message.MetaGet("opcua_path")
					Expect(found).To(BeTrue(), "Could not find opcua_path")

					// Determine the data type from the OPC UA path
					dataType := strings.Split(opcuapath, "_")[5] // This will extract the data type part of the OPC UA path

					// Check if the data type is an array and handle accordingly
					if strings.HasSuffix(dataType, "Array") {
						dataTypeOfArray := strings.Split(opcuapath, "_")[6]

						// Handle array data types
						switch dataTypeOfArray {
						case "Duration", "Guid", "LocaleId", "Boolean", "LocalizedText", "NodeId", "QualifiedName", "UtcTime", "DateTime", "Double", "Enumeration", "Float", "Int16", "Int32", "Int64", "Integer", "Number", "SByte", "StatusCode", "String", "UInt16", "UInt32", "UInt64", "UInteger", "Variant", "XmlElement", "ByteString":
							// Check if the messageParsed is of type slice (array)
							messageParsedArray, ok := messageParsed.([]interface{})
							Expect(ok).To(BeTrue(), fmt.Sprintf("Expected messageParsed to be an array, but got %T: %s : %s", messageParsed, opcuapath, messageParsed))

							for _, item := range messageParsedArray {
								// Here, use the checkDatatypeOfOPCUATag function adapted for Ginkgo
								checkDatatypeOfOPCUATag(dataTypeOfArray, item, opcuapath)
							}
						case "Byte":
							// Here too, use the checkDatatypeOfOPCUATag function adapted for Ginkgo
							checkDatatypeOfOPCUATag("ByteArray", messageParsed, opcuapath)
						default:
							Fail(fmt.Sprintf("Unsupported array data type in OPC UA path: %s:%s", dataType, opcuapath))
						}
					} else {
						Fail(fmt.Sprintf("Received non-array: %s", opcuapath))
					}
				}

				// Close connection
				if input.Client != nil {
					err := input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to DataItem", func() {
			It("should subscribe to all non-null datatype values", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=6;s=DataAccess_DataItem"} // Subscribes to all values with non-null datatype.
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(23)) // these are theoretically >30, but most of them are null, so the browse function ignores them

				for _, message := range messageBatch {
					messageParsed, err := message.AsStructuredMut()
					if err != nil {
						// This might happen if an empty string is returned from OPC-UA
						continue
					}

					opcuapath, found := message.MetaGet("opcua_path")
					Expect(found).To(BeTrue(), "Could not find opcua_path")

					dataType := strings.Split(opcuapath, "_")[5] // Extracts the data type part of the OPC UA path.

					// Check the data type of the message.
					checkDatatypeOfOPCUATag(dataType, messageParsed, opcuapath)
				}

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Testing for failed node crash", func() {
			It("should not crash on failed nodes", func() {
				// https://github.com/united-manufacturing-hub/MgmtIssues/issues/1088
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{
					"ns=3;s=Fast",
					"ns=3;s=Slow",
				}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)
				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(BeNumerically(">", 0))

				for _, message := range messageBatch {
					_, err := message.AsStructured()
					Expect(err).NotTo(HaveOccurred())
				}

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to Scalar Arrays", func() {
			It("should subscribe to all scalar array values with non-null data types", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				nodeIDStrings := []string{
					"ns=6;s=Scalar_Static_Arrays_Boolean",
					"ns=6;s=Scalar_Static_Arrays_Byte",
					"ns=6;s=Scalar_Static_Arrays_ByteString",
					"ns=6;s=Scalar_Static_Arrays_DateTime",
					"ns=6;s=Scalar_Static_Arrays_Double",
					"ns=6;s=Scalar_Static_Arrays_Duration",
					"ns=6;s=Scalar_Static_Arrays_Float",
					"ns=6;s=Scalar_Static_Arrays_Guid",
					"ns=6;s=Scalar_Static_Arrays_Int16",
					"ns=6;s=Scalar_Static_Arrays_Int32",
					"ns=6;s=Scalar_Static_Arrays_Int64",
					"ns=6;s=Scalar_Static_Arrays_Integer",
					"ns=6;s=Scalar_Static_Arrays_LocaleId",
					"ns=6;s=Scalar_Static_Arrays_LocalizedText",
					"ns=6;s=Scalar_Static_Arrays_NodeId",
					"ns=6;s=Scalar_Static_Arrays_Number",
					"ns=6;s=Scalar_Static_Arrays_QualifiedName",
					"ns=6;s=Scalar_Static_Arrays_SByte",
					"ns=6;s=Scalar_Static_Arrays_String",
					"ns=6;s=Scalar_Static_Arrays_UInt16",
					"ns=6;s=Scalar_Static_Arrays_UInt32",
					"ns=6;s=Scalar_Static_Arrays_UInt64",
					"ns=6;s=Scalar_Static_Arrays_UInteger",
					"ns=6;s=Scalar_Static_Arrays_UtcTime",
					// "ns=6;s=Scalar_Static_Arrays_Variant", // Excluded due to library support issues
					"ns=6;s=Scalar_Static_Arrays_XmlElement",
				}

				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(25))

				for _, message := range messageBatch {
					messageParsed, err := message.AsStructuredMut()
					if err != nil {
						// This might happen if an empty string is returned from OPC-UA
						continue
					}
					opcuapath, found := message.MetaGet("opcua_path")
					Expect(found).To(BeTrue(), "Could not find opcua_path")

					// Determine the data type from the OPC UA path
					dataType := strings.Split(opcuapath, "_")[5] // Extracts the data type part
					//GinkgoT().Log(dataType)

					if strings.HasSuffix(dataType, "Arrays") {
						dataTypeOfArray := strings.Split(opcuapath, "_")[6]
						//GinkgoT().Log(dataTypeOfArray)

						// Handle array data types
						switch dataTypeOfArray {
						case "Duration", "Guid", "LocaleId", "Boolean", "LocalizedText", "NodeId", "QualifiedName", "UtcTime", "DateTime", "Double", "Enumeration", "Float", "Int16", "Int32", "Int64", "SByte", "StatusCode", "String", "UInt16", "UInt32", "UInt64", "XmlElement", "ByteString":
							messageParsedArray, ok := messageParsed.([]interface{})
							Expect(ok).To(BeTrue(), fmt.Sprintf("Expected messageParsed to be an array, but got %T: %s : %s", messageParsed, opcuapath, messageParsed))

							for _, item := range messageParsedArray {
								// Use the adapted checkDatatypeOfOPCUATag function for Ginkgo
								checkDatatypeOfOPCUATag(dataTypeOfArray, item, opcuapath)
							}
						case "Byte", "Integer", "Number", "Variant", "UInteger": // Handling specific or unsupported types if necessary
							GinkgoT().Logf("Special handling or unsupported array data type in OPC UA path: %s:%s", dataType, opcuapath)
						default:
							Fail(fmt.Sprintf("Unsupported array data type in OPC UA path: %s:%s", dataType, opcuapath))
						}
					} else {
						Fail(fmt.Sprintf("Received non-array: %s", opcuapath))
					}
				}

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to the entire simulator", func() {
			It("does not fail", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=OpcPlc"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: false,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 125))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to everything", func() {
			It("does not fail", func() {
				Skip("This might take too long...")

				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				nodeIDStrings := []string{"i=84"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(25))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to everything", func() {
			It("does not fail", func() {
				Skip("This might take too long...")

				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				nodeIDStrings := []string{"i=84"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: true,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(25))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to Anomaly", func() {
			It("does not fail", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=Anomaly"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: false,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(4))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to Basic", func() {
			It("does not fail", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=Basic"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: false,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(4))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to Deterministic GUID", func() {
			It("does not fail", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=Deterministic GUID"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: false,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(5))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to Fast", func() {
			It("does not fail", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=Fast"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: false,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 5))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to Slow", func() {
			It("does not fail", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=Slow"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: false,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 100))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

		When("Subscribing to Special", func() {
			It("does not fail", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				nodeIDStrings := []string{"ns=3;s=Special"}
				parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

				input := &OPCUAInput{
					Endpoint:         "opc.tcp://localhost:50000",
					Username:         "",
					Password:         "",
					NodeIDs:          parsedNodeIDs,
					SubscribeEnabled: false,
				}

				// Attempt to connect
				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var messageBatch service.MessageBatch
				Eventually(func() int {
					var err error
					messageBatch, _, err = input.ReadBatch(ctx)
					if err != nil {
						// Log the error but continue - it might succeed on next attempt
						GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
						return -1
					}
					return len(messageBatch)
				}, 10*time.Second, 100*time.Millisecond).Should(Equal(7))

				// Close connection
				if input.Client != nil {
					err = input.Close(ctx)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})

	})

	Describe("metadata", func() {
		It("should create a proper opcua_tag_group and opcua_tag_name and opcua_tag_type", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var nodeIDStrings = []string{"ns=3;s=OpcPlc"}

			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: false,
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			var messageBatch service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch, _, err = input.ReadBatch(ctx)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch)
			}, 10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))

			// for each
			for _, message := range messageBatch {
				opcuaTagPath, err := message.MetaGet("opcua_tag_path")
				Expect(err).To(BeTrue(), "Could not find opcua_tag_path")
				GinkgoT().Log("opcua_tag_path: ", opcuaTagPath)

				opcuaTagGroup, err := message.MetaGet("opcua_tag_group")
				Expect(err).To(BeTrue(), "Could not find opcua_tag_group")
				GinkgoT().Log("opcua_tag_group: ", opcuaTagGroup)

				opcuaTagName, err := message.MetaGet("opcua_tag_name")
				Expect(err).To(BeTrue(), "Could not find opcua_tag_name")
				GinkgoT().Log("opcua_tag_name: ", opcuaTagName)

				opcuaTagType, err := message.MetaGet("opcua_tag_type")
				Expect(err).To(BeTrue(), "Could not find opcua_tag_type")
				GinkgoT().Log("opcua_tag_type: ", opcuaTagType)

				if opcuaTagPath == "StepUp" {
					Expect(opcuaTagGroup).To(Equal("OpcPlc.Telemetry.Basic"))
					Expect(opcuaTagName).To(Equal("StepUp"))
					Expect(opcuaTagType).To(Equal("number"))
				}
			}

			// Close connection
			if input.Client != nil {
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	When("Subscribing to slow values", func() {
		It("keeps sending data at least once every 10 seconds", func() {
			Skip("slow test")
			ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel1()

			nodeIDStrings := []string{"ns=3;s=SlowUInt1"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: true,
			}

			// Attempt to connect
			err := input.Connect(ctx1)
			Expect(err).NotTo(HaveOccurred())

			var messageBatch service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch, _, err = input.ReadBatch(ctx1)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			time.Sleep(15 * time.Second)

			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel2()

			var messageBatch2 service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch2, _, err = input.ReadBatch(ctx2)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch2)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			// Close connection
			if input.Client != nil {
				ctxEnd, cancelEnd := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelEnd()
				err = input.Close(ctxEnd)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	When("Enabling sendHeartbeat", func() {
		It("sends the heartbeat", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var nodeIDStrings []string
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: true,
				UseHeartbeat:     true,
				HeartbeatNodeId:  ua.NewNumericNodeID(0, 2258),
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			var messageBatch service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch, _, err = input.ReadBatch(ctx)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			// Close connection
			if input.Client != nil {
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("sends the heartbeat when manual subscribe to i=2258", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			nodeIDStrings := []string{"i=2258"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: true,
				UseHeartbeat:     true,
				HeartbeatNodeId:  ua.NewNumericNodeID(0, 2258),
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			var messageBatch service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch, _, err = input.ReadBatch(ctx)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(2))

			// Close connection
			if input.Client != nil {
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("does not disconnect if the heartbeat comes in in regular intervals", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			var nodeIDStrings []string
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: true,
				UseHeartbeat:     true,
				HeartbeatNodeId:  ua.NewNumericNodeID(0, 2258),
			}

			// Attempt to connect
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())

			var messageBatch service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch, _, err = input.ReadBatch(ctx)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			time.Sleep(5 * time.Second)

			var messageBatch2 service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch2, _, err = input.ReadBatch(ctx)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch2)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			time.Sleep(5 * time.Second)

			var messageBatch3 service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch3, _, err = input.ReadBatch(ctx)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch3)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			// Close connection
			if input.Client != nil {
				err = input.Close(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("does disconnect if the heartbeat does not come in regular intervals", func() {

			var nodeIDStrings []string
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: true,
				UseHeartbeat:     true,
				HeartbeatNodeId:  ua.NewNumericNodeID(0, 2259), // 2259 is State, which will not change
			}

			// Attempt to connect
			ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel1()
			err := input.Connect(ctx1)
			Expect(err).NotTo(HaveOccurred())

			ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel2()
			var messageBatch service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch, _, err = input.ReadBatch(ctx2)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			time.Sleep(5 * time.Second)

			ctx3, cancel3 := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel3()
			var messageBatch2 service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch2, _, err = input.ReadBatch(ctx3)
				if err != nil {
					// Log the error but continue - it might succeed on next attempt
					GinkgoT().Logf("ReadBatch error (might be temporary): %v", err)
					return -1
				}
				return len(messageBatch2)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(0))

			time.Sleep(10 * time.Second)

			ctx4, cancel4 := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel4()
			var messageBatch3 service.MessageBatch
			Eventually(func() int {
				var err error
				messageBatch3, _, err = input.ReadBatch(ctx4)
				// Expect err to be service.ErrNotConnected
				Expect(err).To(Equal(service.ErrNotConnected))
				return len(messageBatch3)
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(0))

			// Close connection
			if input.Client != nil {
				ctx6, cancel6 := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel6()
				err = input.Close(ctx6)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	When("using a custom poll rate", func() {
		It("should respect the configured poll interval", func() {
			// Create a new stream builder
			builder := service.NewStreamBuilder()

			// Create a new stream with 10 second poll rate on the CurrentTime node
			err := builder.AddInputYAML(`
opcua:
  endpoint: "opc.tcp://localhost:50000"
  username: ""
  password: ""
  subscribeEnabled: false
  useHeartbeat: false
  pollRate: 10000
  nodeIDs:
    - "i=2258"
`)

			Expect(err).NotTo(HaveOccurred())

			err = builder.SetLoggerYAML(`level: off`)
			Expect(err).NotTo(HaveOccurred())

			err = builder.SetTracerYAML(`type: none`)
			Expect(err).NotTo(HaveOccurred())

			// Track message timestamps
			var timestamps []time.Time
			var timestampsMutex sync.Mutex

			err = builder.AddConsumerFunc(func(c context.Context, m *service.Message) error {
				timestampsMutex.Lock()
				timestamps = append(timestamps, time.Now())
				timestampsMutex.Unlock()
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			stream, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			// Run the stream for enough time to get multiple messages
			ctx, cncl := context.WithTimeout(context.Background(), 25*time.Second)
			defer cncl()

			go func() {
				_ = stream.Run(ctx)
			}()

			// Wait until we have at least 2 messages
			Eventually(func() int {
				timestampsMutex.Lock()
				defer timestampsMutex.Unlock()
				return len(timestamps)
			}, 25*time.Second).Should(BeNumerically(">=", 2))

			// Check intervals between messages
			timestampsMutex.Lock()
			defer timestampsMutex.Unlock()

			for i := 1; i < len(timestamps); i++ {
				interval := timestamps[i].Sub(timestamps[i-1])
				// Allow for some timing variation (between 9 and 11 seconds)
				Expect(interval.Seconds()).To(BeNumerically("~", 10.0, 1.0),
					"Expected ~10 second interval, got %v seconds", interval.Seconds())
			}
		})
	})

	Context("When browsing nodes with HasTypeDefinition references on a real OPC UA server", func() {
		It("should not browse HasTypeDefinition references when browsing DataAccess_AnalogType_Byte", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			nodeIDStrings := []string{"ns=6;s=DataAccess_AnalogType_Byte"}
			parsedNodeIDs := ParseNodeIDs(nodeIDStrings)

			input := &OPCUAInput{
				Endpoint:         "opc.tcp://localhost:50000",
				Username:         "",
				Password:         "",
				NodeIDs:          parsedNodeIDs,
				SubscribeEnabled: false,
			}

			// Connect to the server
			err := input.Connect(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer input.Close(ctx)

			// Create channels for browsing
			nodeChan := make(chan NodeDef, 100)
			errChan := make(chan error, 100)
			opcuaBrowserChan := make(chan NodeDef, 100)
			var wg TrackedWaitGroup

			// Browse the node
			wg.Add(1)
			wrapperNodeID := NewOpcuaNodeWrapper(input.Client.Node(parsedNodeIDs[0]))
			go Browse(ctx, wrapperNodeID, "", 0, input.Log, parsedNodeIDs[0].String(), nodeChan, errChan, &wg, opcuaBrowserChan)

			wg.Wait()
			close(nodeChan)
			close(errChan)
			close(opcuaBrowserChan)

			var nodes []NodeDef
			for nodeDef := range nodeChan {
				nodes = append(nodes, nodeDef)
			}

			var errs []error
			for err := range errChan {
				errs = append(errs, err)
			}

			// We expect no errors
			Expect(errs).Should(BeEmpty())

			// We expect only osix node (the analog node itself and all of its properties) since HasTypeDefinition references should be ignored
			Expect(nodes).Should(HaveLen(6))
			Expect(nodes[0].NodeID.String()).To(Equal("ns=6;s=DataAccess_AnalogType_Byte"))
		})
	})
})

func checkDatatypeOfOPCUATag(dataType string, messageParsed interface{}, opcuapath string) {
	//GinkgoT().Logf("%s, %+v, %s", dataType, messageParsed, opcuapath)
	switch dataType {
	case "Boolean":
		// Assignable to bool
		Expect(messageParsed).To(BeAssignableToTypeOf(true), "DataType check for Boolean failed")
		//GinkgoT().Log("Received Boolean message: ", messageParsed)

	case "Byte", "Double", "Enumeration", "Float", "Int16", "Int32", "Int64", "Integer", "Number", "SByte", "StatusCode", "UInt16", "UInt32", "UInt64", "UInteger", "Duration":
		// Assignable to json.number
		Expect(messageParsed).To(BeAssignableToTypeOf(json.Number("")), "DataType check for Byte failed")
		//GinkgoT().Log("Received message: ", dataType, messageParsed)

	case "DateTime", "NodeId", "String", "ByteArray", "ByteString", "LocaleId", "UtcTime", "XmlElement":
		// Assignable to string
		Expect(messageParsed).To(BeAssignableToTypeOf("12fff3"), "DataType check for DateTime failed")
		//GinkgoT().Log("Received DateTime message: ", messageParsed)
	case "ExpandedNodeId":
		// ExpandedNodeId is expected to be a map[string]interface{}
		parsedMap, ok := messageParsed.(map[string]interface{})
		Expect(ok).To(BeTrue(), "Expected messageParsed to be of type map[string]interface{}")

		// Define the expected keys for ExpandedNodeId
		expectedKeys := []string{"NamespaceURI", "NodeID", "ServerIndex"}
		for _, key := range expectedKeys {
			_, exists := parsedMap[key]
			Expect(exists).To(BeTrue(), fmt.Sprintf("Expected key %s missing in messageParsed", key))
		}

		//GinkgoT().Log("Received ExpandedNodeId message: ", messageParsed)

	case "Guid", "LocalizedText", "QualifiedName":
		// These types are expected to be map[string]interface{} with specific keys
		parsedMap, ok := messageParsed.(map[string]interface{})
		Expect(ok).To(BeTrue(), fmt.Sprintf("Expected messageParsed to be of type map[string]interface{} for %s", dataType))

		var expectedKeys []string
		switch dataType {
		case "Guid":
			expectedKeys = []string{"Data1", "Data2", "Data3", "Data4"}
		case "LocalizedText":
			expectedKeys = []string{"EncodingMask", "Locale", "Text"}
		case "QualifiedName":
			expectedKeys = []string{"NamespaceIndex", "Name"}
		}

		for _, key := range expectedKeys {
			_, exists := parsedMap[key]
			Expect(exists).To(BeTrue(), fmt.Sprintf("Expected key %s missing in messageParsed for %s", key, dataType))
		}

		//GinkgoT().Logf("Received %s message: %+v", dataType, messageParsed)

	case "Variant":
		// Variant is expected to be a map[string]interface{}, but without specified keys
		_, ok := messageParsed.(map[string]interface{})
		Expect(ok).To(BeTrue(), "Expected messageParsed to be of type map[string]interface{} for Variant")
		//GinkgoT().Log("Received Variant message: ", messageParsed)

	default:
		Fail(fmt.Sprintf("Unsupported data type in OPC UA path: %s:%s", dataType, opcuapath))
	}
}
