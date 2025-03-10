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
		endpoint    string
		username    string
		password    string
		fingerprint string
		input       *OPCUAInput
		ctx         context.Context
		cancel      context.CancelFunc
	)

	BeforeEach(func() {
		endpoint = os.Getenv("TEST_KEPWARE_ENDPOINT")
		username = os.Getenv("TEST_KEPWARE_USERNAME")
		password = os.Getenv("TEST_KEPWARE_PASSWORD")
		fingerprint = os.Getenv("TEST_KEPWARE_FINGERPRINT")

		if endpoint == "" || username == "" || password == "" {
			Skip("Skipping test: environmental variables are not set")
		}

		ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	})

	AfterEach(func() {
		if input != nil && input.Client != nil {
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
		input.ServerCertificates = make(map[*ua.EndpointDescription]string)

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
			NodeIDs:          nil,
			SubscribeEnabled: false,
			OPCUAConnection: &OPCUAConnection{
				AutoReconnect:              true,
				ReconnectIntervalInSeconds: 5,
			},
		}, false, nil, false),
		Entry("should connect in no security mode", &OPCUAInput{
			NodeIDs:          nil,
			SubscribeEnabled: false,
			OPCUAConnection: &OPCUAConnection{
				SecurityMode:   "None",
				SecurityPolicy: "None",
			},
		}, false, nil, false),
		Entry("should connect with correct credentials", &OPCUAInput{
			NodeIDs: nil,
			OPCUAConnection: &OPCUAConnection{
				Username: username,
				Password: password,
			},
		}, false, nil, false),
		Entry("should fail to connect using incorrect credentials", &OPCUAInput{
			NodeIDs: nil,
			OPCUAConnection: &OPCUAConnection{
				Username: "123",
				Password: "123",
			},
		}, true, nil, false),
		Entry("should check if message-value is 123", &OPCUAInput{
			NodeIDs: ParseNodeIDs([]string{"ns=2;s=Tests.TestDevice.testConstData"}),
			OPCUAConnection: &OPCUAConnection{
				AutoReconnect:              true,
				ReconnectIntervalInSeconds: 5,
			},
		}, false, json.Number("123"), false),
		Entry("should return data changes on subscribe", &OPCUAInput{
			NodeIDs: ParseNodeIDs([]string{"ns=2;s=Tests.TestDevice.testChangingData"}),
			OPCUAConnection: &OPCUAConnection{
				AutoReconnect:              true,
				ReconnectIntervalInSeconds: 5,
			},
		}, false, nil, true),
	)

	// This should successfully connect to Kepware since we already trusted the
	// generated certificates.
	DescribeTable("Selecting a custom SecurityPolicy", func(input *OPCUAInput) {
		// attempt to connect with securityMode and Policy
		input.Endpoint = endpoint
		input.ServerCertificates = make(map[*ua.EndpointDescription]string)
		input.ServerCertificateFingerprint = fingerprint
		err := input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

	},
		Entry("should connect via Basic256Sha256 SignAndEncrypt", &OPCUAInput{
			OPCUAConnection: &OPCUAConnection{
				SecurityMode:      "SignAndEncrypt",
				SecurityPolicy:    "Basic256Sha256",
				ClientCertificate: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURtakNDQW9LZ0F3SUJBZ0lRZi8vLy8vLy8vLytIN0tHYXl3ZVQxekFOQmdrcWhraUc5dzBCQVFzRkFEQTMKTVF3d0NnWURWUVFLRXdOVlRVZ3hKekFsQmdOVkJBTVRIbUpsYm5Sb2IzTXRkVzFvTFhCeVpXUmxabWx1WldRdApkbVE0YkVSa05UQWVGdzB5TlRBeE1ERXdNREF3TURCYUZ3MHpOREV5TXpBd01EQXdNREJhTURjeEREQUtCZ05WCkJBb1RBMVZOU0RFbk1DVUdBMVVFQXhNZVltVnVkR2h2Y3kxMWJXZ3RjSEpsWkdWbWFXNWxaQzEyWkRoc1JHUTEKTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF2U3kxWUYrVXZwMExIQTkxMmlTUgpBMXQreGVTMmxqYzVLM29GZkUycDNKSnNzYWYwNlZRcWhhdGRoUDhrVjNWa0hQNEQweUVab2o3UDlXbFpIc2JOCjV5aGFDWFFoWTRwRUZ2S1E4RWdzNjRlMEcva2htaVFTSnIvSlJoTDZDOEwwaDZwNnhZckxBVlBmc0I2M3MvTisKQmFpNGV4SDJYVktJTE9DTXdDV3NKRFBNYWZnR1pDTllpcFk5cmVUUHEyVFhRNXBxR3NPdlkySDdmRjRwLy9zbQpSS1J6SUsyOS9oWExGNGh0dXZ2Q0ZucnVyQjh6TldHK01LK2NYUERNM1JxNHQyWndnckR2TTJ2NmlrbUQrNm4vCkhTRk9LOFRaWDl4QlBBR2Q2R1I2cDN4ck01MEwwZHg0eHBqUmdKL1dvUjJZbWVRUzFhUUcwRnpDY3huWkMzTloKbHdJREFRQUJvNEdoTUlHZU1BNEdBMVVkRHdFQi93UUVBd0lDOURBZEJnTlZIU1VFRmpBVUJnZ3JCZ0VGQlFjRApBUVlJS3dZQkJRVUhBd0l3REFZRFZSMFRBUUgvQkFJd0FEQmZCZ05WSFJFRVdEQldnaWwxY200NlltVnVkR2h2CmN5MTFiV2c2WTJ4cFpXNTBMWEJ5WldSbFptbHVaV1F0ZG1RNGJFUmtOWVlwZFhKdU9tSmxiblJvYjNNdGRXMW8KT21Oc2FXVnVkQzF3Y21Wa1pXWnBibVZrTFhaa09HeEVaRFV3RFFZSktvWklodmNOQVFFTEJRQURnZ0VCQUdYagpzdUp3ajBPakQ5WW9QTnJIQlNWNjVqRGV3Q3V2L0ZOUHRqSGNnSDZMZ1RONlFDVFNyUkQxWFVxdzB0RnYzV2tyCkkrUGVvODBUb0h6c1VSTnhSUFlxQmVqakZtTmF2T3FLem1kWHlRNEIvNzA0TFJEYjRyQWduTDh6Nk5SQzY1SkwKSlRHMW1DdXdVeWlwWGtHL1FTQk5iS2lITXpaeFdqbjd3K1RJanNRSFhyTml0RG5oYnk3RWpsS3VBWWpTWVFZNQpJVlJmRnNSRUQ2c0hWcExxZEJPZHQyNDFUbGhyUjM3YkkyaDFRL3lmeGdMNWV0MzcrOWV6aW9qYS9SRm5TWmh4CmJjMS83NlFDWGowVCt6T3h2MjQ2TnF4VTArUDBTVW9XV0kvRnkydEpLNVVXcTM0UmtGYkpTdUM3TEdJWkp1QlQKWktENC9jd1RvZWE3Tm9vZGYvWT0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQotLS0tLUJFR0lOIFJTQSBQUklWQVRFIEtFWS0tLS0tCk1JSUVwQUlCQUFLQ0FRRUF2U3kxWUYrVXZwMExIQTkxMmlTUkExdCt4ZVMybGpjNUszb0ZmRTJwM0pKc3NhZjAKNlZRcWhhdGRoUDhrVjNWa0hQNEQweUVab2o3UDlXbFpIc2JOCjV5aGFDWFFoWTRwRUZ2S1E4RWdzNjRlMEcva2htaVFTSnIvSlJoTDZDOEwwaDZwNnhZckxBVlBmc0I2M3MvTisKQmFpNGV4SDJYVktJTE9DTXdDV3NKRFBNYWZnR1pDTllpcFk5cmVUUHEyVFhRNXBxR3NPdlkySDdmRjRwLy9zbVJLUnpJSzI5L2hYTEY0aHR1dnZDRm5ydXJCOHoKTldHK01LK2NYUERNM1JxNHQyWndnckR2TTJ2NmlrbUQrNm4vSFNGT0s4VFpYOXhCUEFHZDZHUjZwM3hyTTUwTAowZHg0eHBqUmdKL1dvUjJZbWVRUzFhUUcwRnpDY3huWkMzTlpsd0lEQVFBQkFvSUJBQ1BwVGRFcXM5anZoUUZoCnFmU3NSbDhGeW00VXVkaVFTU2tJcyt2aDdtSHg1ZkpmdU8xbVRlQXNKTWV2aTUyU3FsdWFtTzFHZGxCSGJrRGYKSzh6YzNvK0lLSGRzOVQycExMM1NkRk00MEZZeDM2NER3QzQ3dExwb1kvUUtmQzhwWmpRdTE3bVNYSEUzRTlxaQpLaXRlQ01sWU94VVoxdFBtYS9WZzl6Y3VyNXY3YVhUK09pRFV1Q2JHSDFhZ1U1SG5yUituSUlWSkFMcWNyeTdiClIyUkZ3YmtTMGZtK0xHOEhQZTZqaDhoSElxQjZxeENxK21oNWNHUGhvQmdHRFRza3NQbzc0L3JsN2w5TE53a0sKTksxZU5PQy9sU0cxOG45T0VUZUxEajdFekpzbU9jbVhqcW1YYzZEeW9WeUkzeG4zVmVMKzhCNHE0ZjdWK1hSSAp0VUJwMDZFQ2dZRUE3MU01RmFjOU01UTMyaXJNNWFiMnBZcjFSd3pUb3grL2dNSDVteHYwT2MrVTZ0d295YzlECndEU1F2VjZFZlFWZXFpK0YwZEFVMGFGTHMxNjlWL3h3YTNUYW5IVDBIR21JWHM4NVZ4aFFPR3d4QVRMZGRZcG8KOVVDQ21QcTFIa3Z0blB6clA4NVlYQ21NcEFHYU40SDFKK2VZb0hmQjJoTUxvRUJUT3Bwc2gzRUNnWUVBeWxyMwpMTmVzU0NqaFFjVlJSM1FKSTY1UGFMaFl2bmhTL29OeFA4ZnFaMk4wd0Fwb0QxY3R5L2JWNUlId1ErYmhUWWlpCnlGcTAxMkV4dmwxSTd3K0FHSHN0V3JiM056aVB0V2ZkYXRyRnZUOUlJRldmYlhBb2ZQd0x6L0lyZnE1czVVNFEKSTBjN3A3b29xN0U1c0c0dml0ZmFBTEpQdVZsS3lqU1FEdG9TUFljQ2dZQW0zS3ErVzJQU3hsU3pkcFBERHZPcQpPZ0JPUTBUeWppczRxMGJ1NndFamloT3lkNEdnRTZuNndnNW0wYnhOMk50Z1kwc2xvTlpnbkFLQTQvZDNIQ1RkCkxpSjVtWHd5U09pK0RJUlJJaitVaWUwRE50RkRUdkJ3TXNPQ04rUEZRYXNaL08vdWNvRmlwZFNTcFRmM240REIKdEJmU3B0K3htN29ka1pSeVJiVXQ0UUtCZ1FDQ0U1ZHdLL09ETDBRZGswbDhOUXJxOU5IMjd3bWM1cHZ6SDJ0ZApKSlY1Z0dVOFRYUTI2RU40S1dPMVVCR3lsR2VmLzRVa1phcDZDUENBL2xZejFqTHhpYnpONDI5bS8rVGhKN01kClRTUnZVbzU2dW8rUk9kZk0vL2hYNDhReDJzNEZXUGptdEpPVWJnWlcxOFhOdEFhN3FhVnBiVFh5WDRQRUY2WEMKNWt0ellRS0JnUUNjeTU3aHdyLzFXQlFvSzc0akRDUmpWaXRXbVBOcytiQU9RMWtZRGp6WW5mSHhQZTVEZkxteAp0cnd1bHczOEZ1VUErelVhbmp6SCtDcXNQSHlRNUpOY1JDcStnRFlyTUxUV1B2VmVKSDUzS3JRTVZFUitzRDZ6CjltVy90VCtETUhrZHo0NGh0clBlNWdoTlRPeTBKVmV3T3c1bDBva1N6QW9SYXRhSi9KSnZqQT09Ci0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg==",
			},
		}),
		Entry("should connect via Basic256 SignAndEncrypt", &OPCUAInput{
			OPCUAConnection: &OPCUAConnection{
				SecurityMode:      "SignAndEncrypt",
				SecurityPolicy:    "Basic256",
				ClientCertificate: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURrakNDQW5xZ0F3SUJBZ0lJT3NUanI1NldMbEl3RFFZSktvWklodmNOQVFFRkJRQXdOekVNTUFvR0ExVUUKQ2hNRFZVMUlNU2N3SlFZRFZRUURFeDVpWlc1MGFHOXpMWFZ0YUMxd2NtVmtaV1pwYm1Wa0xVMXNhV3haWjBZdwpIaGNOTWpVd01UQXhNREF3TURBd1doY05NelF4TWpNd01EQXdNREF3V2pBNE1Rd3dDZ1lEClZRUUtFd05WVFVneEtEQW1CZ05WQkFNVEgySmxiblJvYjNNdGRXMW9MWEJ5WldSbFptbHVaV1F0VVhGdFVrTnYKU1Rnd2dnRWlNQTBHQ1NxR1NJYjNEUUVCQVFVQUE0SUJEd0F3Z2dFS0FvSUJBUURRdGxmd2NSRFJZQm5uZVNsKwp4NG11ZjN6VzZZUUI1bHo2TkRNQkVoSk5Bb1ByZHoybmNGeEVkWE9yc1FLd2toWHNLUzdVaTJWMEZ2akhWQWlpCkJmalBiT0dYS0F1YWV1OERxU1R6S1M4ZWQ5Vjh1YWJSUnAvL0dPR01XUi9VWFhoMkhkdEJqbHI5UFpaZis0SzkKU0JtRWNDeEtCUXVRV3VUK1dsSUJITlJCMXBXQmRRYU04cnB3a0x1NW1BTjFhKzd3R0w0RFBUZStJelJGZ2pHRgpRS3pxcitjbHNoUzRlVDZETWNyRmlpL3Q1amVObEw3UDFyKzAzSEUzTG81Umx3S2Z4MERtZ1dCL3hOUExocHc1ClB2bTV6bkZ5L21XOXAyenJJbHp3WHVIR25OZitsekQyMTd6MEphV0hPU0JXMG1HOWxPN1lEdTRKakx6VlJQNTYKK3l3aEFnTUJBQUdqZ2FNd2dhQXdEZ1lEVlIwUEFRSC9CQVFEQWdiQU1CMEdBMVVkSlFRV01CUUdDQ3NHQVFVRgpCd01CQmdnckJnRUZCUWNEQWpBTUJnTlZIUk1CQWY4RUFqQUFNR0VHQTFVZEVRUmFNRmlDS25WeWJqcGlaVzUwCmFHOXpMWFZ0YURwamJHbGxiblF0Y0hKbFpHVm1hVzVsWkMxUmNXMVNRMjlKT0lZcWRYSnVPbUpsYm5Sb2IzTXQKZFcxb09tTnNhV1Z1ZEMxd2NtVmtaV1pwYm1Wa0xWRnhiVkpEYjBrNE1BMEdDU3FHU0liM0RRRUJCUVVBQTRJQgpBUUNEcWlqZ1lUbE1Sd0owekVOR09aT2lBRVgvVEY1alNYY1hyQlpKNHozRW9LZXJsRG5WVHBsZUJPUTBXczF5CjNoVmJhUWVrWjlQNG9NTFdWTmN0dzhZR3dyV0lQdWNWUnpZZmF2VUxlazFpUGN1aGNoY2hzVmxYTElZcnFuY3cKSjY5OXp5MlVXUFNuOVNueGM4S2lDK3RHaFlCUzZCQlNKa2pwRE8rSUtXbzlBTWtEbmJweFM2UjBISjJySmxGUApSaVZLK1BRSjFTZzgwenp2UlJoeDM5N2VxRnlnRVFkMkhxcXNzSVp0RUlrWkpXc2w1ZnlVd3JDem9iWm1KK3FxCldtUzJxcUVUckEzN1R1b1pjd2NhWkNJUiswREs0TkRoK3lsL2FoYkc0ZENza21JNmNRa0daYVVrRW1xczdxaUgKQ0EzREdFcUs2MEF5TnhSOVpHcHoyWVJaCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcGdJQkFBS0NBUUVBMExaWDhIRVEwV0FaNTNrcGZzZUpybjk4MXVtRUFlWmMralF6QVJJU1RRS0Q2M2M5CnAzQmNSSFZ6cTdFQ3NKSVY3Q2t1MUl0bGRCYjR4MVFJb2dYNHoyemhseWdMbW5ydkE2a2s4eWt2SG5mVmZMbW0KMFVhZi94amhqRmtmMUYxNGRoM2JRWTVhL1QyV1gvdUN2VWdaaEhBc1NnVUxrRnJrL2xwU0FSelVRZGFWZ1hVRwpqUEs2Y0pDN3VaZ0RkV3Z1OEJpK0F6MDN2aU0wUllJeGhVQ3M2cS9uSmJJVXVIaytnekhLeFlvdjdlWTNqWlMrCno5YS90Tnh4Tnk2T1VaY0NuOGRBNW9GZ2Y4VFR5NGFjT1Q3NXVjNXhjdjVsdmFkczZ5SmM4RjdoeHB6WC9wY3cKOXRlODlDV2xoemtnVnRKaHZaVHUyQTd1Q1l5ODFVVCtldnNzSVFJREFRQUJBb0lCQVFDSXovNjFUbWlIMTBjagp4UGkrY201K1JIUEJMVEdyVFNhRm5OSVNVWlpOaE9pVTRZVTR6UjZ1Z1k5aGJKY214NXczUW9mQUsrQkZTUW1yCklCckltc0dPdHdEcDVRWTJMWCtnRnJCeDlQMCtLNjkySXZ5SEVwU3UwOUNGLzZZdHYxZkhsYXEwUG16R3RDaHUKb3FBQkU5SW1UcUc1bzdVQTI1UmdaelI2Smp1OUs0MlQzNUNLSlltUHRCYWJWcUxRUkxsY21ORnM5Z0l1czI2Ywo1aFprOXhiQ0VtVDdReTZ6eHlXTXl1dFZPUmlSSlJJTFJJYzBJMVFhNEdSc1dJNlExWm1YS2ljSzNJZUoxMEsxCmQvK3VjSlBWdXFWRWpZbVRXRThZai9BdkhrM0xDZjV1aDZpWEsyTVBZNG1BUldZSkRTRjBoQk8vRVo3MDAyYlkKMWs1bllMeEJBb0dCQU95d3d2aUdlYzl1aXlUQ1c3ZnZUa3QzNjAvR3g0NVNrNnJwTGVQWGhrL2x1c2VVYWl4egptcmNpbEJTLy9JbU52OW5pVjQzRFY3MDk4TlJnSGllY056ZFYyMS9LVjhMRlprRlUwTUVCNll0WHI1MUhCWE0rCnR4UXFjSXl0d2JYUjM4TkdUMlRMOXo5cjdXRW1oTnNrZGVsdVNHeUJKZU5EV2REZExFWHkwUHhMQW9HQkFPRzkKUXlTUVprM0E2bUVCRW91TjQ1eVl4YlVqVnZkSjN1RVRYTjFrYkUwN29YYlNXai9aWVpKcDlQK0lEYlh6ZUtFVApVT25HV3V1WHNTdXZnNXVSbUYzYVFDeXdOaGVLVXF5ejgvMkFkaDdzZVcrV2pRZEdsRHEyeU5XelVmYWFUanZ6CmMvNjFBZVV6NVZ3WWlQSkJtUE44TEZRc2k1M0J6SXdybVh6MGs1M0RBb0dCQU1jdk9lVEx4bGE2UUg1KzBaREkKNHFyQUVGYmlnYTFUOE5FenlscWpWWFNIYjlmbEhqU3FWTmtwVUZUbC9EQUdDaDNpVEt3UWFCWHB1bkgvTVdGRQpKWE43M1ZHUGhxdHlVOHRIQlNabjVaSk9DSXZpNk1ORFUrNjBpR0xiRnRsYjlXTHdHSUJLNVplSnpBcWx6OGhiCnRwN3JJQ2V2eDZLcFd6eFo1Smc0NzRaaEFvR0JBTlpYY3kyQXZ1TU5UbjhWR1pyUSsxSng3U2gxaWRuOGxsaDQKbmpESmJkeFh4cFNnWDNsSTIxQytzeGIvQktYRHJNS2xLS3NRNEx2YXFTdWwxLzBiWGVXZm1sZlVhVWdvMngrSgpMeCtCbnFiMk9zZ3QxM01WSFNJeTlMZVZNVHBLZ2dhQ3Y1MFdHZHFjVUNnR1UrSlRUdnZDNkFSMDE0elB1MzFNCjJNekJIbmtwQW9HQkFLTllSVUQvRkhJT1FhL3V1YWxzTTVBUEJPdkM3TGVmM0YyUjFTL0lVT0Z6Q20zQk0yMUYKRUlNdC9Dd1kwbDlnbHZIbmlnd2xNb05YZk9VTmdkZzM4elRPNEFSUWZGQmVMUHFhQ2JlOHBOZDJBQVdXMUwyUgpUNVpmSDZUekNWZFlDb2puKzg3NlhMZ1FCclgwNmNzWTVFZEk0NEk5TjRYSHQvTFJIZVg1alZzcwotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=",
			},
		}),
		Entry("should connect via Basic128Rsa15 Sign", &OPCUAInput{
			OPCUAConnection: &OPCUAConnection{
				SecurityMode:      "Sign",
				SecurityPolicy:    "Basic128Rsa15",
				ClientCertificate: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNtVENDQWdLZ0F3SUJBZ0lRRmVvcHhRSGdqREJxeDdScHNlVkkxREFOQmdrcWhraUc5dzBCQVFVRkFEQTQKTVF3d0NnWURWUVFLRXdOVlRVZ3hLREFtQmdOVkJBTVRIMkpsYm5Sb2IzTXRkVzFvTFhCeVpXUmxabWx1WldRdApObTV4VkZrMVVFTXdIaGNOTWpVd01UQXhNREF3TURBd1doY05NelF4TWpNd01EQXdNREF3V2pBNE1Rd3dDZ1lEClZRUUtFd05WVFVneEtEQW1CZ05WQkFNVEgySmxiblJvYjNNdGRXMW9MWEJ5WldSbFptbHVaV1F0Tm01eFZGazEKVUVNd2daOHdEUVlKS29aSWh2Y05BUUVCQlFBRGdZMEFNSUdKQW9HQkFPV1p4bzZqZHcvempFWUdJbkZQZWFETgo3N05PbWs3YVArZE9xQjZGN1lWOWRyRTJzRnVlSWZ2WDNIWGZUMk1zUXpFenlPTVgvUy9CZUJaQWYwemFyLytSCnZyd0k4MU1jaW8zUm5CS0ZGVDFSMzBWOVJRZmFjK0UrTnMwcy9Ya1lCTmNWMG4ybmxnQVlLYmZscmt1VTUzU0kKYmMzS3hLenlBeFA2VzJER3pFUGhBZ01CQUFHamdhTXdnYUF3RGdZRFZSMFBBUUgvQkFRREFnYkFNQjBHQTFVZApKUVFXTUJRR0NDc0dBUVVGQndNQkJnZ3JCZ0VGQlFjREFqQU1CZ05WSFJNQkFmOEVBakFBTUdFR0ExVWRFUVJhCk1GaUNLblZ5YmpwaVpXNTBhRzl6TFhWdGFEcGpiR2xsYm5RdGNISmxaR1ZtYVc1bFpDMHhaVGxxYzFkWWI0WXEKZFhKdU9tSmxiblJvYjNNdGRXMW9PbU5zYVdWdWRDMXdjbVZrWldacGJtVmtMVEZsT1dwelYxaHZNQTBHQ1NxRwpTSWIzRFFFQkJRVUFBNEdCQUQvYXphSW1jUnYwUTFMVy9WTDNhcXNLM2xoRVRLL0svYkwzMW1uMTFiREMwemllCnFUenZmTWIxTit0V1psQVNPSnlwQVRKb2pEaFNINVZqNisyUC9RMEZweTNmSDBWZXZiUi9LQXFBZzZoMnVkOCsKanBDMWswL1laZ2taY3dqTmRyVkppeWxocWcwNi91cVVtblNYWTdyZkpxdFFFZkswT0hzNmVpdEY2M3lmCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlDWEFJQkFBS0JnUURsbWNhT28zY1A4NHhHQmlKeFQzbWd6ZSt6VHBwTzJqL25UcWdlaGUyRmZYYXhOckJiCm5pSDcxOXgxMzA5akxFTXhNOGpqRi8wdndYZ1dRSDlNMnEvL2tiNjhDUE5USElxTjBad1NoUlU5VWQ5RmZVVUgKMm5QaFBqYk5MUDE1R0FUWEZkSjlwNVlBR0NtMzVhNUxsT2QwaUczTnlzU3M4Z01UK2x0Z3hzeEQ0UUlEQVFBQgpBb0dBRFZMR0ZlTGdkdG1BSzFRUnpaZDZERjNHNmhYR21JckxxSVdFOWZoNWx3UjN6Y0xKcXhkYkMzMDBPdGJSCmlZUzVCWExtMWw2Ky8zVnZuWUx5b0NnVWpGam5pSitLT3JiTzNYRkZDV2ozWU1OZU9GS3E3cGI2cmFicGNhNE8KMVVqcEV2OE9QOXVQamZVS3VRNFMzb0grT2VvVlhxSE5RMmR6NXpkUll0Q0pWaEVDUVFENW9HRDF4c3V1MUJFaQpLZ3N6SDk1ek0xUWpyRi9LMnE2dXFhV0tzSWFqUHRZRG01YWkrZW5JbDJIK1BDaGdLMnQxOElxMXNGR1RKb0xSCkxhbG9Feks5QWtFQTYzYUExWFNzaEVxeXpEbGU1YW1YZjhJY1BNU3AwUzNhdmV6V1djUjUyQ3lJR010NUd0MEMKODRHQURxRGg2MkQyenhac1p3SmVzQ2hpNW43MDkvMVo5UUpBWlp0MlhCUlREQktkOXI0T1dQejcyd0JsbXkrcQp2ak5OTHlNMmtzRlB6RnJqV2d5V3dEZmhoUmk1ZG5hZUtLY0Qwcm5hZkNJTTBreTJxdFpmUWxHdU1RSkFXeWRPCkt0UjNNT09tSWkrWGtEcytQaVJNUUM0Mk81ZVAxZlRJNm9tSVRlcTNhVG5rRVVVOExqNlU3NVRTd1FlUnBJdmUKdldZS2VCRzZiOWI0U01UWXZRSkJBTjBYemVkdnRWTGlFb0RHSkg2QUVxbXU4bDY5NzBWUWRvdXJibFFqWklsNQpJNU4xOXpHbzE4RjdYanBJcXZVK3Q0U212NXdXQy9MMEdJS21xUVdybnI0PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=",
			},
		}),
		Entry("should connect via Basic256Sha256 Sign", &OPCUAInput{
			OPCUAConnection: &OPCUAConnection{
				SecurityMode:      "Sign",
				SecurityPolicy:    "Basic256Sha256",
				ClientCertificate: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURrakNDQW5xZ0F3SUJBZ0lJRklpL0FNNTBPYWd3RFFZSktvWklodmNOQVFFTEJRQXdOekVNTUFvR0ExVUUKQ2hNRFZVMUlNU2N3SlFZRFZRUURFeDVpWlc1MGFHOXpMWFZ0YUMxd2NtVmtaV1pwYm1Wa0xXUkpjVVpRZEVjdwpIaGNOTWpVd01UQXhNREF3TURBd1doY05NelF4TWpNd01EQXdNREF3V2pBM01Rd3dDZ1lEVlFRS0V3TlZUVWd4Ckp6QWxCZ05WQkFNVEhtSmxiblJvYjNNdGRXMW9MWEJ5WldSbFptbHVaV1F0WkVseFJsQjBSekNDQVNJd0RRWUoKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBUE9xaEtxVmlhUy9UMGo4NEtFOENJQ1NRTkZkYTkyUApwNkhqdm9rMEszdS9mS0JTWGRUM0FacnZkcWlZOVM5My9UM3RlR1RJUmV6NTM5MFZCbEFyRGdLQW1Tdk1pTkZMCmpzZm1jUVcwdnhvSDRqbEY1T0djSllBT3BkQ3Rya2Vjc0ZldTRGQ1R3NERxUVN1d3BXb3laRkY0VVV2OG04ZmMKcWkxeWZPUStHNTAyam1scFhsRFFxa0dFNENiQ09NMHBsQTdrcXZ1enFEakFxcS9zVzVBYXVZV1k4aVdnaG52UgoweWpQR3lybVFCZzBUMEV6bzlVek9wQzUxT2YwUTVUMTdrWDhPUkpyZVFYbVhsOTRmKzdLS1hUWVRpNWpHVEpTClBHRDZUMmoxWWNoYWZoTENrSzc2Y08yRWpzRHNXSDhQM3d6dHg4Qm1sSFh2NWtxNTFjZC9MSjhDQXdFQUFhT0IKb1RDQm5qQU9CZ05WSFE4QkFmOEVCQU1DQXZRd0hRWURWUjBsQkJZd0ZBWUlLd1lCQlFVSEF3RUdDQ3NHQVFVRgpCd01DTUF3R0ExVWRFd0VCL3dRQ01BQXdYd1lEVlIwUkJGZ3dWb0lwZFhKdU9tSmxiblJvYjNNdGRXMW9PbU5zCmFXVnVkQzF3Y21Wa1pXWnBibVZrTFdSSmNVWlFkRWVHS1hWeWJqcGlaVzUwYUc5ekxYVnRhRHBqYkdsbGJuUXQKY0hKbFpHVm1hVzVsWkMxa1NYRkdVSFJITUEwR0NTcUdTSWIzRFFFQkN3VUFBNElCQVFCTGcvV05neHRFZXZ4WQphWHVQMFl6QW85Y25lUXIvZXNXc2tQRHRWYloxclM0Qit3YVBKM2d3eG13TmxBcS9FTE4xcklKS01mdDBXdFRwCkRFc2VnQnN5L0hjWkI5dS9aUythTittempERDR6MTRYOWNrUVhyVnZ1eXY2YnE2RFJhZjJ4UjJlV0tKYjJ2VUYKMUVYWjNaQVUwVkFWL1d5aTdySHR5T1pud3dwSWptOEJHanV3L1F1bGJUd0MrSDFsOGVQTkFncVB4K1dQeGRiVgppcEEvWmtwUVJmLy9MYnpMa0lYOWVnS3VQY2w4VnlTaDJ1ZDhCRFZUaERsVk1zeVlrRUtwMGpWcGxaVUFwWGNkCjQ2RnE5b3BaWnkyclVEOUpoNzgzbnVUUUI1YUtGbWJKSUtxZHI4ZVBnWUFibnUwbitPK0J1cEtza0c2S1pLOXIKSXdtSzZCc1gKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQotLS0tLUJFR0lOIFJTQSBQUklWQVRFIEtFWS0tLS0tCk1JSUVwZ0lCQUFLQ0FRRUE4NnFFcXBXSnBMOVBTUHpnb1R3SWdKSkEwVjFyM1krbm9lTytpVFFyZTc5OG9GSmQKMVBjQm11OTJxSmoxTDNmOVBlMTRaTWhGN1BuZjNSVUdVQ3NPQW9DWks4eUkwVXVPeCtaeEJiUy9HZ2ZpT1VYawo0WndsZ0E2bDBLMnVSNXl3VjY3Z1VKUERnT3BCSzdDbGFqSmtVWGhSUy95Yng5eXFMWEo4NUQ0Ym5UYU9hV2xlClVOQ3FRWVRnSnNJNHpTbVVEdVNxKzdPb09NQ3FyK3hia0JxNWhaanlKYUNHZTlIVEtNOGJLdVpBR0RSUFFUT2oKMVRNNmtMblU1L1JEbFBYdVJmdzVFbXQ1QmVaZVgzaC83c29wZE5oT0xtTVpNbEk4WVBwUGFQVmh5RnArRXNLUQpydnB3N1lTT3dPeFlmdy9mRE8zSHdHYVVkZS9tU3JuVngzOHNud0lEQVFBQkFvSUJBUUMvVVJhRFhaQktVNCtzCkRpbE5UM2FaaEx2eDV6a25LSUVGUW0wN0MwUk5FSWVJMWNNbzBKeHBDellPb0xKNmgrckZzZXZDcmVFQmRSeEoKV1JXdzRtMUlsN0lzU3BidVJqWWdUSlpYVlpocWx1QVArZ29BL25vUE52RUlqU1gyd2xkUE1WYWN6YmhHUXlmUwpad3NwKzlENmlzN0NLK0FncCtqU2NEOFJjcFo1ODNEYUV0OVByYWkzRXpOSGN0ZWxCb1hkK2g2OHU2UFA3Q2FCCmNmS1pDSHNGdUhiODlTRGx5NTA0RUZlUjBEcTJYaDViYjQvSk1oZEcrQjJVZkI4ZnYvQS9TVHpsN3dHTmNmOVkKeTlDc2Zrb1VFT2RjL1JtM09NcS80dklHRjZFZ0hZcVR0Wmc0cHdRY1R0YkJRRzh0RGFVNWdkZ09tVnpHbWhUZApTMmc1ekFpcEFvR0JBUGNqbC9YMWZGT0tpZHNkRWpSMUpRY0tuNmhnSjdBdFpqdkRKckowRFNLOWRJeS8ycDZtCnk0YW1UR0ZDeVYzUEJEemlzcXU4K1VrdTU3UjBsblU4cHkvNFpuMmdKTXlFeVNMWUdLMG8reDRlODdmcjZIVG8KYmNqbmFJWWZIVTBLYzgwRkhEc044MlRpbWNTdEpBTDNLK1RSMk1ua01HWXR1cmJuc0FnUjlVRmpBb0dCQVB4bgpEQzVDdDllaEJyTUVMTFNqdWhjVGdNKzZqZUd3VWdTMXhxRm9pS2FuOTF2TnVJRHJTUW51UExNVWhxRm9sMHVFCi92aDFnanVRZitIQW11K25SVnNDcGd0ZWpqZWc5aW9vTTROdDdoaDJmVWlKRFhEOVViZSthbG1FOEcxL1lHd1EKT0pJZnhtNjhVbWl2aU9rSy9reUlLM0kwT1FlVTNScmFZZ1NsdU1xVkFvR0JBS29uaFg3QXNBTE14YkZveUpuNQpkYTd0YjVONzhKZHFDcE5tLzRPcVIwajk2L3JrTk44NnM3SlhXUXMxOG5KMkQ5TGp1bVJuemJMSGFweGlFUjFTCnowRkY0enJuUWE3V1ZhMTUzek9KbmF2Vlg3UG41cnBuTlA4MzVFMURxdWJhSlhTbzZoRDJ2L2RiMU1jRzlsNWMKVTRwTnVOYjRGeUtpcFlVODY0UUM2VHRGQW9HQkFMclBva3BUQXpMTGNlUHBnUlVwNFQvWEpZMmFMQTBRcllQaQo3bUxrdGM5em5qY0thamF3N2x3MVFpUGhXMHYzMTVNRG1wdFdqYzk4UWNwYS9kSEc0aVJjTDMxV3ZMZ2JvRmJQCmJtRW1hZ1VkSGRrajV4anJ4U0hVQTI3Q1lCa0xIOWlib09NMk5XNGNZSG92QVl4S29MRVFUK2UxRTBpek8zcjIKaTNtWkZzNkpBb0dCQUlVd3REUnNVcytZckZSOFZHYjRSNnFtM3ZPSWRaTHQrM2FtcEx0ai9mTHBCMmJ3UUhDOQpuT2tjZ1pKbytHcnJOVDdmRWRpSnlIQ3pObVRYRXBTVUs4TzRtK2h5YlRXMDNIT1lDRkp2MENnNEkzVG14L0YxCk1zUkkxSHlJVnVsVFZWV2FjZ0EzeUsrV29vTkdnSklTcEd5UitqUkFNeUZjS1c3V29qcUVVMXhiCi0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg==",
			},
		}),
		Entry("should connect via Basic256 Sign", &OPCUAInput{
			OPCUAConnection: &OPCUAConnection{
				SecurityMode:      "Sign",
				SecurityPolicy:    "Basic256",
				ClientCertificate: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNtVENDQWdLZ0F3SUJBZ0lRRmVvcHhRSGdqREJxeDdScHNlVkkxREFOQmdrcWhraUc5dzBCQVFVRkFEQTQKTVF3d0NnWURWUVFLRXdOVlRVZ3hLREFtQmdOVkJBTVRIMkpsYm5Sb2IzTXRkVzFvTFhCeVpXUmxabWx1WldRdApObTV4VkZrMVVFTXdIaGNOTWpVd01UQXhNREF3TURBd1doY05NelF4TWpNd01EQXdNREF3V2pBNE1Rd3dDZ1lEClZRUUtFd05WVFVneEtEQW1CZ05WQkFNVEgySmxiblJvYjNNdGRXMW9MWEJ5WldSbFptbHVaV1F0Tm01eFZGazEKVUVNd2daOHdEUVlKS29aSWh2Y05BUUVCQlFBRGdZMEFNSUdKQW9HQkFPV1p4bzZqZHcvempFWUdJbkZQZWFETgo3N05PbWs3YVArZE9xQjZGN1lWOWRyRTJzRnVlSWZ2WDNIWGZUMk1zUXpFenlPTVgvUy9CZUJaQWYwemFyLytSCnZyd0k4MU1jaW8zUm5CS0ZGVDFSMzBWOVJRZmFjK0UrTnMwcy9Ya1lCTmNWMG4ybmxnQVlLYmZscmt1VTUzU0kKYmMzS3hLenlBeFA2VzJER3pFUGhBZ01CQUFHamdhTXdnYUF3RGdZRFZSMFBBUUgvQkFRREFnYkFNQjBHQTFVZApKUVFXTUJRR0NDc0dBUVVGQndNQkJnZ3JCZ0VGQlFjREFqQU1CZ05WSFJNQkFmOEVBakFBTUdFR0ExVWRFUVJhCk1GaUNLblZ5YmpwaVpXNTBhRzl6TFhWdGFEcGpiR2xsYm5RdGNISmxaR1ZtYVc1bFpDMHhaVGxxYzFkWWI0WXEKZFhKdU9tSmxiblJvYjNNdGRXMW9PbU5zYVdWdWRDMXdjbVZrWldacGJtVmtMVEZsT1dwelYxaHZNQTBHQ1NxRwpTSWIzRFFFQkJRVUFBNEdCQUFTL3psQWk1bGIvcUVYdXRpdHltaXlaUGFFVC9JZDdsdEl3MlNzUzZ2aEdFRW1ZClJGQmxaNjhjVk40T3BPY1pBYVV1RW9GVEJ0RDYxamVuemlQRnp3ekFFeEJmY2h3VmN2dU5LMTR1R29mNWJvcXEKY1FvVDMweDVKWHhvMHJlNVV0bDd6TkZDZkJoUXl1NjRsamJyRkwvd2hPdXNMV2RwTitOTTdaOG8yMFE1Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlDWFFJQkFBS0JnUURGN2ZjM0JpT3JYalBKMlczT3c5MnNQOTVTb0swZHFzalFrZTZvNnBIanJiZVRHMVZ6CmhFK25GYkJtM3J4TG1NVnlZLzF5SnJHODB3aE9tQUttbHZ3anVIN2ozSzFoeVJhZEZxOHdKTENZdmp4NTI4OVEKZGRVRjZ4eDMvcFliTllVc1VqQ3NlQzlRdEhGU3dJVnoybW01cVJDQmU4YmNDcEwxVzNVZzAzLzh1d0lEQVFBQgpBb0dBVkFwMXBKeHJ1dERWNW9mMjB3dGhiVWoxS2xwbEJ5ckQ5Nk52RmJQNzNCT2YxY3VPWGUwNU1QWEpuL1JGCk9VZ1YxanRVbXdxSS8yY1BxT2RzZ0xXdE92SWFzWHlEODR6QUM2NVlUd1JUdFh2SWhOOTRIb2p5RFhBUHl5YmUKWmNoSS9nYmZlR0xFYWNzZUd2anI2TU80MUNrYkhpZlQ1eko4VTM3akhhbzdpUWtDUVFEdWVWUGhxcGZCcVhqNQpiY0RNRFZ1VUtZZ1hrcVJsY2lOODJOSnUvajdzK3lyWXNIVXh4eXpqVXhvUUpydWV1YVJDVjg4azV6b1FwbDRDCi9TeXJ3SUwxQWtFQTFIblUvRXQ1c3pLWEdRTy9aV3NtYldnVTRjU2lhYlcrSGxOd3pjSjhZM3l3NEphTTUxUnIKZCtzd25JSko0SUVRcFViZ0pTaHBTeGoySllGOUhmNlM3d0pBSUlCZ3Nrenh5ZTh6RWF1bnJ5ZlM1MnFScGNUUwpxeERYVFZpdnRYanBVcHNZeDllazRWZm9Ba045TmQ1Umk5eDVTcUYxRmU1OXQyODFPT3NRZjRnSlVRSkJBTTAxCjNiN09KbndzSUVMSW05SVg2c1ZBQU9zTVB0Qlo5NFRTa2VBb05ucmZzdlUyY2wvNTZOR3BGUW9UeThaSTRRcS8KcVR3NkMzZThZLzlWVU1IblhZRUNRUUNvKzkyYmYzbXBwSWJyMmVQUVhsSlEzMnNIRDRaWnZnR2dROWpkelJpdQpvWHJ5OTJ4UnZRVkZ4REN0UjY5blpYTEEyanBTOTJnWnZ0a1FqOGNtb0VEQgotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=",
			},
		}),
		Entry("should connect via Basic128Rsa15 Sign", &OPCUAInput{
			OPCUAConnection: &OPCUAConnection{
				SecurityMode:      "Sign",
				SecurityPolicy:    "Basic128Rsa15",
				ClientCertificate: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNtVENDQWdLZ0F3SUJBZ0lRRmVvcHhRSGdqREJxeDdScHNlVkkxREFOQmdrcWhraUc5dzBCQVFVRkFEQTQKTVF3d0NnWURWUVFLRXdOVlRVZ3hLREFtQmdOVkJBTVRIMkpsYm5Sb2IzTXRkVzFvTFhCeVpXUmxabWx1WldRdApObTV4VkZrMVVFTXdIaGNOTWpVd01UQXhNREF3TURBd1doY05NelF4TWpNd01EQXdNREF3V2pBNE1Rd3dDZ1lEClZRUUtFd05WVFVneEtEQW1CZ05WQkFNVEgySmxiblJvYjNNdGRXMW9MWEJ5WldSbFptbHVaV1F0Tm01eFZGazEKVUVNd2daOHdEUVlKS29aSWh2Y05BUUVCQlFBRGdZMEFNSUdKQW9HQkFPV1p4bzZqZHcvempFWUdJbkZQZWFETgo3N05PbWs3YVArZE9xQjZGN1lWOWRyRTJzRnVlSWZ2WDNIWGZUMk1zUXpFenlPTVgvUy9CZUJaQWYwemFyLytSCnZyd0k4MU1jaW8zUm5CS0ZGVDFSMzBWOVJRZmFjK0UrTnMwcy9Ya1lCTmNWMG4ybmxnQVlLYmZscmt1VTUzU0kKYmMzS3hLenlBeFA2VzJER3pFUGhBZ01CQUFHamdhTXdnYUF3RGdZRFZSMFBBUUgvQkFRREFnYkFNQjBHQTFVZApKUVFXTUJRR0NDc0dBUVVGQndNQkJnZ3JCZ0VGQlFjREFqQU1CZ05WSFJNQkFmOEVBakFBTUdFR0ExVWRFUVJhCk1GaUNLblZ5YmpwaVpXNTBhRzl6TFhWdGFEcGpiR2xsYm5RdGNISmxaR1ZtYVc1bFpDMHhaVGxxYzFkWWI0WXEKZFhKdU9tSmxiblJvYjNNdGRXMW9PbU5zYVdWdWRDMXdjbVZrWldacGJtVmtMVEZsT1dwelYxaHZNQTBHQ1NxRwpTSWIzRFFFQkJRVUFBNEdCQUQvYXphSW1jUnYwUTFMVy9WTDNhcXNLM2xoRVRLL0svYkwzMW1uMTFiREMwemllCnFUenZmTWIxTit0V1psQVNPSnlwQVRKb2pEaFNINVZqNisyUC9RMEZweTNmSDBWZXZiUi9LQXFBZzZoMnVkOCsKanBDMWswL1laZ2taY3dqTmRyVkppeWxocWcwNi91cVVtblNYWTdyZkpxdFFFZkswT0hzNmVpdEY2M3lmCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlDWEFJQkFBS0JnUURsbWNhT28zY1A4NHhHQmlKeFQzbWd6ZSt6VHBwTzJqL25UcWdlaGUyRmZYYXhOckJiCm5pSDcxOXgxMzA5akxFTXhNOGpqRi8wdndYZ1dRSDlNMnEvL2tiNjhDUE5USElxTjBad1NoUlU5VWQ5RmZVVUgKMm5QaFBqYk5MUDE1R0FUWEZkSjlwNVlBR0NtMzVhNUxsT2QwaUczTnlzU3M4Z01UK2x0Z3hzeEQ0UUlEQVFBQgpBb0dBRFZMR0ZlTGdkdG1BSzFRUnpaZDZERjNHNmhYR21JckxxSVdFOWZoNWx3UjN6Y0xKcXhkYkMzMDBPdGJSCmlZUzVCWExtMWw2Ky8zVnZuWUx5b0NnVWpGam5pSitLT3JiTzNYRkZDV2ozWU1OZU9GS3E3cGI2cmFicGNhNE8KMVVqcEV2OE9QOXVQamZVS3VRNFMzb0grT2VvVlhxSE5RMmR6NXpkUll0Q0pWaEVDUVFENW9HRDF4c3V1MUJFaQpLZ3N6SDk1ek0xUWpyRi9LMnE2dXFhV0tzSWFqUHRZRG01YWkrZW5JbDJIK1BDaGdLMnQxOElxMXNGR1RKb0xSCkxhbG9Feks5QWtFQTYzYUExWFNzaEVxeXpEbGU1YW1YZjhJY1BNU3AwUzNhdmV6V1djUjUyQ3lJR010NUd0MEMKODRHQURxRGg2MkQyenhac1p3SmVzQ2hpNW43MDkvMVo5UUpBWlp0MlhCUlREQktkOXI0T1dQejcyd0JsbXkrcQp2ak5OTHlNMmtzRlB6RnJqV2d5V3dEZmhoUmk1ZG5hZUtLY0Qwcm5hZkNJTTBreTJxdFpmUWxHdU1RSkFXeWRPCkt0UjNNT09tSWkrWGtEcytQaVJNUUM0Mk81ZVAxZlRJNm9tSVRlcTNhVG5rRVVVOExqNlU3NVRTd1FlUnBJdmUKdldZS2VCRzZiOWI0U01UWXZRSkJBTjBYemVkdnRWTGlFb0RHSkg2QUVxbXU4bDY5NzBWUWRvdXJibFFqWklsNQpJNU4xOXpHbzE4RjdYanBJcXZVK3Q0U212NXdXQy9MMEdJS21xUVdybnI0PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=",
			},
		}),
	)
})

// Here we are testing the underlying opc-clients, which are siemens s7 / wago
// they're connected via opc-ua as clients
// We verify that we are able to find their namespaceArrays and check for the
// correct namespace. On top of that we are reading static and changing data
// from the underlying S7-1200.

// FlakeAttempts were added due to unreliability of the plc-clients (wago / siemens),
// which are connected via OPC-UA to Kepware server.
var _ = Describe("Test underlying OPC-clients", FlakeAttempts(3), func() {
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
		if input != nil && input.Client != nil {
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
			OPCUAConnection: &OPCUAConnection{
				Endpoint:                   endpoint,
				Username:                   username,
				Password:                   password,
				ServerCertificates:         make(map[*ua.EndpointDescription]string),
				AutoReconnect:              true,
				ReconnectIntervalInSeconds: 5,
			},
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
		input.ServerCertificates = make(map[*ua.EndpointDescription]string)

		err := input.Connect(ctx)
		Expect(err).NotTo(HaveOccurred())

		// validate on the static and dynamic data from underlying s7-1200
		validateStaticAndChangingData(ctx, input, expectedValue, isChangingValue)
	},
		Entry("should check if message-value is true", &OPCUAInput{
			NodeIDs: ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.ServerInterfaces.Server _interface_1.test"}),
			OPCUAConnection: &OPCUAConnection{
				AutoReconnect:              true,
				ReconnectIntervalInSeconds: 5,
			},
		}, true, false),
		Entry("should return data changes on subscribe", &OPCUAInput{
			NodeIDs:          ParseNodeIDs([]string{"ns=2;s=SiemensPLC_main.main.ServerInterfaces.Server _interface_1.counter"}),
			SubscribeEnabled: true,
			OPCUAConnection:  &OPCUAConnection{},
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
