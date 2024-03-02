package ads

// AMSAddress netid and port of device
type AmsAddress struct {
	NetID [6]byte
	Port  uint16
}

// TransMode transmission mode for notifications
type TransMode uint32

const (
	TransModeNoTransmission TransMode = iota
	TransModeClientCycle
	TransModeClientOnChange
	TransModeServerCycle
	TransModeServerOnChange
	TransModeClient1Reqest
)

type AdsState uint16

const (
	AdsStateInvalid      AdsState = 0
	AdsStateIdle         AdsState = 1
	AdsStateReset        AdsState = 2
	AdsStateInit         AdsState = 3
	AdsStateStart        AdsState = 4
	AdsStateRun          AdsState = 5
	AdsStateStop         AdsState = 6
	AdsStateSaveCfg      AdsState = 7
	AdsStateLoadCfg      AdsState = 8
	AdsStatePowerFailure AdsState = 9
	AdsStatePowerGood    AdsState = 10
	AdsStateError        AdsState = 11
	AdsStateShutdown     AdsState = 12
	AdsStateSuspend      AdsState = 13
	AdsStateResume       AdsState = 14
	AdsStateConfig       AdsState = 15 // System Is In Config Mode
	AdsStateReconfig     AdsState = 16 // System Should Restart In Config Mode
	AdsStateMaxStates    AdsState = 255
)

// Port default twincat ports
type Port uint32

const (
	PortLogger    Port = 100
	PortR0Rtime   Port = 200
	PortR0Trace   Port = (PortR0Rtime + 90)
	PortR0Io      Port = 300
	PortR0Sps     Port = 400
	PortR0Nc      Port = 500
	PortR0Isg     Port = 550
	PortR0Pcs     Port = 600
	PortR0Plc     Port = 801
	PortR0PlcRts1 Port = 801
	PortR0PlcRts2 Port = 811
	PortR0PlcRts3 Port = 821
	PortR0PlcRts4 Port = 831
	PortR0PlcTc3  Port = 851
)

// CommandID Ads Command IDS
type CommandID uint16

const (
	CommandIDInValueID CommandID = iota
	CommandIDReadDeviceInfo
	CommandIDRead
	CommandIDWrite
	CommandIDReadState
	CommandIDWriteControl
	CommandIDAddDeviceNotification
	CommandIDDeleteDeviceNotification
	CommandIDDeviceNotification
	CommandIDReadWrite
)

// Group reserved index groups
type Group uint32

const (
	GroupSymbolTab   Group = 0xf000
	GroupSymbolName  Group = 0xf001
	GroupSymbolValue Group = 0xf002

	GroupSymbolHandleByName  Group = 0xF003
	GroupSymbolValueByName   Group = 0xF004
	GroupSymbolValueByHandle Group = 0xF005
	GroupSymbolReleaseHandle Group = 0xF006
	GroupSymbolInfoByName    Group = 0xF007
	GroupSymbolVersion       Group = 0xF008
	GroupSymbolInfoByNameEx  Group = 0xF009

	GroupSymbolDownload       Group = 0xF00A
	GroupSymbolUpload         Group = 0xF00B
	GroupSymbolUploadInfo     Group = 0xF00C
	GroupSymbolDownload2      Group = 0xF00D
	GroupSymbolDataTypeUpload Group = 0xF00E
	GroupSymbolUploadInfo2    Group = 0xF00F

	GroupSymbolNotification Group = 0xf010 // notification of named handle

	GroupSumupRead                     Group = 0xF080
	GroupSumupWrite                    Group = 0xF081
	GroupSumupReadWrite                Group = 0xF082
	GroupSumupReadEx                   Group = 0xF083
	GroupSumupReadEx2                  Group = 0xF084
	GroupSumupAddDeviceNotification    Group = 0xF085
	GroupSumupDeleteDeviceNotification Group = 0xF086

	GroupIoImageRwib   Group = 0xF020 // read/write input byte(s)
	GroupIoImageRwix   Group = 0xF021 // read/write input bit
	GroupIoImageRisize Group = 0xF025 // read input size (in byte)
	GroupIoImageRwob   Group = 0xF030 // read/write output byte(s)
	GroupIoImageRwox   Group = 0xF031 // read/write output bit
	GroupIoImageCleari Group = 0xF040 // write inputs to null
	GroupIoImageClearo Group = 0xF050 // write outputs to null
	GroupIoImageRwiob  Group = 0xF060 // read input and write output byte(s)

	GroupDeviceData Group = 0xF100 // state, name, etc...
)

type Offset uint32

const (
	OffsetDeviceDataAdsState    Offset = 0x0000 // ads state of device
	OffsetDeviceDataDeviceState Offset = 0x0002 // device state
)

// ReturnCode ADS Return codes
type ReturnCode uint32

// ReturnCodeErrorOffset Errors begin at this offset
const ReturnCodeErrorOffset = 0x0700

const (
	ReturnCodeNoErrors ReturnCode = 0x00

	ReturnCodeDeviceError                 ReturnCode = (0x00 + ReturnCodeErrorOffset) //Error Class < Device Error >
	ReturnCodeDeviceServiceNotSupported   ReturnCode = (0x01 + ReturnCodeErrorOffset) //Service Is Not Supported By Server
	ReturnCodeDeviceInvalidGroup          ReturnCode = (0x02 + ReturnCodeErrorOffset) //Invalid Indexgroup
	ReturnCodeDeviceInvalidOffset         ReturnCode = (0x03 + ReturnCodeErrorOffset) //Invalid Indexoffset
	ReturnCodeDeviceInvalidAccess         ReturnCode = (0x04 + ReturnCodeErrorOffset) //Reading/Writing Not Permitted
	ReturnCodeDeviceInvalidSize           ReturnCode = (0x05 + ReturnCodeErrorOffset) //Parameter Size Not Correct
	ReturnCodeDeviceInvalidData           ReturnCode = (0x06 + ReturnCodeErrorOffset) //Invalid Parameter Value(S)
	ReturnCodeDeviceNotReady              ReturnCode = (0x07 + ReturnCodeErrorOffset) //Device Is Not In A Ready State
	ReturnCodeDeviceBusy                  ReturnCode = (0x08 + ReturnCodeErrorOffset) //DeviceIs Busy
	ReturnCodeDeviceInvalidContext        ReturnCode = (0x09 + ReturnCodeErrorOffset) //InvalidContext (Must Be Inwindows)
	ReturnCodeDeviceNoMemory              ReturnCode = (0x0a + ReturnCodeErrorOffset) //OutOf Memory
	ReturnCodeDeviceInvalidParam          ReturnCode = (0x0b + ReturnCodeErrorOffset) //InvalidParameter Value(S)
	ReturnCodeDeviceNotFound              ReturnCode = (0x0c + ReturnCodeErrorOffset) //NotFound (Files, ...)
	ReturnCodeDeviceSyntax                ReturnCode = (0x0d + ReturnCodeErrorOffset) //SyntaxError In Comand Or File
	ReturnCodeDeviceIncompatible          ReturnCode = (0x0e + ReturnCodeErrorOffset) //ObjectsDo Not Match
	ReturnCodeDeviceExists                ReturnCode = (0x0f + ReturnCodeErrorOffset) //ObjectAlready Exists
	ReturnCodeDeviceSymbolNoFound         ReturnCode = (0x10 + ReturnCodeErrorOffset) //SymbolNot Found
	ReturnCodeDeviceSymbolVersionInvalid  ReturnCode = (0x11 + ReturnCodeErrorOffset) //SymbolVersion Invalid
	ReturnCodeDeviceInvalidState          ReturnCode = (0x12 + ReturnCodeErrorOffset) //ServerIs In Invalid State
	ReturnCodeDeviceTransModeNotSupported ReturnCode = (0x13 + ReturnCodeErrorOffset) //AdstransmodeNot Supported
	ReturnCodeDeviceNotifyHandleInvalid   ReturnCode = (0x14 + ReturnCodeErrorOffset) //NotificationHandle Is Invalid
	ReturnCodeDeviceClientUnknown         ReturnCode = (0x15 + ReturnCodeErrorOffset) //NotificationClient Not Registered
	ReturnCodeDeviceNoMoreHandles         ReturnCode = (0x16 + ReturnCodeErrorOffset) //NoMore Notification Handles
	ReturnCodeDeviceInvalidWatchSize      ReturnCode = (0x17 + ReturnCodeErrorOffset) //SizeFor Watch To Big
	ReturnCodeDeviceNotInitialized        ReturnCode = (0x18 + ReturnCodeErrorOffset) //DeviceNot Initialized
	ReturnCodeDeviceTimeout               ReturnCode = (0x19 + ReturnCodeErrorOffset) //DeviceHas A Timeout
	ReturnCodeDeviceNoInterface           ReturnCode = (0x1a + ReturnCodeErrorOffset) //QueryInterface Failed
	ReturnCodeDeviceInvalidInterface      ReturnCode = (0x1b + ReturnCodeErrorOffset) //WrongInterface Required
	ReturnCodeDeviceInvalidClsID          ReturnCode = (0x1c + ReturnCodeErrorOffset) //ClassId Is Invalid
	ReturnCodeDeviceInvalidObjID          ReturnCode = (0x1d + ReturnCodeErrorOffset) //ObjectId Is Invalid
	ReturnCodeDevicePending               ReturnCode = (0x1e + ReturnCodeErrorOffset) //RequestIs Pending
	ReturnCodeDeviceAborted               ReturnCode = (0x1f + ReturnCodeErrorOffset) //RequestIs Aborted
	ReturnCodeDeviceWarning               ReturnCode = (0x20 + ReturnCodeErrorOffset) //SignalWarning
	ReturnCodeDeviceInvalidArrayIndex     ReturnCode = (0x21 + ReturnCodeErrorOffset) //InvalidArray Index
	ReturnCodeDeviceSymbolNotActive       ReturnCode = (0x22 + ReturnCodeErrorOffset) //SymbolNot Active -> Release Handle And Try Again
	ReturnCodeDeviceAccessDenied          ReturnCode = (0x23 + ReturnCodeErrorOffset) //AccessDenied
	ReturnCodeDeviceLicenseNotFound       ReturnCode = (0x24 + ReturnCodeErrorOffset) //NoLicense Found
	ReturnCodeDeviceLicenseExpired        ReturnCode = (0x25 + ReturnCodeErrorOffset) //LicenseExpired
	ReturnCodeDeviceLicenseExceeded       ReturnCode = (0x26 + ReturnCodeErrorOffset) //LicenseExceeded
	ReturnCodeDeviceLicenseInvalid        ReturnCode = (0x27 + ReturnCodeErrorOffset) //LicenseInvalid
	ReturnCodeDeviceLicenseSystemID       ReturnCode = (0x28 + ReturnCodeErrorOffset) //LicenseInvalid System Id
	ReturnCodeDeviceLicenseNoTimeLimit    ReturnCode = (0x29 + ReturnCodeErrorOffset) //LicenseNot Time Limited
	ReturnCodeDeviceLicenseFutureIssue    ReturnCode = (0x2a + ReturnCodeErrorOffset) //LicenseIssue Time In The Future
	ReturnCodeDeviceLicenseTimeToLong     ReturnCode = (0x2b + ReturnCodeErrorOffset) //LicenseTime Period To Long
	ReturnCodeDeviceException             ReturnCode = (0x2c + ReturnCodeErrorOffset) //ExceptionIn Device Specific Code
	ReturnCodeDeviceLicenseDuplicated     ReturnCode = (0x2d + ReturnCodeErrorOffset) //LicenseFile Read Twice
	ReturnCodeDeviceSignatureInvalid      ReturnCode = (0x2e + ReturnCodeErrorOffset) //InvalidSignature
	ReturnCodeDeviceCertificateInvalid    ReturnCode = (0x2f + ReturnCodeErrorOffset) //PublicKey Certificate
	ReturnCodeClientError                 ReturnCode = (0x40 + ReturnCodeErrorOffset) //ErrorClass < Client Error >
	ReturnCodeClientInvalidParameter      ReturnCode = (0x41 + ReturnCodeErrorOffset) //InvalidParameter At Service Call
	ReturnCodeClientListEmpty             ReturnCode = (0x42 + ReturnCodeErrorOffset) //PollingList	Is Empty
	ReturnCodeClientVarUsed               ReturnCode = (0x43 + ReturnCodeErrorOffset) //VarConnection Already In Use
	ReturnCodeClientDuplicateInvokeID     ReturnCode = (0x44 + ReturnCodeErrorOffset) //InvokeId In Use
	ReturnCodeClientSyncTimeout           ReturnCode = (0x45 + ReturnCodeErrorOffset) //TimeoutElapsed
	ReturnCodeClientW32Error              ReturnCode = (0x46 + ReturnCodeErrorOffset) //ErrorIn Win32 Subsystem
	ReturnCodeClientTimeoutInvalid        ReturnCode = (0x47 + ReturnCodeErrorOffset) //? ReturnCodeClientPortNot Open ReturnCode = (0x48 + ReturnCodeErrorOffset)          //AdsDll
	ReturnCodeClientNoAmsAddress          ReturnCode = (0x49 + ReturnCodeErrorOffset) //AdsDll
	ReturnCodeClientSyncInternal          ReturnCode = (0x50 + ReturnCodeErrorOffset) //InternalError In Ads Sync
	ReturnCodeClientAddHash               ReturnCode = (0x51 + ReturnCodeErrorOffset) //HashTable Overflow
	ReturnCodeClientRemoveHash            ReturnCode = (0x52 + ReturnCodeErrorOffset) //KeyNot Found In Hash Table
	ReturnCodeClientNoMoreSymbols         ReturnCode = (0x53 + ReturnCodeErrorOffset) //NoMore Symbols In Cache
	ReturnCodeClientSyncResponseInvalid   ReturnCode = (0x54 + ReturnCodeErrorOffset) //InvalidResponse Received
	ReturnCodeClientSyncPortLocked        ReturnCode = (0x55 + ReturnCodeErrorOffset) //SyncPort Is Locked
)
