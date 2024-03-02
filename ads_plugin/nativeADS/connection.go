package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/rs/zerolog/log"
	"go.uber.org/atomic"
)

type Connection struct {
	ip   string
	port int

	connection  net.Conn
	target      AmsAddress
	source      AmsAddress
	sendChannel chan []byte

	symbols             map[string]*Symbol
	activeNotifications map[uint32]*Symbol
	symbolLock          sync.Mutex

	datatypes map[string]SymbolUploadDataType
	ctx       context.Context
	shutdown  context.CancelFunc
	waitGroup sync.WaitGroup

	// List of active requests that waits a response, invokeid is key and value is a channel to the request rutine
	currentRequest    atomic.Uint32
	activeRequestLock sync.Mutex
	activeRequests    map[uint32]chan []byte

	systemResponse chan []byte
}

// type requestResponse struct {
// 	id       atomic.Uint32
// 	response map[uint32]chan []byte
// }

// NewConnection blah blah blah
func NewConnection(ctx context.Context, ip string, port int, netid string, amsPort int, localNetID string, localPort int) (conn *Connection, err error) {
	conn = &Connection{ip: ip, port: port}
	conn.ip = ip
	conn.port = port
	conn.target.NetID = stringToNetID(netid)
	conn.target.Port = uint16(amsPort)
	conn.source.NetID = stringToNetID(localNetID)
	conn.source.Port = uint16(localPort)
	conn.systemResponse = make(chan []byte)
	conn.activeRequests = map[uint32]chan []byte{}
	// for i := CommandID(0); i < 10; i++ {
	// 	conn.activeRequests[i] = &requestResponse{
	// 		response: map[uint32]chan []byte{},
	// 	}
	// }
	conn.activeNotifications = make(map[uint32]*Symbol)
	conn.sendChannel = make(chan []byte)
	conn.ctx, conn.shutdown = context.WithCancel(ctx)
	return
}

func (conn *Connection) Connect(local bool) error {
	var err error
	log.Info().
		Msg("TEsting DEBUG!")
	log.Debug().
		Msgf("Dailing ip: %s NetID: %d", conn.ip, conn.port)
	if local {
		conn.target.NetID = [6]byte{127, 0, 0, 1, 1, 1}
		conn.ip = "127.0.0.1"
	}
	conn.connection, err = net.Dial("tcp", fmt.Sprintf("%s:%d", conn.ip, conn.port))
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error connecting")
		return err
	}
	log.Trace().
		Msgf("Connected")
	go conn.listen()
	go conn.transmitWorker()
	if local {
		resp, _ := conn.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
		buf := bytes.NewBuffer(resp)
		result := AmsAddress{}
		log.Trace().
			Bytes("stuff", buf.Bytes()).Msg("got stuff")
		err = binary.Read(buf, binary.LittleEndian, &result)
		log.Info().
			Msgf("result %d", result)
		if err != nil {
			log.Error().
				Msgf("ERROR %v", err)
		}
		conn.source = result
	}
	res, err := conn.GetSymbolUploadInfo()
	if err != nil {
		log.Error().
			Err(err).
			Msgf("ERROR %v", err)
	}
	datatypesResponse, err := conn.GetUploadSymbolInfoDataTypes(res.DataTypeLength)
	if err != nil {
		log.Error().
			Err(err).
			Msgf("ERROR %v", err)
	}
	datatypes, err := ParseUploadSymbolInfoDataTypes(datatypesResponse)
	if err != nil {
		log.Error().
			Err(err).
			Msgf("ERROR %v", err)
	}
	conn.datatypes = datatypes
	symbolsResponse, err := conn.GetUploadSymbolInfoSymbols(res.SymbolLength)
	if err != nil {
		log.Error().
			Err(err).
			Msgf("ERROR %v", err)
	}
	symbols, err := ParseUploadSymbolInfoSymbols(symbolsResponse, datatypes)
	if err != nil {
		log.Error().
			Err(err).
			Msgf("ERROR %v", err)
	}
	conn.symbols = symbols
	return nil
}

// Close closes connection and waits for completion
func (conn *Connection) Close() {
	log.Info().
		Msg("CLOSE is called")
	log.Info().
		Msg("Sending shutdown to workers")
	for handle := range conn.activeNotifications {
		conn.DeleteDeviceNotification(handle)
		log.Info().
			Uint32("handle", handle).
			Msg("Removed Notification handle")
	}
	for _, symbol := range conn.symbols {
		if symbol.Handle != 0 {
			log.Info().
				Uint32("handle", symbol.Handle).
				Msg("Handle deleted")
			handleBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(handleBytes, symbol.Handle)
			conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes)
		}
	}
	conn.shutdown()
	log.Info().
		Msg("Waiting for workers to close")
	conn.waitGroup.Wait()
	log.Info().
		Msg("Close DONE")
	conn.connection.Close()
}
