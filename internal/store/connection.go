package store

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"
)

import "go.uber.org/zap"

type CommandRequest struct {
	RequestId    uint64
	ResultFuture *Future[CommandResult]
}

type ClientMessageBatch []ClientMessage
type ServerMessageBatch []ServerMessage
type CommandRequestBatch []CommandRequest

type ConnectionConfig struct {
	ReadTimeout              time.Duration
	WriteTimeout             time.Duration
	MaxFrameSize             uint32
	CommandBufferDepth       uint32
	ClientMessageBufferDepth uint32
	ServerMessageBufferDepth uint32
	ReadBufferSize           uint32
	WriteBufferSize          uint32
}

type ConnectionClosedCb func(*Connection)

type Connection struct {
	conn            net.Conn
	store           *Store
	logger          *zap.Logger
	config          ConnectionConfig
	reader          FrameReader
	writer          FrameWriter
	clientCodec     ClientMessageCodec
	serverCodec     ServerMessageCodec
	pendingCommands chan CommandRequestBatch
	clientMessages  chan ClientMessageBatch
	serverMessages  chan ServerMessageBatch
	stopFlag        atomic.Bool
	done            chan struct{}
	closedCb        ConnectionClosedCb
}

func NewConnection(conn net.Conn, store *Store, logger *zap.Logger, config ConnectionConfig, closedCb ConnectionClosedCb) *Connection {
	var reader FrameReader = &ConnFrameReader{
		conn:         conn,
		reader:       bufio.NewReaderSize(conn, int(config.ReadBufferSize)),
		maxFrameSize: config.MaxFrameSize,
	}
	var writer FrameWriter = &ConnFrameWriter{
		conn:   conn,
		writer: bufio.NewWriterSize(conn, int(config.WriteBufferSize)),
	}
	return &Connection{
		conn:            conn,
		store:           store,
		logger:          logger,
		config:          config,
		reader:          reader,
		writer:          writer,
		clientCodec:     &PbClientMessageCodec{},
		serverCodec:     &PbServerMessageCodec{},
		pendingCommands: make(chan CommandRequestBatch, config.CommandBufferDepth),
		clientMessages:  make(chan ClientMessageBatch, config.ClientMessageBufferDepth),
		serverMessages:  make(chan ServerMessageBatch, config.ServerMessageBufferDepth),
		done:            make(chan struct{}),
		closedCb:        closedCb,
	}
}

func (conn *Connection) Handle() {
	go conn.receiveMessages()
	go conn.processMessages()
	go conn.processPendingCommands()
	go conn.sendMessages()
}

func (conn *Connection) Stop() {
	conn.stopFlag.Store(true)
}

func (conn *Connection) Shutdown(ctx context.Context) error {
	conn.Stop()
	select {
	case <-conn.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (conn *Connection) receiveMessages() {
	defer close(conn.clientMessages)
	conn.logger.Debug("starting to receive messages", zap.String("addr", conn.conn.RemoteAddr().String()))
	for !conn.stopFlag.Load() {
		if conn.config.ReadTimeout > 0 {
			err := conn.conn.SetReadDeadline(time.Now().Add(conn.config.ReadTimeout))
			if err != nil {
				conn.logger.Debug("error setting read deadline", zap.Error(err))
				conn.stopFlag.Store(true)
				break
			}
		}
		frames, err := conn.reader.ReadFrames()
		if errors.Is(err, io.EOF) {
			conn.logger.Debug("received EOF")
			conn.stopFlag.Store(true)
			break
		} else if err != nil {
			conn.logger.Debug("error reading frames", zap.Error(err))
			conn.stopFlag.Store(true)
			break
		}
		messageBatch := make(ClientMessageBatch, 0, len(frames))
		for _, frame := range frames {
			message, decErr := conn.clientCodec.Decode(frame.Data)
			if decErr != nil {
				conn.logger.Debug("error decoding frame", zap.Error(decErr))
				continue
			}
			messageBatch = append(messageBatch, message)
		}
		if len(messageBatch) > 0 {
			conn.clientMessages <- messageBatch
		}
	}
}

func (conn *Connection) processMessages() {
	defer close(conn.pendingCommands)
	for clientMessageBatch := range conn.clientMessages {
		commandBatch := make([]Command, 0)
		commandRequestIds := make([]uint64, 0)
		for _, message := range clientMessageBatch {
			switch msg := message.(type) {
			case CommandRequestMessage:
				commandBatch = append(commandBatch, msg.Command)
				commandRequestIds = append(commandRequestIds, msg.RequestId)
			}
		}
		if len(commandBatch) > 0 {
			futures := conn.store.SubmitBatch(commandBatch)
			commandRequestBatch := make([]CommandRequest, 0, len(commandBatch))
			for i := 0; i < len(commandBatch); i++ {
				commandRequestBatch = append(commandRequestBatch, CommandRequest{
					RequestId:    commandRequestIds[i],
					ResultFuture: futures[i],
				})
			}
			conn.pendingCommands <- commandRequestBatch
		}
	}
}

func (conn *Connection) processPendingCommands() {
	defer close(conn.serverMessages)
	for pendingCommandBatch := range conn.pendingCommands {
		commandResultMessages := make([]ServerMessage, 0, len(pendingCommandBatch))
		for _, commandReq := range pendingCommandBatch {
			result, err := commandReq.ResultFuture.Get()
			errorCode, errorMessage := uint32(0), ""
			var storeErr StoreError
			if errors.As(err, &storeErr) {
				errorCode = storeErr.Code
				errorMessage = storeErr.Message
			} else if err != nil {
				errorMessage = err.Error()
			}
			commandResultMessages = append(commandResultMessages, CommandResultMessage{
				RequestId:     commandReq.RequestId,
				CommandResult: result,
				ErrorCode:     errorCode,
				ErrorMessage:  errorMessage,
			})
		}
		if len(commandResultMessages) > 0 {
			conn.serverMessages <- commandResultMessages
		}
	}
}

func (conn *Connection) sendMessages() {
	for serverMessageBatch := range conn.serverMessages {
		frameBatch := make([]Frame, 0, len(serverMessageBatch))
		for _, message := range serverMessageBatch {
			data, err := conn.serverCodec.Encode(message)
			if err != nil {
				conn.logger.Error("encode error", zap.Error(err))
				continue
			}
			frameBatch = append(frameBatch, Frame{Data: data})
		}
		if len(frameBatch) == 0 {
			continue
		}
		if conn.config.WriteTimeout > 0 {
			err := conn.conn.SetWriteDeadline(time.Now().Add(conn.config.WriteTimeout))
			if err != nil {
				conn.logger.Error("failed to set write deadline on client connection", zap.Error(err))
				break
			}
		}
		err := conn.writer.WriteFrames(frameBatch)
		if err != nil {
			conn.logger.Error("failed to write data frames", zap.Error(err))
			break
		}
	}
	for range conn.serverMessages {
	}
	err := conn.conn.Close()
	if err != nil {
		conn.logger.Error("failed to close connection", zap.Error(err))
	}
	conn.logger.Debug("connection closed")
	close(conn.done)
	if conn.closedCb != nil {
		conn.closedCb(conn)
	}
}
