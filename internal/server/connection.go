package server

import (
	"context"
	"errors"
	"io"
	"lukas/simplekv/internal/future"
	"lukas/simplekv/internal/msg"
	"lukas/simplekv/internal/store"
	"net"
	"sync/atomic"
	"time"
)

import "go.uber.org/zap"

type CommandRequest struct {
	RequestId    uint64
	ResultFuture *future.Future[store.CommandResult]
}

type CommandRequestBatch []CommandRequest

type ConnectionConfig struct {
	ReadTimeout              time.Duration
	WriteTimeout             time.Duration
	MaxFrameSize             uint32
	CommandBufferDepth       uint32
	ClientMessageBufferDepth uint32
	ServerMessageBufferDepth uint32
	ReadBufferSize           int
	WriteBufferSize          int
}

type ConnectionClosedCb func(*Connection)

type Connection struct {
	conn            net.Conn
	store           *store.Store
	logger          *zap.Logger
	config          ConnectionConfig
	reader          msg.FrameReader
	writer          msg.FrameWriter
	clientCodec     msg.ClientMessageCodec
	serverCodec     msg.ServerMessageCodec
	pendingCommands chan CommandRequestBatch
	clientMessages  chan msg.ClientMessageBatch
	serverMessages  chan msg.ServerMessageBatch
	connClosed      atomic.Bool
	done            chan struct{}
	closedCb        ConnectionClosedCb
}

func NewConnection(conn net.Conn, store *store.Store, logger *zap.Logger, config ConnectionConfig, closedCb ConnectionClosedCb) *Connection {
	return &Connection{
		conn:            conn,
		store:           store,
		logger:          logger,
		config:          config,
		reader:          msg.NewBufferedFrameReader(conn, config.ReadBufferSize),
		writer:          msg.NewBufferedFrameWriter(conn, config.WriteBufferSize),
		clientCodec:     &msg.PbClientMessageCodec{},
		serverCodec:     &msg.PbServerMessageCodec{},
		pendingCommands: make(chan CommandRequestBatch, config.CommandBufferDepth),
		clientMessages:  make(chan msg.ClientMessageBatch, config.ClientMessageBufferDepth),
		serverMessages:  make(chan msg.ServerMessageBatch, config.ServerMessageBufferDepth),
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
	conn.closeConnection()
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

func (conn *Connection) closeConnection() {
	if conn.connClosed.CompareAndSwap(false, true) {
		err := conn.conn.Close()
		if err != nil {
			conn.logger.Error("error closing connection", zap.Error(err))
		}
	}
}

func (conn *Connection) receiveMessages() {
	defer close(conn.clientMessages)
	conn.logger.Debug("starting to receive messages", zap.String("addr", conn.conn.RemoteAddr().String()))
	for {
		if conn.config.ReadTimeout > 0 {
			err := conn.conn.SetReadDeadline(time.Now().Add(conn.config.ReadTimeout))
			if err != nil {
				conn.logger.Debug("error setting read deadline", zap.Error(err))
				break
			}
		}
		frames, err := conn.reader.ReadFrames()
		if errors.Is(err, io.EOF) {
			conn.logger.Debug("received EOF")
			break
		} else if err != nil {
			conn.logger.Debug("error reading frames", zap.Error(err))
			break
		}
		messageBatch := make(msg.ClientMessageBatch, 0, len(frames))
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
		commandBatch := make([]store.Command, 0)
		commandRequestIds := make([]uint64, 0)
		for _, message := range clientMessageBatch {
			switch msg := message.(type) {
			case msg.CommandRequestMessage:
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
		commandResultMessages := make([]msg.ServerMessage, 0, len(pendingCommandBatch))
		for i := len(pendingCommandBatch) - 1; i >= 0; i-- {
			commandReq := pendingCommandBatch[i]
			result, err := commandReq.ResultFuture.Get()
			errorCode, errorMessage := uint32(0), ""
			var storeErr store.StoreError
			if errors.As(err, &storeErr) {
				errorCode = storeErr.Code
				errorMessage = storeErr.Message
			} else if err != nil {
				errorMessage = err.Error()
			}
			commandResultMessages = append(commandResultMessages, msg.CommandResultMessage{
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
		frameBatch := make([]msg.Frame, 0, len(serverMessageBatch))
		for _, message := range serverMessageBatch {
			data, err := conn.serverCodec.Encode(message)
			if err != nil {
				conn.logger.Error("encode error", zap.Error(err))
				continue
			}
			frameBatch = append(frameBatch, msg.Frame{Data: data})
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
