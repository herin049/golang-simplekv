package client

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"lukas/simplekv/internal/future"
	"lukas/simplekv/internal/msg"
	"lukas/simplekv/internal/store"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ClientError struct {
	Message string
}

func (err ClientError) Error() string {
	return err.Message
}

type CommandError struct {
	Message string
}

func (e CommandError) Error() string {
	return e.Message
}

type ClientRequest struct {
	RequestId      uint64
	RequestMessage msg.ClientMessage
	ResultFuture   *future.Future[any]
}

type ClientConfig struct {
	ServerAddr               string
	ServerPort               uint16
	ReadBufferSize           int
	WriteBufferSize          int
	ClientMessageBufferDepth uint32
	ServerMessageBufferDepth uint32
	WriteTimeout             time.Duration
	ReadTimeout              time.Duration
	CommandRequestTimeout    time.Duration
	ConnectTimeout           time.Duration
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		ServerAddr:      "localhost",
		ServerPort:      8080,
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		ConnectTimeout:  5 * time.Second,
	}
}

type Client struct {
	conn                  net.Conn
	logger                *zap.Logger
	config                ClientConfig
	nextRequestId         atomic.Uint64
	pendingCommands       map[uint64]PendingCommand
	pendingCommandsLock   sync.Mutex
	clientMessagesChannel chan msg.ClientMessageBatch
	serverMessagesChannel chan msg.ServerMessageBatch
	reader                msg.FrameReader
	writer                msg.FrameWriter
	clientCodec           msg.ClientMessageCodec
	serverCodec           msg.ServerMessageCodec
	ctx                   context.Context
	cancel                context.CancelFunc
	wg                    sync.WaitGroup
}

func NewClient(logger *zap.Logger, config ClientConfig) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		conn:                  nil,
		logger:                logger,
		config:                config,
		nextRequestId:         atomic.Uint64{},
		pendingCommands:       make(map[uint64]PendingCommand),
		pendingCommandsLock:   sync.Mutex{},
		clientMessagesChannel: make(chan msg.ClientMessageBatch, config.ClientMessageBufferDepth),
		serverMessagesChannel: make(chan msg.ServerMessageBatch, config.ServerMessageBufferDepth),
		reader:                nil,
		writer:                nil,
		clientCodec:           &msg.PbClientMessageCodec{},
		serverCodec:           &msg.PbServerMessageCodec{},
		ctx:                   ctx,
		cancel:                cancel,
		wg:                    sync.WaitGroup{},
	}
}

type PendingCommand struct {
	future  *future.Future[store.CommandResult]
	timeout time.Time
}

func (c *Client) Connect() error {
	serverAddr := fmt.Sprintf("%s:%d", c.config.ServerAddr, c.config.ServerPort)

	var err error
	if c.config.ConnectTimeout > 0 {
		c.conn, err = net.DialTimeout("tcp", serverAddr, c.config.ConnectTimeout)
	} else {
		c.conn, err = net.Dial("tcp", serverAddr)
	}
	if err != nil {
		return ClientError{fmt.Sprintf("Failed to connect to server: %s", err)}
	}
	c.logger.Info("connected to server", zap.String("address", serverAddr))

	c.reader = msg.NewBufferedFrameReader(c.conn, c.config.ReadBufferSize)
	c.writer = msg.NewBufferedFrameWriter(c.conn, c.config.WriteBufferSize)

	c.wg.Add(4)
	go c.sendMessages()
	go c.receiveMessages()
	go c.processMessages()
	go c.commandTimeoutLoop()
	return nil
}

func (c *Client) Shutdown() {
	c.cancel()
	err := c.conn.Close()
	if err != nil {
		c.logger.Error("error closing connection", zap.Error(err))
	}
	close(c.clientMessagesChannel)
	c.wg.Wait()
}

func (c *Client) SubmitCommand(command store.Command) *future.Future[store.CommandResult] {
	return c.submitCommandMessage(msg.CommandRequestMessage{
		RequestId: c.nextRequestId.Add(1),
		Command:   command,
	})
}

func (c *Client) SubmitCommandBatch(commands []store.Command) []*future.Future[store.CommandResult] {
	commandRequests := make([]msg.CommandRequestMessage, 0, len(commands))
	for _, command := range commands {
		commandRequests = append(commandRequests, msg.CommandRequestMessage{
			RequestId: c.nextRequestId.Add(1),
			Command:   command,
		})
	}
	return c.submitCommandMessageBatch(commandRequests)
}

func (c *Client) submitCommandMessage(commandMessage msg.CommandRequestMessage) *future.Future[store.CommandResult] {
	pendingCommand := PendingCommand{
		future:  future.NewFuture[store.CommandResult](),
		timeout: time.Now().Add(c.config.CommandRequestTimeout),
	}
	c.pendingCommandsLock.Lock()
	c.pendingCommands[commandMessage.RequestId] = pendingCommand
	c.pendingCommandsLock.Unlock()
	c.clientMessagesChannel <- msg.ClientMessageBatch{commandMessage}
	return pendingCommand.future
}

func (c *Client) submitCommandMessageBatch(commandMessages []msg.CommandRequestMessage) []*future.Future[store.CommandResult] {
	timeout := time.Now().Add(c.config.CommandRequestTimeout)
	futures := make([]*future.Future[store.CommandResult], 0, len(commandMessages))
	clientMessages := make(msg.ClientMessageBatch, 0, len(commandMessages))
	for _, commandMessage := range commandMessages {
		futures = append(futures, future.NewFuture[store.CommandResult]())
		clientMessages = append(clientMessages, commandMessage)
	}
	c.pendingCommandsLock.Lock()
	for i, commandMessage := range commandMessages {
		c.pendingCommands[commandMessage.RequestId] = PendingCommand{
			future:  futures[i],
			timeout: timeout,
		}
	}
	c.pendingCommandsLock.Unlock()
	c.clientMessagesChannel <- clientMessages
	return futures
}

func (c *Client) sendMessages() {
	defer c.wg.Done()
	for clientMessageBatch := range c.clientMessagesChannel {
		frameBatch := make([]msg.Frame, 0, len(clientMessageBatch))
		for _, message := range clientMessageBatch {
			data, err := c.clientCodec.Encode(message)
			if err != nil {
				c.logger.Error("encode error", zap.Error(err))
				continue
			}
			frameBatch = append(frameBatch, msg.Frame{Data: data})
		}
		if len(frameBatch) == 0 {
			continue
		}
		if c.config.WriteTimeout > 0 {
			err := c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
			if err != nil {
				c.logger.Error("set write deadline error", zap.Error(err))
				continue
			}
		}
		err := c.writer.WriteFrames(frameBatch)
		if err != nil {
			c.logger.Error("failed to write data frames", zap.Error(err))
			continue
		}
	}
}

func (c *Client) receiveMessages() {
	defer c.wg.Done()
	for c.ctx.Err() == nil {
		if c.config.ReadTimeout > 0 {
			err := c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
			if err != nil {
				c.logger.Error("set read deadline error", zap.Error(err))
				continue
			}
		}
		frames, err := c.reader.ReadFrames()
		if errors.Is(err, io.EOF) {
			c.logger.Debug("received EOF")
			continue
		} else if err != nil {
			c.logger.Debug("error reading frames", zap.Error(err))
			continue
		}
		messageBatch := make(msg.ServerMessageBatch, 0, len(frames))
		for _, frame := range frames {
			message, decErr := c.serverCodec.Decode(frame.Data)
			if decErr != nil {
				c.logger.Debug("error decoding frame", zap.Error(decErr))
				continue
			}
			messageBatch = append(messageBatch, message)
		}
		if len(messageBatch) > 0 {
			c.serverMessagesChannel <- messageBatch
		}
	}
	close(c.serverMessagesChannel)
}

func (c *Client) processMessages() {
	defer c.wg.Done()
	for serverMessageBatch := range c.serverMessagesChannel {
		for _, message := range serverMessageBatch {
			switch messageValue := message.(type) {
			case msg.CommandResultMessage:
				var pendingCommand PendingCommand
				c.pendingCommandsLock.Lock()
				pendingCommand = c.pendingCommands[messageValue.RequestId]
				delete(c.pendingCommands, messageValue.RequestId)
				c.pendingCommandsLock.Unlock()
				if pendingCommand.future != nil {
					if messageValue.ErrorCode == 0 {
						pendingCommand.future.Set(messageValue.CommandResult)
					} else {
						pendingCommand.future.SetErr(CommandError{messageValue.ErrorMessage})
					}
				}
			}
		}
	}
	c.pendingCommandsLock.Lock()
	for _, pendingCommand := range c.pendingCommands {
		pendingCommand.future.SetErr(CommandError{"client shutdown"})
	}
	c.pendingCommandsLock.Unlock()
}

func (c *Client) commandTimeoutLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Debug("command timeout loop exit")
			return
		case <-ticker.C:
			c.checkCommandTimeouts()
		}
	}
}

func (c *Client) checkCommandTimeouts() {
	now := time.Now()
	var timedOutCommands []*future.Future[store.CommandResult]
	c.pendingCommandsLock.Lock()
	for requestId, pendingCommand := range c.pendingCommands {
		if now.After(pendingCommand.timeout) {
			timedOutCommands = append(timedOutCommands, pendingCommand.future)
			delete(c.pendingCommands, requestId)
		}
	}
	c.pendingCommandsLock.Unlock()
	for _, pendingCommand := range timedOutCommands {
		pendingCommand.SetErr(CommandError{"command timed out"})
	}
}
