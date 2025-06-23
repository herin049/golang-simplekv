package store

import (
	"bufio"
	"errors"
	"go.uber.org/zap"
	"net"
)

type Client struct {
	conn        net.Conn
	logger      *zap.Logger
	reader      FrameReader
	writer      FrameWriter
	clientCodec ClientMessageCodec
	serverCodec ServerMessageCodec
}

func NewClient(conn net.Conn, logger *zap.Logger) *Client {
	return &Client{
		conn:   nil,
		logger: logger,
		reader: &ConnFrameReader{
			conn:         conn,
			reader:       bufio.NewReader(conn),
			maxFrameSize: 4096,
		},
		writer: &ConnFrameWriter{
			conn:   conn,
			writer: bufio.NewWriter(conn),
		},
		clientCodec: &PbClientMessageCodec{},
		serverCodec: &PbServerMessageCodec{},
	}
}

func (client *Client) ExecuteCommand(command Command) (CommandResult, error) {
	req := CommandRequestMessage{
		RequestId: 123,
		Command:   command,
	}
	encData, encErr := client.clientCodec.Encode(req)
	if encErr != nil {
		client.logger.Error("failed to encode request", zap.Error(encErr))
		return nil, encErr
	}
	writeErr := client.writer.WriteFrame(Frame{
		Data: encData,
	})
	if writeErr != nil {
		client.logger.Error("failed to write request", zap.Error(writeErr))
		return nil, writeErr
	}
	frame, readErr := client.reader.ReadFrame()
	if readErr != nil {
		client.logger.Error("failed to read frame", zap.Error(readErr))
		return nil, readErr
	}
	serverMsg, decErr := client.serverCodec.Decode(frame.Data)
	if decErr != nil {
		client.logger.Error("failed to decode response", zap.Error(decErr))
		return nil, decErr
	}
	switch m := serverMsg.(type) {
	case CommandResultMessage:
		return m.CommandResult, nil
	}
	return nil, errors.New("unknown server response")
}
