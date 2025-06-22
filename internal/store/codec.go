package store

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"lukas/simplekv/internal/pb"
)

type ClientMessageCodec interface {
	Encode(message ClientMessage) ([]byte, error)
	Decode(data []byte) (ClientMessage, error)
}

type ClientCodecError struct {
	Message string
}

func (err ClientCodecError) Error() string {
	return err.Message
}

type ServerMessageCodec interface {
	Encode(message ServerMessage) ([]byte, error)
	Decode(data []byte) (ServerMessage, error)
}

type ServerCodecError struct {
	Message string
}

func (err ServerCodecError) Error() string {
	return err.Message
}

type PbClientMessageCodec struct {
}

func NewPbClientMessageCodec() PbClientMessageCodec {
	return PbClientMessageCodec{}
}

func (*PbClientMessageCodec) toProtoCommandRequest(commandRequest CommandRequestMessage) (*pb.ClientMessage, error) {
	pbCmdReqMsg := &pb.CommandRequestMessage{
		RequestId: commandRequest.RequestId,
	}

	switch cmd := commandRequest.Command.(type) {
	case SetCommand:
		pbCmdReqMsg.Command = &pb.CommandRequestMessage_SetCommand{
			SetCommand: &pb.SetCommand{
				Key:   cmd.Key,
				Value: cmd.Value,
			},
		}
	case GetCommand:
		pbCmdReqMsg.Command = &pb.CommandRequestMessage_GetCommand{
			GetCommand: &pb.GetCommand{
				Key: cmd.Key,
			},
		}
	case DelCommand:
		pbCmdReqMsg.Command = &pb.CommandRequestMessage_DelCommand{
			DelCommand: &pb.DelCommand{
				Key: cmd.Key,
			},
		}
	default:
		return nil, ClientCodecError{fmt.Sprintf("unknown client command type: %T", cmd)}
	}

	return &pb.ClientMessage{
		Message: &pb.ClientMessage_CommandRequest{
			CommandRequest: pbCmdReqMsg,
		},
	}, nil
}

func (c *PbClientMessageCodec) Encode(message ClientMessage) ([]byte, error) {
	var pbMsg *pb.ClientMessage
	var err error

	switch req := message.(type) {
	case CommandRequestMessage:
		pbMsg, err = c.toProtoCommandRequest(req)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ClientCodecError{fmt.Sprintf("unknown client request type: %T", req)}
	}

	return proto.Marshal(pbMsg)
}

func (*PbClientMessageCodec) fromProtoCommandRequest(pbCmdReqMsg *pb.CommandRequestMessage) (CommandRequestMessage, error) {
	var cmd Command

	switch pbCmd := pbCmdReqMsg.Command.(type) {
	case *pb.CommandRequestMessage_SetCommand:
		cmd = SetCommand{
			Key:   pbCmd.SetCommand.Key,
			Value: pbCmd.SetCommand.Value,
		}
	case *pb.CommandRequestMessage_GetCommand:
		cmd = GetCommand{
			Key: pbCmd.GetCommand.Key,
		}
	case *pb.CommandRequestMessage_DelCommand:
		cmd = DelCommand{
			Key: pbCmd.DelCommand.Key,
		}
	default:
		return CommandRequestMessage{}, ClientCodecError{fmt.Sprintf("unknown command type: %T", pbCmd)}
	}

	return CommandRequestMessage{
		RequestId: pbCmdReqMsg.RequestId,
		Command:   cmd,
	}, nil
}

func (c *PbClientMessageCodec) Decode(data []byte) (ClientMessage, error) {
	pbMsg := &pb.ClientMessage{}
	if err := proto.Unmarshal(data, pbMsg); err != nil {
		return nil, ClientCodecError{Message: "failed to unmarshal client message: " + err.Error()}
	}

	switch msg := pbMsg.Message.(type) {
	case *pb.ClientMessage_CommandRequest:
		return c.fromProtoCommandRequest(msg.CommandRequest)
	default:
		return nil, ClientCodecError{fmt.Sprintf("unknown client request type: %T", pbMsg)}
	}
}

type PbServerMessageCodec struct {
}

func NewPbServerMessageCodec() PbServerMessageCodec {
	return PbServerMessageCodec{}
}

func (*PbServerMessageCodec) toProtoCommandResult(commandResult CommandResultMessage) (*pb.ServerMessage, error) {
	pbCmdResultMsg := &pb.CommandResultMessage{
		RequestId: commandResult.RequestId,
		ErrorCode: commandResult.ErrorCode,
	}

	if commandResult.ErrorMessage != "" {
		pbCmdResultMsg.ErrorMessage = &commandResult.ErrorMessage
	}

	switch result := commandResult.CommandResult.(type) {
	case SetCommandResult:
		pbCmdResultMsg.CommandResult = &pb.CommandResultMessage_SetCommandResult{
			SetCommandResult: &pb.SetCommandResult{},
		}
	case GetCommandResult:
		pbCmdResultMsg.CommandResult = &pb.CommandResultMessage_GetCommandResult{
			GetCommandResult: &pb.GetCommandResult{
				Value: result.Value,
			},
		}
	case DelCommandResult:
		pbCmdResultMsg.CommandResult = &pb.CommandResultMessage_DelCommandResult{
			DelCommandResult: &pb.DelCommandResult{},
		}
	}

	return &pb.ServerMessage{
		Message: &pb.ServerMessage_CommandResult{
			CommandResult: pbCmdResultMsg,
		},
	}, nil
}

func (c *PbServerMessageCodec) Encode(message ServerMessage) ([]byte, error) {
	var pbMsg *pb.ServerMessage
	var err error

	switch result := message.(type) {
	case CommandResultMessage:
		pbMsg, err = c.toProtoCommandResult(result)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ServerCodecError{fmt.Sprintf("unknown server message type: %T", result)}
	}

	return proto.Marshal(pbMsg)
}

func (*PbServerMessageCodec) fromProtoCommandResult(pbCmdResultMsg *pb.CommandResultMessage) (CommandResultMessage, error) {
	errorCode := pbCmdResultMsg.ErrorCode
	errorMessage := ""
	if pbCmdResultMsg.ErrorMessage != nil {
		errorMessage = *pbCmdResultMsg.ErrorMessage
	}

	var cmdResult CommandResult
	switch pbResult := pbCmdResultMsg.CommandResult.(type) {
	case *pb.CommandResultMessage_SetCommandResult:
		cmdResult = SetCommandResult{}
	case *pb.CommandResultMessage_GetCommandResult:
		cmdResult = GetCommandResult{
			Value: pbResult.GetCommandResult.Value,
		}
	case *pb.CommandResultMessage_DelCommandResult:
		cmdResult = DelCommandResult{}
	default:
		cmdResult = nil
		if errorCode == 0 {
			return CommandResultMessage{}, ServerCodecError{fmt.Sprintf("unknown command type: %T", pbCmdResultMsg.CommandResult)}
		}
	}

	return CommandResultMessage{
		RequestId:     pbCmdResultMsg.RequestId,
		CommandResult: cmdResult,
		ErrorCode:     errorCode,
		ErrorMessage:  errorMessage,
	}, nil
}

func (c *PbServerMessageCodec) Decode(data []byte) (ServerMessage, error) {
	pbMsg := &pb.ServerMessage{}
	if err := proto.Unmarshal(data, pbMsg); err != nil {
		return nil, ServerCodecError{Message: "failed to unmarshal server message: " + err.Error()}
	}

	switch msg := pbMsg.Message.(type) {
	case *pb.ServerMessage_CommandResult:
		return c.fromProtoCommandResult(msg.CommandResult)
	default:
		return nil, ServerCodecError{fmt.Sprintf("unknown server response type: %T", pbMsg)}
	}
}
