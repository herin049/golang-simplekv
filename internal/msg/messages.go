package msg

import (
	"lukas/simplekv/internal/store"
)

type ClientMessage interface {
	isClientMessage() bool
}

type ServerMessage interface {
	isServerMessage() bool
}

type ClientMessageBatch []ClientMessage
type ServerMessageBatch []ServerMessage

type CommandRequestMessage struct {
	RequestId uint64
	Command   store.Command
}

func (c CommandRequestMessage) isClientMessage() bool {
	return true
}

type CommandResultMessage struct {
	RequestId     uint64
	CommandResult store.CommandResult
	ErrorCode     uint32
	ErrorMessage  string
}

func (c CommandResultMessage) isServerMessage() bool {
	return true
}
