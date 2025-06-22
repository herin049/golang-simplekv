package store

type Command interface {
	isCommand() bool
}

type CommandResult interface {
	isCommandResult() bool
}

type SetCommand struct {
	Key   string
	Value string
}

func (c SetCommand) isCommand() bool {
	return true
}

type SetCommandResult struct{}

func (c SetCommandResult) isCommandResult() bool {
	return true
}

type GetCommand struct {
	Key string
}

func (c GetCommand) isCommand() bool {
	return true
}

type GetCommandResult struct {
	Value string
}

func (c GetCommandResult) isCommandResult() bool {
	return true
}

type DelCommand struct {
	Key string
}

func (c DelCommand) isCommand() bool {
	return true
}

type DelCommandResult struct{}

func (c DelCommandResult) isCommandResult() bool {
	return true
}
