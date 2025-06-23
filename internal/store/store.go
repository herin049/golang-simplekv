package store

import (
	"context"
	"go.uber.org/zap"
)

var KeyNotFoundError = StoreError{1, "key not found"}
var UnknownCommandError = StoreError{2, "unknown command"}

type StoreError struct {
	Code    uint32
	Message string
}

func (err StoreError) Error() string {
	return err.Message
}

type StoreOptions struct {
	TaskBufferDepth  int
	MaxTaskBatchSize int
}

type StoreTask struct {
	command Command
	future  *Future[CommandResult]
}

type StoreTaskBatch []StoreTask

type Store struct {
	logger           *zap.Logger
	tasks            chan StoreTaskBatch
	items            map[string]string
	maxTaskBatchSize int
	done             chan struct{}
}

func NewStore(logger *zap.Logger, options StoreOptions) *Store {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Store{
		logger:           logger,
		tasks:            make(chan StoreTaskBatch, options.TaskBufferDepth),
		items:            make(map[string]string),
		maxTaskBatchSize: options.MaxTaskBatchSize,
		done:             make(chan struct{}),
	}
}

func (s *Store) executeSetCommand(command SetCommand) (SetCommandResult, error) {
	s.items[command.Key] = command.Value
	return SetCommandResult{}, nil
}

func (s *Store) executeGetCommand(command GetCommand) (GetCommandResult, error) {
	val, ok := s.items[command.Key]
	if ok {
		return GetCommandResult{val}, nil
	}
	return GetCommandResult{}, KeyNotFoundError
}

func (s *Store) executeDelCommand(command DelCommand) (DelCommandResult, error) {
	delete(s.items, command.Key)
	return DelCommandResult{}, nil
}

func (s *Store) executeCommand(command Command) (CommandResult, error) {
	switch v := command.(type) {
	case SetCommand:
		return s.executeSetCommand(v)
	case GetCommand:
		return s.executeGetCommand(v)
	case DelCommand:
		return s.executeDelCommand(v)
	default:
		return nil, UnknownCommandError
	}
}

func (s *Store) Submit(command Command) *Future[CommandResult] {
	resultFuture := NewFuture[CommandResult]()
	s.tasks <- StoreTaskBatch{StoreTask{command: command, future: resultFuture}}
	return resultFuture
}

func (s *Store) SubmitBatch(commands []Command) []*Future[CommandResult] {
	resultFutures := make([]*Future[CommandResult], 0, len(commands))
	taskBatch := StoreTaskBatch(make([]StoreTask, 0, len(commands)))
	for i := 0; i < len(commands); i++ {
		resultFuture := NewFuture[CommandResult]()
		resultFutures = append(resultFutures, resultFuture)
		taskBatch = append(taskBatch, StoreTask{command: commands[i], future: resultFuture})
	}
	for i := 0; i < len(taskBatch); i += s.maxTaskBatchSize {
		s.tasks <- taskBatch[i:min(i+s.maxTaskBatchSize, len(taskBatch))]
	}
	return resultFutures
}

func (s *Store) Execute(command Command) (CommandResult, error) {
	resultFuture := s.Submit(command)
	return resultFuture.Get()
}

func (s *Store) ExecuteBatch(commands []Command) ([]CommandResult, []error) {
	resultFutures := s.SubmitBatch(commands)
	commandResults := make([]CommandResult, 0, len(commands))
	commandErrors := make([]error, 0, len(commands))
	for i, _ := range commands {
		commandResult, commandError := resultFutures[i].Get()
		commandResults = append(commandResults, commandResult)
		commandErrors = append(commandErrors, commandError)
	}
	return commandResults, commandErrors
}

func (s *Store) Run() {
	s.logger.Info("running store")
	for taskBatch := range s.tasks {
		for _, task := range taskBatch {
			result, err := s.executeCommand(task.command)
			if err != nil {
				task.future.SetErr(err)
			} else {
				task.future.Set(result)
			}
		}
	}
	s.logger.Info("store finished processing commands")
	close(s.done)
}

func (s *Store) Stop() {
	close(s.tasks)
}

func (s *Store) Shutdown(ctx context.Context) error {
	s.Stop()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
