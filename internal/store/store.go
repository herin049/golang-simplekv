package store

import (
	"context"
)

const KeyNotFoundError = stringError("Key not found")
const UnknownCommandError = stringError("Unknown command")

type StoreOptions struct {
	Ctx               context.Context
	CommandBufferSize int
}

type StoreTask struct {
	operation any
	future    *Future[any]
}

type Store struct {
	ctx    context.Context
	cancel context.CancelFunc
	tasks  chan StoreTask
	items  map[string]string
}

func NewStore(options StoreOptions) *Store {
	ctx, cancel := context.WithCancel(options.Ctx)
	return &Store{
		ctx:    ctx,
		cancel: cancel,
		tasks:  make(chan StoreTask, options.CommandBufferSize),
		items:  make(map[string]string),
	}
}

func (s *Store) executeSetOperation(op *SetOperation) (*SetOperationResult, error) {
	s.items[op.Key] = op.Value
	return &SetOperationResult{}, nil
}

func (s *Store) executeGetOperation(op *GetOperation) (*GetOperationResult, error) {
	val, ok := s.items[op.Key]
	if ok {
		return &GetOperationResult{val}, nil
	}
	return nil, KeyNotFoundError
}

func (s *Store) executeDelOperation(op *DelOperation) (*DelOperationResult, error) {
	delete(s.items, op.Key)
	return &DelOperationResult{}, nil
}

func (s *Store) executeOperation(op any) (any, error) {
	switch v := op.(type) {
	case *SetOperation:
		return s.executeSetOperation(v)
	case *GetOperation:
		return s.executeGetOperation(v)
	case *DelOperation:
		return s.executeDelOperation(v)
	default:
		return nil, UnknownCommandError
	}
}

func (s *Store) Submit(op any) *Future[any] {
	resultFuture := NewFuture[any]()
	s.tasks <- StoreTask{operation: op, future: resultFuture}
	return resultFuture
}

func (s *Store) Execute(op any) (any, error) {
	resultFuture := s.Submit(op)
	return resultFuture.Get()
}

func (s *Store) Run() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case task := <-s.tasks:
			result, err := s.executeOperation(task.operation)
			if err != nil {
				task.future.SetErr(err)
			} else {
				task.future.Set(result)
			}
		}
	}
}
