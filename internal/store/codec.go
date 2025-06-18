package store

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"lukas/simplekv/internal/pb"
)

func ToProtoOperation(operation any) (*pb.Operation, error) {
	if operation == nil {
		return nil, fmt.Errorf("operation cannot be nil")
	}
	switch op := operation.(type) {
	case *SetOperation:
		return &pb.Operation{
			Operation: &pb.Operation_SetOperation{
				SetOperation: &pb.SetOperation{
					Key:   op.Key,
					Value: op.Value,
				},
			},
		}, nil
	case *GetOperation:
		return &pb.Operation{
			Operation: &pb.Operation_GetOperation{
				GetOperation: &pb.GetOperation{
					Key: op.Key,
				},
			},
		}, nil
	case *DelOperation:
		return &pb.Operation{
			Operation: &pb.Operation_DelOperation{
				DelOperation: &pb.DelOperation{
					Key: op.Key,
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operation type: %T", op)
	}
}

func FromProtoOperation(operation *pb.Operation) (any, error) {
	if operation == nil {
		return nil, fmt.Errorf("operation cannot be nil")
	}
	switch op := operation.Operation.(type) {
	case *pb.Operation_SetOperation:
		return &SetOperation{
			Key:   op.SetOperation.Key,
			Value: op.SetOperation.Value,
		}, nil
	case *pb.Operation_GetOperation:
		return &GetOperation{
			Key: op.GetOperation.Key,
		}, nil
	case *pb.Operation_DelOperation:
		return &DelOperation{
			Key: op.DelOperation.Key,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operation type: %T", op)
	}
}

func EncodeOperation(operation any) ([]byte, error) {
	pbOperation, err := ToProtoOperation(operation)
	if err != nil {
		return nil, fmt.Errorf("failed to encode operation: %v", err)
	}
	data, err := proto.Marshal(pbOperation)
	if err != nil {
		return nil, fmt.Errorf("failed to encode operation: %v", err)
	}
	return data, nil
}

func DecodeOperation(data []byte) (any, error) {
	pbOperation := &pb.Operation{}
	if err := proto.Unmarshal(data, pbOperation); err != nil {
		return nil, fmt.Errorf("failed to decode operation: %v", err)
	}
	operation, err := FromProtoOperation(pbOperation)
	if err != nil {
		return nil, fmt.Errorf("failed to decode operation: %v", err)
	}
	return operation, nil
}
