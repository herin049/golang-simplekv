protoc -I=internal/pb --go_out=internal internal/pb/messages.proto
protoc -I=internal/pb --go_out=internal internal/pb/operations.proto
