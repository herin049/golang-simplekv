package msg

import (
	"bytes"
	"errors"
	"lukas/simplekv/internal/store"
	"testing"
)

// Test data constants
const (
	testKey     = "test_key"
	testValue   = "test_value"
	testReqID   = uint64(12345)
	testErrCode = uint32(404)
	testErrMsg  = "not found"
)

// Helper function to create test commands
func createTestSetCommand() store.SetCommand {
	return store.SetCommand{Key: testKey, Value: testValue}
}

func createTestGetCommand() store.GetCommand {
	return store.GetCommand{Key: testKey}
}

func createTestDelCommand() store.DelCommand {
	return store.DelCommand{Key: testKey}
}

// Helper function to create test command request messages
func createTestCommandRequestMessage(cmd store.Command) CommandRequestMessage {
	return CommandRequestMessage{
		RequestId: testReqID,
		Command:   cmd,
	}
}

// Helper function to create test command result messages
func createTestCommandResultMessage(result store.CommandResult, errCode uint32, errMsg string) CommandResultMessage {
	return CommandResultMessage{
		RequestId:     testReqID,
		CommandResult: result,
		ErrorCode:     errCode,
		ErrorMessage:  errMsg,
	}
}

// Unit Tests for Client Message Codec
func TestPbClientMessageCodec_EncodeDecodeSetCommand(t *testing.T) {
	codec := NewPbClientMessageCodec()
	setCmd := createTestSetCommand()
	reqMsg := createTestCommandRequestMessage(setCmd)

	// Test encoding
	data, err := codec.Encode(reqMsg)
	if err != nil {
		t.Fatalf("Failed to encode SetCommand: %v", err)
	}

	// Test decoding
	decodedMsg, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode SetCommand: %v", err)
	}

	// Verify decoded msg
	decodedReq, ok := decodedMsg.(CommandRequestMessage)
	if !ok {
		t.Fatalf("Expected CommandRequestMessage, got %T", decodedMsg)
	}

	if decodedReq.RequestId != testReqID {
		t.Errorf("Expected RequestId %d, got %d", testReqID, decodedReq.RequestId)
	}

	decodedSetCmd, ok := decodedReq.Command.(store.SetCommand)
	if !ok {
		t.Fatalf("Expected SetCommand, got %T", decodedReq.Command)
	}

	if decodedSetCmd.Key != testKey {
		t.Errorf("Expected Key %s, got %s", testKey, decodedSetCmd.Key)
	}

	if decodedSetCmd.Value != testValue {
		t.Errorf("Expected Value %s, got %s", testValue, decodedSetCmd.Value)
	}
}

func TestPbClientMessageCodec_EncodeDecodeGetCommand(t *testing.T) {
	codec := NewPbClientMessageCodec()
	getCmd := createTestGetCommand()
	reqMsg := createTestCommandRequestMessage(getCmd)

	// Test round-trip encoding/decoding
	data, err := codec.Encode(reqMsg)
	if err != nil {
		t.Fatalf("Failed to encode GetCommand: %v", err)
	}

	decodedMsg, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode GetCommand: %v", err)
	}

	decodedReq := decodedMsg.(CommandRequestMessage)
	decodedGetCmd := decodedReq.Command.(store.GetCommand)

	if decodedGetCmd.Key != testKey {
		t.Errorf("Expected Key %s, got %s", testKey, decodedGetCmd.Key)
	}
}

func TestPbClientMessageCodec_EncodeDecodeDelCommand(t *testing.T) {
	codec := NewPbClientMessageCodec()
	delCmd := createTestDelCommand()
	reqMsg := createTestCommandRequestMessage(delCmd)

	// Test round-trip encoding/decoding
	data, err := codec.Encode(reqMsg)
	if err != nil {
		t.Fatalf("Failed to encode DelCommand: %v", err)
	}

	decodedMsg, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode DelCommand: %v", err)
	}

	decodedReq := decodedMsg.(CommandRequestMessage)
	decodedDelCmd := decodedReq.Command.(store.DelCommand)

	if decodedDelCmd.Key != testKey {
		t.Errorf("Expected Key %s, got %s", testKey, decodedDelCmd.Key)
	}
}

func TestPbClientMessageCodec_DecodeInvalidData(t *testing.T) {
	codec := NewPbClientMessageCodec()

	// Test with invalid protobuf data
	invalidData := []byte("invalid protobuf data")
	_, err := codec.Decode(invalidData)
	if err == nil {
		t.Fatal("Expected error for invalid protobuf data")
	}

	var clientErr ClientCodecError
	ok := errors.As(err, &clientErr)
	if !ok {
		t.Fatalf("Expected ClientCodecError, got %T", err)
	}

	if !bytes.Contains([]byte(clientErr.Message), []byte("failed to unmarshal")) {
		t.Errorf("Expected error msg to contain 'failed to unmarshal', got: %s", clientErr.Message)
	}
}

// Unit Tests for Server Message Codec
func TestPbServerMessageCodec_EncodeDecodeSetCommandResult(t *testing.T) {
	codec := NewPbServerMessageCodec()
	setResult := store.SetCommandResult{}
	resultMsg := createTestCommandResultMessage(setResult, 0, "")

	// Test round-trip encoding/decoding
	data, err := codec.Encode(resultMsg)
	if err != nil {
		t.Fatalf("Failed to encode SetCommandResult: %v", err)
	}

	decodedMsg, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode SetCommandResult: %v", err)
	}

	decodedResult := decodedMsg.(CommandResultMessage)
	if decodedResult.RequestId != testReqID {
		t.Errorf("Expected RequestId %d, got %d", testReqID, decodedResult.RequestId)
	}

	_, ok := decodedResult.CommandResult.(store.SetCommandResult)
	if !ok {
		t.Fatalf("Expected SetCommandResult, got %T", decodedResult.CommandResult)
	}
}

func TestPbServerMessageCodec_EncodeDecodeGetCommandResult(t *testing.T) {
	codec := NewPbServerMessageCodec()
	getResult := store.GetCommandResult{Value: testValue}
	resultMsg := createTestCommandResultMessage(getResult, 0, "")

	// Test round-trip encoding/decoding
	data, err := codec.Encode(resultMsg)
	if err != nil {
		t.Fatalf("Failed to encode GetCommandResult: %v", err)
	}

	decodedMsg, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode GetCommandResult: %v", err)
	}

	decodedResult := decodedMsg.(CommandResultMessage)
	decodedGetResult := decodedResult.CommandResult.(store.GetCommandResult)

	if decodedGetResult.Value != testValue {
		t.Errorf("Expected Value %s, got %s", testValue, decodedGetResult.Value)
	}
}

func TestPbServerMessageCodec_EncodeDecodeWithError(t *testing.T) {
	codec := NewPbServerMessageCodec()
	delResult := store.DelCommandResult{}
	resultMsg := createTestCommandResultMessage(delResult, testErrCode, testErrMsg)

	// Test round-trip encoding/decoding
	data, err := codec.Encode(resultMsg)
	if err != nil {
		t.Fatalf("Failed to encode DelCommandResult with error: %v", err)
	}

	decodedMsg, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode DelCommandResult with error: %v", err)
	}

	decodedResult := decodedMsg.(CommandResultMessage)
	if decodedResult.ErrorCode != testErrCode {
		t.Errorf("Expected ErrorCode %d, got %d", testErrCode, decodedResult.ErrorCode)
	}

	if decodedResult.ErrorMessage != testErrMsg {
		t.Errorf("Expected ErrorMessage %s, got %s", testErrMsg, decodedResult.ErrorMessage)
	}
}

// Benchmark Tests
func BenchmarkClientCodec_EncodeSetCommand(b *testing.B) {
	codec := NewPbClientMessageCodec()
	setCmd := createTestSetCommand()
	reqMsg := createTestCommandRequestMessage(setCmd)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Encode(reqMsg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClientCodec_DecodeSetCommand(b *testing.B) {
	codec := NewPbClientMessageCodec()
	setCmd := createTestSetCommand()
	reqMsg := createTestCommandRequestMessage(setCmd)

	data, err := codec.Encode(reqMsg)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Decode(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClientCodec_RoundTripSetCommand(b *testing.B) {
	codec := NewPbClientMessageCodec()
	setCmd := createTestSetCommand()
	reqMsg := createTestCommandRequestMessage(setCmd)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := codec.Encode(reqMsg)
		if err != nil {
			b.Fatal(err)
		}
		_, err = codec.Decode(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServerCodec_EncodeGetCommandResult(b *testing.B) {
	codec := NewPbServerMessageCodec()
	getResult := store.GetCommandResult{Value: testValue}
	resultMsg := createTestCommandResultMessage(getResult, 0, "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Encode(resultMsg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServerCodec_DecodeGetCommandResult(b *testing.B) {
	codec := NewPbServerMessageCodec()
	getResult := store.GetCommandResult{Value: testValue}
	resultMsg := createTestCommandResultMessage(getResult, 0, "")

	data, err := codec.Encode(resultMsg)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Decode(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServerCodec_RoundTripGetCommandResult(b *testing.B) {
	codec := NewPbServerMessageCodec()
	getResult := store.GetCommandResult{Value: testValue}
	resultMsg := createTestCommandResultMessage(getResult, 0, "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := codec.Encode(resultMsg)
		if err != nil {
			b.Fatal(err)
		}
		_, err = codec.Decode(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark with different payload sizes
func BenchmarkClientCodec_EncodeLargeSetCommand(b *testing.B) {
	codec := NewPbClientMessageCodec()

	// Create a large value (1KB)
	largeValue := string(make([]byte, 1024))
	setCmd := store.SetCommand{Key: testKey, Value: largeValue}
	reqMsg := createTestCommandRequestMessage(setCmd)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Encode(reqMsg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServerCodec_EncodeLargeGetCommandResult(b *testing.B) {
	codec := NewPbServerMessageCodec()

	// Create a large value (1KB)
	largeValue := string(make([]byte, 1024))
	getResult := store.GetCommandResult{Value: largeValue}
	resultMsg := createTestCommandResultMessage(getResult, 0, "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Encode(resultMsg)
		if err != nil {
			b.Fatal(err)
		}
	}
}
