package main

import (
	"context"
	"go.uber.org/zap"
	"lukas/simplekv/internal/store"
	"time"
)

func main() {
	logger, _ := zap.NewDevelopment()
	logger.Debug("starting test")
	server := store.NewServer(logger, store.ServerConfig{
		Addr:           "",
		Port:           8080,
		MaxConnections: 1024,
	}, store.StoreOptions{
		TaskBufferDepth:  1024,
		MaxTaskBatchSize: 16,
	}, store.ConnectionConfig{
		ReadTimeout:              time.Second * 15,
		WriteTimeout:             time.Second * 15,
		MaxFrameSize:             4096,
		CommandBufferDepth:       256,
		ClientMessageBufferDepth: 256,
		ServerMessageBufferDepth: 256,
		ReadBufferSize:           1024 * 64,
		WriteBufferSize:          1024 * 64,
	})
	server.Start()
	client := store.NewClient(logger, store.ClientConfig{
		ServerAddr:               "localhost",
		ServerPort:               8080,
		ReadBufferSize:           1024 * 64,
		WriteBufferSize:          1024 * 64,
		ClientMessageBufferDepth: 128,
		ServerMessageBufferDepth: 128,
		WriteTimeout:             time.Second * 15,
		ReadTimeout:              time.Second * 15,
		CommandRequestTimeout:    time.Second * 30,
		ConnectTimeout:           time.Second * 30,
	})
	err := client.Connect()
	if err != nil {
		logger.Debug("failed to connect to server", zap.Error(err))
		panic(err)
	}
	logger.Debug("successfully connected to server")
	numCommands := 100
	commandBatch := make([]store.Command, 0, numCommands)
	for i := 0; i < numCommands; i++ {
		commandBatch = append(commandBatch, store.SetCommand{Key: "test", Value: "test"})
	}
	for i := 0; i < 100000; i++ {
		client.SubmitCommandBatch(commandBatch)
	}
	logger.Debug("executed set commands")
	getResult, getErr := client.SubmitCommand(store.GetCommand{Key: "test"}).Get()
	if getErr != nil {
		logger.Debug("error getting value", zap.Error(getErr))
	} else {
		logger.Debug("got value", zap.Any("value", getResult))
	}

	client.Shutdown()
	server.Shutdown(context.Background())
}
