package main

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"lukas/simplekv/internal/store"
	"net"
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
		CommandBufferDepth:       1024,
		ClientMessageBufferDepth: 1024,
		ServerMessageBufferDepth: 1024,
		ReadBufferSize:           4096,
		WriteBufferSize:          4096,
	})
	server.Start()
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		logger.Debug("error connecting to server", zap.Error(err))
		panic(err)
	} else {
		logger.Debug("connected to server")
	}
	client := store.NewClient(conn, logger)
	for i := 0; i < 1000000; i++ {
		testStr := fmt.Sprintf("test%d", i)
		_, setErr := client.ExecuteCommand(store.SetCommand{Key: testStr, Value: testStr})
		if setErr != nil {
			logger.Debug("error setting value", zap.Error(setErr))
		}
	}

	for i := 0; i < 1000000; i++ {
		testStr := fmt.Sprintf("test%d", i)
		getResult, getErr := client.ExecuteCommand(store.GetCommand{Key: testStr})
		if getErr != nil {
			logger.Debug("error getting value", zap.Error(getErr))
		} else if i%100000 == 0 {
			logger.Debug("got value", zap.Any("value", getResult))
		}
	}
	conn.Close()
	server.Shutdown(context.Background())
}
