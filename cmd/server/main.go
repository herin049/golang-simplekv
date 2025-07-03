package main

import (
	"context"
	"go.uber.org/zap"
	"lukas/simplekv/internal/server"
	"lukas/simplekv/internal/store"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(1)
	logger, _ := zap.NewDevelopment()
	logger.Debug("starting test")
	s := server.NewServer(logger, server.ServerConfig{
		Addr:           "",
		Port:           8080,
		MaxConnections: 1024,
	}, store.StoreOptions{
		TaskBufferDepth:  1024,
		MaxTaskBatchSize: 16,
	}, server.ConnectionConfig{
		ReadTimeout:              time.Second * 15,
		WriteTimeout:             time.Second * 15,
		MaxFrameSize:             4096,
		CommandBufferDepth:       256,
		ClientMessageBufferDepth: 256,
		ServerMessageBufferDepth: 256,
		ReadBufferSize:           1024 * 256,
		WriteBufferSize:          1024 * 256,
	})
	s.Start(false)
	err := s.Shutdown(context.Background())
	if err != nil {
		logger.Error("failed to shutdown server", zap.Error(err))
	}
}
