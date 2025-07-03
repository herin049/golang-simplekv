package server

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"lukas/simplekv/internal/store"
	"net"
)

type ServerConfig struct {
	Addr           string
	Port           uint16
	MaxConnections int
}

type Server struct {
	store             *store.Store
	logger            *zap.Logger
	connectionManager ConnectionManager
	serverConfig      ServerConfig
}

func NewServer(logger *zap.Logger, serverConfig ServerConfig, storeOptions store.StoreOptions, connConfig ConnectionConfig) *Server {
	store := store.NewStore(logger, storeOptions)
	return &Server{
		store:             store,
		logger:            logger,
		connectionManager: NewDefaultConnectionManager(store, logger, serverConfig.MaxConnections, connConfig),
		serverConfig:      serverConfig,
	}
}

func (s *Server) Start(daemon bool) {
	listenerAddr := fmt.Sprintf("%s:%d", s.serverConfig.Addr, s.serverConfig.Port)
	listener, err := net.Listen("tcp", listenerAddr)
	if err != nil {
		s.logger.Fatal("Error starting listener", zap.Error(err))
		return
	}
	s.connectionManager.Start(listener)
	if daemon {
		go s.store.Run()
	} else {
		s.store.Run()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	cmErr := s.connectionManager.Shutdown(ctx)
	if cmErr != nil {
		s.store.Stop()
		s.logger.Warn("error shutting down connection manager", zap.Error(cmErr))
		return cmErr
	}
	storeErr := s.store.Shutdown(ctx)
	if storeErr != nil {
		s.logger.Warn("error shutting down store", zap.Error(storeErr))
		return storeErr
	}
	return nil
}
