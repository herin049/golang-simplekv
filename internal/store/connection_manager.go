package store

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"net"
	"sync"
	"sync/atomic"
)

type ConnectionManager interface {
	Start(listener net.Listener)
	Stop()
	Shutdown(ctx context.Context) error
}

type DefaultConnectionManager struct {
	store          *Store
	logger         *zap.Logger
	listener       net.Listener
	listenerClosed atomic.Bool
	mutex          sync.Mutex
	nextConnId     uint64
	activeConn     map[uint64]*Connection
	maxConnections int
	connConfig     ConnectionConfig
	done           chan struct{}
	stopFlag       atomic.Bool
	connWg         sync.WaitGroup
}

func NewDefaultConnectionManager(store *Store, logger *zap.Logger, maxConnections int, connConfig ConnectionConfig) *DefaultConnectionManager {
	return &DefaultConnectionManager{
		store:          store,
		logger:         logger,
		listener:       nil,
		activeConn:     make(map[uint64]*Connection),
		maxConnections: maxConnections,
		connConfig:     connConfig,
		done:           make(chan struct{}),
	}
}

func (m *DefaultConnectionManager) Stop() {
	m.stopFlag.Store(true)
	m.closeListener()
}

func (m *DefaultConnectionManager) Shutdown(ctx context.Context) error {
	m.Stop()
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *DefaultConnectionManager) Start(listener net.Listener) {
	m.listener = listener
	go m.acceptConnections()
}

func (m *DefaultConnectionManager) closeListener() {
	if m.listenerClosed.CompareAndSwap(false, true) {
		err := m.listener.Close()
		if err != nil {
			m.logger.Error("error closing listener", zap.Error(err))
		}
	}
}

func (m *DefaultConnectionManager) acceptConnections() {
	for !m.stopFlag.Load() {
		conn, err := m.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			m.logger.Debug("listener closed")
			break
		} else if err != nil {
			// TODO: Add error handling
			m.logger.Error("error accepting connection", zap.Error(err))
			continue
		}
		connId := atomic.AddUint64(&m.nextConnId, 1)
		m.logger.Debug("accepting new connection", zap.Uint64("connId", connId), zap.String("addr", conn.RemoteAddr().String()))
		m.addConnection(conn, connId)
	}
	m.closeListener()
	m.mutex.Lock()
	for _, conn := range m.activeConn {
		conn.Stop()
	}
	m.mutex.Unlock()
	m.connWg.Wait()
	close(m.done)
}

func (m *DefaultConnectionManager) closeConn(conn net.Conn, connId uint64) {
	m.logger.Debug("closing connection", zap.Uint64("connId", connId), zap.String("addr", conn.RemoteAddr().String()))
	err := conn.Close()
	if err != nil {
		m.logger.Error("error closing connection", zap.Error(err))
	}
}

func (m *DefaultConnectionManager) addConnection(conn net.Conn, connId uint64) bool {
	newConn := NewConnection(conn, m.store, m.logger, m.connConfig, func(connection *Connection) {
		m.removeConnection(connection, connId)
	})
	m.connWg.Add(1)
	m.mutex.Lock()
	if len(m.activeConn) >= m.maxConnections {
		m.mutex.Unlock()
		m.logger.Debug("maximum number of connections exceeded", zap.Int("maxConnections", m.maxConnections))
		m.connWg.Done()
		m.closeConn(conn, connId)
		return false
	}
	newConn.Handle()
	m.activeConn[connId] = newConn
	m.mutex.Unlock()
	return true
}

func (m *DefaultConnectionManager) removeConnection(connection *Connection, connId uint64) {
	m.logger.Debug("removing connection", zap.Uint64("connId", connId), zap.String("addr", connection.conn.RemoteAddr().String()))
	m.mutex.Lock()
	delete(m.activeConn, connId)
	m.mutex.Unlock()
	m.connWg.Done()
}
