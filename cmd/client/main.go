package main

import (
	"go.uber.org/zap"
	"lukas/simplekv/internal/client"
	"lukas/simplekv/internal/store"
	"runtime"
	"sync"
	"time"
)

func runClient(logger *zap.Logger, wg *sync.WaitGroup) {
	defer wg.Done()
	client := client.NewClient(logger, client.ClientConfig{
		ServerAddr:               "localhost",
		ServerPort:               8080,
		ReadBufferSize:           1024 * 256,
		WriteBufferSize:          1024 * 256,
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
	numCommands := 10
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

}

func main() {
	runtime.GOMAXPROCS(4)
	logger, _ := zap.NewDevelopment()
	logger.Debug("starting test")
	numClients := 4
	wg := &sync.WaitGroup{}
	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go runClient(logger, wg)
	}
	wg.Wait()
}
