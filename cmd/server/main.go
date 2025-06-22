package main

import (
	"go.uber.org/zap"
	store "lukas/simplekv/internal/store"
	"strconv"
)

func main() {
	logger, _ := zap.NewDevelopment()
	kvStore := store.NewStore(logger, store.StoreOptions{
		TaskBufferDepth:  1024,
		MaxTaskBatchSize: 16,
	})
	go kvStore.Run()
	for i := 0; i < 10000; i++ {
		commands := make([]store.Command, 0, 16)
		for j := 0; j < 16; j++ {
			var command store.Command = store.SetCommand{
				Key:   strconv.Itoa(i),
				Value: strconv.Itoa(i),
			}
			commands = append(commands, command)
		}
		kvStore.SubmitBatch(commands)
	}

	for i := 0; i < 10000; i++ {
		var command store.Command = store.GetCommand{
			Key: strconv.Itoa(i),
		}
		result, err := kvStore.Execute(command)
		if err != nil {
			logger.Error("error executing command", zap.Error(err))
		} else {
			logger.Info("command result", zap.Any("result", result))
		}
	}
}
