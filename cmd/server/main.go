package main

import (
	"context"
	"fmt"
	store "lukas/simplekv/internal/store"
	"math/rand"
	"strconv"
)

func main() {
	kvStore := store.NewStore(store.StoreOptions{
		Ctx:               context.Background(),
		CommandBufferSize: 1024,
	})
	go kvStore.Run()
	for i := 0; i < 10000; i++ {
		var command store.Command = &store.SetOperation{
			Key:   strconv.Itoa(i),
			Value: strconv.Itoa(rand.Int()),
		}
		kvStore.Submit(command)
	}

	for i := 0; i < 10000; i++ {
		var command store.Command = &store.GetOperation{
			Key: strconv.Itoa(i),
		}
		result, err := kvStore.Execute(command)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(result)
		}
	}
}
