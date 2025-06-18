package store

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Benchmark helper functions
func generateRandomKey(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func generateRandomValue(length int) string {
	return generateRandomKey(length) // Same implementation for values
}

func setupStore(bufferSize int) (*Store, func()) {
	ctx := context.Background()
	store := NewStore(StoreOptions{
		Ctx:               ctx,
		CommandBufferSize: bufferSize,
	})

	// Start the store in a goroutine
	go store.Run()

	// Return cleanup function
	cleanup := func() {
		store.cancel()
	}

	return store, cleanup
}

// Basic operation benchmarks
func BenchmarkSet(b *testing.B) {
	store, cleanup := setupStore(1000)
	defer cleanup()

	// Pre-generate all keys and values outside timed section
	keys := make([]string, b.N)
	values := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = generateRandomKey(10)
		values[i] = generateRandomValue(100)
	}

	// Pre-create all tasks
	commands := make([]*SetOperation, b.N)
	for i := 0; i < b.N; i++ {
		commands[i] = &SetOperation{Key: keys[i], Value: values[i]}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Execute(commands[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	store, cleanup := setupStore(1000)
	defer cleanup()

	// Pre-populate store
	numKeys := 10000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		keys[i] = key

		cmd := &SetOperation{Key: key, Value: value}
		store.Execute(cmd)
	}

	// Pre-generate random indices for key selection
	keyIndices := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		keyIndices[i] = rand.Intn(numKeys)
	}

	// Pre-create all GET tasks
	commands := make([]*GetOperation, b.N)
	for i := 0; i < b.N; i++ {
		commands[i] = &GetOperation{Key: keys[keyIndices[i]]}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Execute(commands[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDel(b *testing.B) {
	store, cleanup := setupStore(1000)
	defer cleanup()

	// Pre-populate store with more keys than we'll delete
	numKeys := b.N * 2
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		keys[i] = key

		cmd := &SetOperation{Key: key, Value: value}
		store.Execute(cmd)
	}

	// Pre-create all DELETE tasks
	commands := make([]*DelOperation, b.N)
	for i := 0; i < b.N; i++ {
		commands[i] = &DelOperation{Key: keys[i]}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Execute(commands[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Mixed workload benchmarks
func BenchmarkMixedWorkload(b *testing.B) {
	store, cleanup := setupStore(1000)
	defer cleanup()

	// Pre-populate with some data
	baseKeys := 1000
	keys := make([]string, baseKeys)
	for i := 0; i < baseKeys; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		keys[i] = key

		cmd := &SetOperation{Key: key, Value: value}
		store.Execute(cmd)
	}

	// Pre-generate operation types and all necessary data
	operations := make([]float32, b.N)
	readIndices := make([]int, b.N)
	writeKeys := make([]string, b.N)
	writeValues := make([]string, b.N)
	deleteIndices := make([]int, b.N)

	for i := 0; i < b.N; i++ {
		operations[i] = rand.Float32()
		readIndices[i] = rand.Intn(len(keys))
		writeKeys[i] = generateRandomKey(10)
		writeValues[i] = generateRandomValue(100)
		deleteIndices[i] = rand.Intn(len(keys))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		op := operations[i]
		switch {
		case op < 0.7: // 70% reads
			cmd := &GetOperation{Key: keys[readIndices[i]]}
			store.Execute(cmd)
		case op < 0.9: // 20% writes
			cmd := &SetOperation{Key: writeKeys[i], Value: writeValues[i]}
			store.Execute(cmd)
		default: // 10% deletes
			cmd := &DelOperation{Key: keys[deleteIndices[i]]}
			store.Execute(cmd)
		}
	}
}

// Concurrency benchmarks
func BenchmarkConcurrentSet(b *testing.B) {
	store, cleanup := setupStore(100000)
	defer cleanup()

	numGoroutines := runtime.NumCPU()
	opsPerGoroutine := b.N / numGoroutines

	// Pre-generate all data for all goroutines
	allKeys := make([][]string, numGoroutines)
	allValues := make([][]string, numGoroutines)
	allCommands := make([][]*SetOperation, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		allKeys[g] = make([]string, opsPerGoroutine)
		allValues[g] = make([]string, opsPerGoroutine)
		allCommands[g] = make([]*SetOperation, opsPerGoroutine)

		for i := 0; i < opsPerGoroutine; i++ {
			allKeys[g][i] = fmt.Sprintf("key_%d_%d", g, i)
			allValues[g][i] = generateRandomValue(100)
			allCommands[g][i] = &SetOperation{Key: allKeys[g][i], Value: allValues[g][i]}
		}
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			commands := allCommands[goroutineID]
			for i := 0; i < opsPerGoroutine; i++ {
				_, err := store.Execute(commands[i])
				if err != nil {
					b.Error(err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
}

func BenchmarkConcurrentGet(b *testing.B) {
	store, cleanup := setupStore(10000)
	defer cleanup()

	// Pre-populate store
	numKeys := 10000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		keys[i] = key

		cmd := &SetOperation{Key: key, Value: value}
		store.Execute(cmd)
	}

	numGoroutines := runtime.NumCPU()
	opsPerGoroutine := b.N / numGoroutines

	// Pre-generate all tasks for all goroutines
	allCommands := make([][]*GetOperation, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		allCommands[g] = make([]*GetOperation, opsPerGoroutine)
		for i := 0; i < opsPerGoroutine; i++ {
			keyIndex := rand.Intn(numKeys)
			allCommands[g][i] = &GetOperation{Key: keys[keyIndex]}
		}
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			commands := allCommands[goroutineID]
			for i := 0; i < opsPerGoroutine; i++ {
				_, err := store.Execute(commands[i])
				if err != nil {
					b.Error(err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
}

func BenchmarkConcurrentMixed(b *testing.B) {
	store, cleanup := setupStore(10000)
	defer cleanup()

	// Pre-populate store
	baseKeys := 1000
	keys := make([]string, baseKeys)
	for i := 0; i < baseKeys; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		keys[i] = key

		cmd := &SetOperation{Key: key, Value: value}
		store.Execute(cmd)
	}

	numGoroutines := runtime.NumCPU()
	opsPerGoroutine := b.N / numGoroutines

	// Pre-generate all operations and data for all goroutines
	type opData struct {
		opType float32
		getCmd *GetOperation
		setCmd *SetOperation
		delCmd *DelOperation
	}

	allOps := make([][]opData, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		allOps[g] = make([]opData, opsPerGoroutine)
		for i := 0; i < opsPerGoroutine; i++ {
			op := rand.Float32()
			allOps[g][i].opType = op

			switch {
			case op < 0.7: // 70% reads
				keyIndex := rand.Intn(len(keys))
				allOps[g][i].getCmd = &GetOperation{Key: keys[keyIndex]}
			case op < 0.9: // 20% writes
				key := fmt.Sprintf("key_%d_%d", g, i)
				value := generateRandomValue(100)
				allOps[g][i].setCmd = &SetOperation{Key: key, Value: value}
			default: // 10% deletes
				keyIndex := rand.Intn(len(keys))
				allOps[g][i].delCmd = &DelOperation{Key: keys[keyIndex]}
			}
		}
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			ops := allOps[goroutineID]
			for i := 0; i < opsPerGoroutine; i++ {
				op := ops[i]
				switch {
				case op.opType < 0.7: // 70% reads
					store.Execute(op.getCmd)
				case op.opType < 0.9: // 20% writes
					store.Execute(op.setCmd)
				default: // 10% deletes
					store.Execute(op.delCmd)
				}
			}
		}(g)
	}

	wg.Wait()
}

// Buffer size impact benchmarks
func BenchmarkBufferSize_Small(b *testing.B) {
	benchmarkWithBufferSize(b, 10)
}

func BenchmarkBufferSize_Medium(b *testing.B) {
	benchmarkWithBufferSize(b, 1000)
}

func BenchmarkBufferSize_Large(b *testing.B) {
	benchmarkWithBufferSize(b, 10000)
}

func benchmarkWithBufferSize(b *testing.B, bufferSize int) {
	store, cleanup := setupStore(bufferSize)
	defer cleanup()

	// Pre-generate all data
	commands := make([]*SetOperation, b.N)
	for i := 0; i < b.N; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		commands[i] = &SetOperation{Key: key, Value: value}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Execute(commands[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Value size impact benchmarks
func BenchmarkValueSize_Small(b *testing.B) {
	benchmarkWithValueSize(b, 10)
}

func BenchmarkValueSize_Medium(b *testing.B) {
	benchmarkWithValueSize(b, 1000)
}

func BenchmarkValueSize_Large(b *testing.B) {
	benchmarkWithValueSize(b, 10000)
}

func benchmarkWithValueSize(b *testing.B, valueSize int) {
	store, cleanup := setupStore(1000)
	defer cleanup()

	// Pre-generate all data
	commands := make([]*SetOperation, b.N)
	for i := 0; i < b.N; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(valueSize)
		commands[i] = &SetOperation{Key: key, Value: value}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Execute(commands[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Latency benchmarks using Submit (async)
func BenchmarkAsyncSubmit(b *testing.B) {
	store, cleanup := setupStore(10000)
	defer cleanup()

	// Pre-generate all tasks
	commands := make([]*SetOperation, b.N)
	for i := 0; i < b.N; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		commands[i] = &SetOperation{Key: key, Value: value}
	}

	b.ResetTimer()

	results := make([]*Future[any], b.N)

	// Submit all tasks
	start := time.Now()
	for i := 0; i < b.N; i++ {
		results[i] = store.Submit(commands[i])
	}
	submitTime := time.Since(start)

	// Wait for all results
	start = time.Now()
	for i := 0; i < b.N; i++ {
		_, err := results[i].Get()
		if err != nil {
			b.Fatal(err)
		}
	}
	waitTime := time.Since(start)

	b.ReportMetric(float64(submitTime.Nanoseconds())/float64(b.N), "submit-ns/op")
	b.ReportMetric(float64(waitTime.Nanoseconds())/float64(b.N), "wait-ns/op")
}

// Memory usage benchmark
func BenchmarkMemoryUsage(b *testing.B) {
	store, cleanup := setupStore(1000)
	defer cleanup()

	// Pre-generate all tasks
	commands := make([]*SetOperation, b.N)
	for i := 0; i < b.N; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		commands[i] = &SetOperation{Key: key, Value: value}
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Execute(commands[i])
		if err != nil {
			b.Fatal(err)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	b.ReportMetric(float64(m2.Alloc-m1.Alloc)/float64(b.N), "bytes/op")
}

// Stress test - high contention
func BenchmarkHighContention(b *testing.B) {
	store, cleanup := setupStore(10000) // Small buffer to create contention
	defer cleanup()

	numGoroutines := runtime.NumCPU() * 100 // More goroutines than CPUs
	opsPerGoroutine := b.N / numGoroutines

	// Pre-generate all tasks for all goroutines
	allCommands := make([][]*SetOperation, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		allCommands[g] = make([]*SetOperation, opsPerGoroutine)
		for i := 0; i < opsPerGoroutine; i++ {
			key := generateRandomKey(10)
			value := generateRandomValue(100)
			allCommands[g][i] = &SetOperation{Key: key, Value: value}
		}
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			commands := allCommands[goroutineID]
			for i := 0; i < opsPerGoroutine; i++ {
				_, err := store.Execute(commands[i])
				if err != nil {
					b.Error(err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
}
