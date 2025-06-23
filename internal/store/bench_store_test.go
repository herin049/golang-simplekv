package store

import (
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
	store := NewStore(nil, StoreOptions{
		TaskBufferDepth:  bufferSize,
		MaxTaskBatchSize: 1024,
	})

	// Start the store in a goroutine
	go store.Run()

	// Return cleanup function
	cleanup := func() {
		store.Stop()
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
	commands := make([]SetCommand, b.N)
	for i := 0; i < b.N; i++ {
		commands[i] = SetCommand{Key: keys[i], Value: values[i]}
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

		cmd := SetCommand{Key: key, Value: value}
		store.Execute(cmd)
	}

	// Pre-generate random indices for key selection
	keyIndices := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		keyIndices[i] = rand.Intn(numKeys)
	}

	// Pre-create all GET tasks
	commands := make([]GetCommand, b.N)
	for i := 0; i < b.N; i++ {
		commands[i] = GetCommand{Key: keys[keyIndices[i]]}
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

		cmd := SetCommand{Key: key, Value: value}
		store.Execute(cmd)
	}

	// Pre-create all DELETE tasks
	commands := make([]DelCommand, b.N)
	for i := 0; i < b.N; i++ {
		commands[i] = DelCommand{Key: keys[i]}
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

		cmd := SetCommand{Key: key, Value: value}
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
			cmd := GetCommand{Key: keys[readIndices[i]]}
			store.Execute(cmd)
		case op < 0.9: // 20% writes
			cmd := SetCommand{Key: writeKeys[i], Value: writeValues[i]}
			store.Execute(cmd)
		default: // 10% deletes
			cmd := DelCommand{Key: keys[deleteIndices[i]]}
			store.Execute(cmd)
		}
	}
}

// Concurrency benchmarks
func BenchmarkConcurrentSet(b *testing.B) {
	store, cleanup := setupStore(100000)
	defer cleanup()

	numGoroutines := runtime.NumCPU() * 100
	opsPerGoroutine := b.N / numGoroutines

	// Pre-generate all data for all goroutines
	allKeys := make([][]string, numGoroutines)
	allValues := make([][]string, numGoroutines)
	allCommands := make([][]SetCommand, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		allKeys[g] = make([]string, opsPerGoroutine)
		allValues[g] = make([]string, opsPerGoroutine)
		allCommands[g] = make([]SetCommand, opsPerGoroutine)

		for i := 0; i < opsPerGoroutine; i++ {
			allKeys[g][i] = fmt.Sprintf("key_%d_%d", g, i)
			allValues[g][i] = generateRandomValue(100)
			allCommands[g][i] = SetCommand{Key: allKeys[g][i], Value: allValues[g][i]}
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

		cmd := SetCommand{Key: key, Value: value}
		store.Execute(cmd)
	}

	numGoroutines := runtime.NumCPU()
	opsPerGoroutine := b.N / numGoroutines

	// Pre-generate all tasks for all goroutines
	allCommands := make([][]GetCommand, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		allCommands[g] = make([]GetCommand, opsPerGoroutine)
		for i := 0; i < opsPerGoroutine; i++ {
			keyIndex := rand.Intn(numKeys)
			allCommands[g][i] = GetCommand{Key: keys[keyIndex]}
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

		cmd := SetCommand{Key: key, Value: value}
		store.Execute(cmd)
	}

	numGoroutines := runtime.NumCPU()
	opsPerGoroutine := b.N / numGoroutines

	// Pre-generate all operations and data for all goroutines
	type opData struct {
		opType float32
		getCmd GetCommand
		setCmd SetCommand
		delCmd DelCommand
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
				allOps[g][i].getCmd = GetCommand{Key: keys[keyIndex]}
			case op < 0.9: // 20% writes
				key := fmt.Sprintf("key_%d_%d", g, i)
				value := generateRandomValue(100)
				allOps[g][i].setCmd = SetCommand{Key: key, Value: value}
			default: // 10% deletes
				keyIndex := rand.Intn(len(keys))
				allOps[g][i].delCmd = DelCommand{Key: keys[keyIndex]}
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
	commands := make([]SetCommand, b.N)
	for i := 0; i < b.N; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		commands[i] = SetCommand{Key: key, Value: value}
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
	commands := make([]SetCommand, b.N)
	for i := 0; i < b.N; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(valueSize)
		commands[i] = SetCommand{Key: key, Value: value}
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
	commands := make([]SetCommand, b.N)
	for i := 0; i < b.N; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		commands[i] = SetCommand{Key: key, Value: value}
	}

	b.ResetTimer()

	results := make([]*Future[CommandResult], b.N)

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
	commands := make([]SetCommand, b.N)
	for i := 0; i < b.N; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		commands[i] = SetCommand{Key: key, Value: value}
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
	allCommands := make([][]SetCommand, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		allCommands[g] = make([]SetCommand, opsPerGoroutine)
		for i := 0; i < opsPerGoroutine; i++ {
			key := generateRandomKey(10)
			value := generateRandomValue(100)
			allCommands[g][i] = SetCommand{Key: key, Value: value}
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

// Add these benchmarks to your existing benchmark file

// Batch operation benchmarks
func BenchmarkBatchSet_10(b *testing.B) {
	benchmarkBatchSet(b, 10)
}

func BenchmarkBatchSet_100(b *testing.B) {
	benchmarkBatchSet(b, 100)
}

func BenchmarkBatchSet_1000(b *testing.B) {
	benchmarkBatchSet(b, 1000)
}

func benchmarkBatchSet(b *testing.B, batchSize int) {
	store, cleanup := setupStore(10000)
	defer cleanup()

	// Calculate number of batches needed
	numBatches := b.N / batchSize
	if b.N%batchSize != 0 {
		numBatches++
	}

	// Pre-generate all batches
	allBatches := make([][]Command, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		currentBatchSize := batchSize
		if batchIdx == numBatches-1 && b.N%batchSize != 0 {
			currentBatchSize = b.N % batchSize
		}

		batch := make([]Command, currentBatchSize)
		for i := 0; i < currentBatchSize; i++ {
			key := generateRandomKey(10)
			value := generateRandomValue(100)
			batch[i] = SetCommand{Key: key, Value: value}
		}
		allBatches[batchIdx] = batch
	}

	b.ResetTimer()

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		results, errors := store.ExecuteBatch(allBatches[batchIdx])
		for i, err := range errors {
			if err != nil {
				b.Fatalf("Batch operation %d failed: %v", i, err)
			}
		}
		_ = results // Avoid unused variable warning
	}
}

func BenchmarkBatchGet_10(b *testing.B) {
	benchmarkBatchGet(b, 10)
}

func BenchmarkBatchGet_100(b *testing.B) {
	benchmarkBatchGet(b, 100)
}

func BenchmarkBatchGet_1000(b *testing.B) {
	benchmarkBatchGet(b, 1000)
}

func benchmarkBatchGet(b *testing.B, batchSize int) {
	store, cleanup := setupStore(10000)
	defer cleanup()

	// Pre-populate store with keys
	numKeys := 10000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		keys[i] = key

		cmd := SetCommand{Key: key, Value: value}
		store.Execute(cmd)
	}

	// Calculate number of batches needed
	numBatches := b.N / batchSize
	if b.N%batchSize != 0 {
		numBatches++
	}

	// Pre-generate all batches
	allBatches := make([][]Command, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		currentBatchSize := batchSize
		if batchIdx == numBatches-1 && b.N%batchSize != 0 {
			currentBatchSize = b.N % batchSize
		}

		batch := make([]Command, currentBatchSize)
		for i := 0; i < currentBatchSize; i++ {
			keyIndex := rand.Intn(numKeys)
			batch[i] = GetCommand{Key: keys[keyIndex]}
		}
		allBatches[batchIdx] = batch
	}

	b.ResetTimer()

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		results, errors := store.ExecuteBatch(allBatches[batchIdx])
		for i, err := range errors {
			if err != nil {
				b.Fatalf("Batch operation %d failed: %v", i, err)
			}
		}
		_ = results // Avoid unused variable warning
	}
}

func BenchmarkBatchMixed_10(b *testing.B) {
	benchmarkBatchMixed(b, 10)
}

func BenchmarkBatchMixed_100(b *testing.B) {
	benchmarkBatchMixed(b, 100)
}

func BenchmarkBatchMixed_1000(b *testing.B) {
	benchmarkBatchMixed(b, 1000)
}

func benchmarkBatchMixed(b *testing.B, batchSize int) {
	store, cleanup := setupStore(10000)
	defer cleanup()

	// Pre-populate store
	baseKeys := 1000
	keys := make([]string, baseKeys)
	for i := 0; i < baseKeys; i++ {
		key := generateRandomKey(10)
		value := generateRandomValue(100)
		keys[i] = key

		cmd := SetCommand{Key: key, Value: value}
		store.Execute(cmd)
	}

	// Calculate number of batches needed
	numBatches := b.N / batchSize
	if b.N%batchSize != 0 {
		numBatches++
	}

	// Pre-generate all batches
	allBatches := make([][]Command, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		currentBatchSize := batchSize
		if batchIdx == numBatches-1 && b.N%batchSize != 0 {
			currentBatchSize = b.N % batchSize
		}

		batch := make([]Command, currentBatchSize)
		for i := 0; i < currentBatchSize; i++ {
			op := rand.Float32()
			switch {
			case op < 0.7: // 70% reads
				keyIndex := rand.Intn(len(keys))
				batch[i] = GetCommand{Key: keys[keyIndex]}
			case op < 0.9: // 20% writes
				key := generateRandomKey(10)
				value := generateRandomValue(100)
				batch[i] = SetCommand{Key: key, Value: value}
			default: // 10% deletes
				keyIndex := rand.Intn(len(keys))
				batch[i] = DelCommand{Key: keys[keyIndex]}
			}
		}
		allBatches[batchIdx] = batch
	}

	b.ResetTimer()

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		results, errors := store.ExecuteBatch(allBatches[batchIdx])
		for i, err := range errors {
			if err != nil && err != KeyNotFoundError {
				b.Fatalf("Batch operation %d failed: %v", i, err)
			}
		}
		_ = results // Avoid unused variable warning
	}
}

// Async batch benchmarks using SubmitBatch
func BenchmarkAsyncBatchSubmit_10(b *testing.B) {
	benchmarkAsyncBatchSubmit(b, 10)
}

func BenchmarkAsyncBatchSubmit_100(b *testing.B) {
	benchmarkAsyncBatchSubmit(b, 100)
}

func BenchmarkAsyncBatchSubmit_1000(b *testing.B) {
	benchmarkAsyncBatchSubmit(b, 1000)
}

func benchmarkAsyncBatchSubmit(b *testing.B, batchSize int) {
	store, cleanup := setupStore(10000)
	defer cleanup()

	// Calculate number of batches needed
	numBatches := b.N / batchSize
	if b.N%batchSize != 0 {
		numBatches++
	}

	// Pre-generate all batches
	allBatches := make([][]Command, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		currentBatchSize := batchSize
		if batchIdx == numBatches-1 && b.N%batchSize != 0 {
			currentBatchSize = b.N % batchSize
		}

		batch := make([]Command, currentBatchSize)
		for i := 0; i < currentBatchSize; i++ {
			key := generateRandomKey(10)
			value := generateRandomValue(100)
			batch[i] = SetCommand{Key: key, Value: value}
		}
		allBatches[batchIdx] = batch
	}

	b.ResetTimer()

	allFutures := make([][]*Future[CommandResult], numBatches)

	// Submit all batches
	start := time.Now()
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		allFutures[batchIdx] = store.SubmitBatch(allBatches[batchIdx])
	}
	submitTime := time.Since(start)

	// Wait for all results
	start = time.Now()
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		futures := allFutures[batchIdx]
		for _, future := range futures {
			_, err := future.Get()
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	waitTime := time.Since(start)

	b.ReportMetric(float64(submitTime.Nanoseconds())/float64(b.N), "submit-ns/op")
	b.ReportMetric(float64(waitTime.Nanoseconds())/float64(b.N), "wait-ns/op")
}

// Concurrent batch benchmarks
func BenchmarkConcurrentBatch_10(b *testing.B) {
	benchmarkConcurrentBatch(b, 10)
}

func BenchmarkConcurrentBatch_100(b *testing.B) {
	benchmarkConcurrentBatch(b, 100)
}

func BenchmarkConcurrentBatch_1000(b *testing.B) {
	benchmarkConcurrentBatch(b, 1000)
}

func benchmarkConcurrentBatch(b *testing.B, batchSize int) {
	store, cleanup := setupStore(100000)
	defer cleanup()

	numGoroutines := runtime.NumCPU()
	opsPerGoroutine := b.N / numGoroutines
	batchesPerGoroutine := opsPerGoroutine / batchSize
	if opsPerGoroutine%batchSize != 0 {
		batchesPerGoroutine++
	}

	// Pre-generate all batches for all goroutines
	allGoroutineBatches := make([][][]Command, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		allGoroutineBatches[g] = make([][]Command, batchesPerGoroutine)

		for batchIdx := 0; batchIdx < batchesPerGoroutine; batchIdx++ {
			currentBatchSize := batchSize
			if batchIdx == batchesPerGoroutine-1 && opsPerGoroutine%batchSize != 0 {
				currentBatchSize = opsPerGoroutine % batchSize
			}

			batch := make([]Command, currentBatchSize)
			for i := 0; i < currentBatchSize; i++ {
				key := fmt.Sprintf("key_%d_%d_%d", g, batchIdx, i)
				value := generateRandomValue(100)
				batch[i] = SetCommand{Key: key, Value: value}
			}
			allGoroutineBatches[g][batchIdx] = batch
		}
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			batches := allGoroutineBatches[goroutineID]
			for _, batch := range batches {
				results, errors := store.ExecuteBatch(batch)
				for i, err := range errors {
					if err != nil {
						b.Errorf("Goroutine %d batch operation %d failed: %v", goroutineID, i, err)
						return
					}
				}
				_ = results // Avoid unused variable warning
			}
		}(g)
	}

	wg.Wait()
}

// Batch vs Individual operation comparison
func BenchmarkBatchVsIndividual_Set_100(b *testing.B) {
	b.Run("Individual", func(b *testing.B) {
		store, cleanup := setupStore(1000)
		defer cleanup()

		commands := make([]SetCommand, b.N)
		for i := 0; i < b.N; i++ {
			key := generateRandomKey(10)
			value := generateRandomValue(100)
			commands[i] = SetCommand{Key: key, Value: value}
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := store.Execute(commands[i])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Batch", func(b *testing.B) {
		store, cleanup := setupStore(10000)
		defer cleanup()

		batchSize := 100
		numBatches := b.N / batchSize
		if b.N%batchSize != 0 {
			numBatches++
		}

		allBatches := make([][]Command, numBatches)
		for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
			currentBatchSize := batchSize
			if batchIdx == numBatches-1 && b.N%batchSize != 0 {
				currentBatchSize = b.N % batchSize
			}

			batch := make([]Command, currentBatchSize)
			for i := 0; i < currentBatchSize; i++ {
				key := generateRandomKey(10)
				value := generateRandomValue(100)
				batch[i] = SetCommand{Key: key, Value: value}
			}
			allBatches[batchIdx] = batch
		}

		b.ResetTimer()

		for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
			results, errors := store.ExecuteBatch(allBatches[batchIdx])
			for i, err := range errors {
				if err != nil {
					b.Fatalf("Batch operation %d failed: %v", i, err)
				}
			}
			_ = results
		}
	})
}

// Test impact of MaxTaskBatchSize configuration
func BenchmarkBatchWithMaxTaskBatchSize_10(b *testing.B) {
	benchmarkBatchWithMaxTaskBatchSize(b, 10)
}

func BenchmarkBatchWithMaxTaskBatchSize_100(b *testing.B) {
	benchmarkBatchWithMaxTaskBatchSize(b, 100)
}

func BenchmarkBatchWithMaxTaskBatchSize_1000(b *testing.B) {
	benchmarkBatchWithMaxTaskBatchSize(b, 1000)
}

func benchmarkBatchWithMaxTaskBatchSize(b *testing.B, maxTaskBatchSize int) {
	store := NewStore(nil, StoreOptions{
		TaskBufferDepth:  10000,
		MaxTaskBatchSize: maxTaskBatchSize,
	})

	go store.Run()

	cleanup := func() {
		store.Stop()
	}
	defer cleanup()

	// Create a large batch that will be split by MaxTaskBatchSize
	batchSize := 1000
	numBatches := b.N / batchSize
	if b.N%batchSize != 0 {
		numBatches++
	}

	allBatches := make([][]Command, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		currentBatchSize := batchSize
		if batchIdx == numBatches-1 && b.N%batchSize != 0 {
			currentBatchSize = b.N % batchSize
		}

		batch := make([]Command, currentBatchSize)
		for i := 0; i < currentBatchSize; i++ {
			key := generateRandomKey(10)
			value := generateRandomValue(100)
			batch[i] = SetCommand{Key: key, Value: value}
		}
		allBatches[batchIdx] = batch
	}

	b.ResetTimer()

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		results, errors := store.ExecuteBatch(allBatches[batchIdx])
		for i, err := range errors {
			if err != nil {
				b.Fatalf("Batch operation %d failed: %v", i, err)
			}
		}
		_ = results
	}
}

// Large batch stress test
func BenchmarkLargeBatch_10000(b *testing.B) {
	store, cleanup := setupStore(100000)
	defer cleanup()

	batchSize := 10000
	numBatches := b.N / batchSize
	if b.N%batchSize != 0 {
		numBatches++
	}

	allBatches := make([][]Command, numBatches)
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		currentBatchSize := batchSize
		if batchIdx == numBatches-1 && b.N%batchSize != 0 {
			currentBatchSize = b.N % batchSize
		}

		batch := make([]Command, currentBatchSize)
		for i := 0; i < currentBatchSize; i++ {
			key := generateRandomKey(10)
			value := generateRandomValue(100)
			batch[i] = SetCommand{Key: key, Value: value}
		}
		allBatches[batchIdx] = batch
	}

	b.ResetTimer()

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		results, errors := store.ExecuteBatch(allBatches[batchIdx])
		for i, err := range errors {
			if err != nil {
				b.Fatalf("Batch operation %d failed: %v", i, err)
			}
		}
		_ = results
	}
}
