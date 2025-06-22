package store

import (
	"context"
	"sync"
	"time"
)

type Future[T any] struct {
	lock  sync.Mutex
	cond  *sync.Cond
	value T
	err   error
	done  bool
}

func NewFuture[T any]() *Future[T] {
	f := &Future[T]{}
	f.cond = sync.NewCond(&f.lock)
	return f
}
func (f *Future[T]) Set(value T) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.done {
		return
	}
	f.value = value
	f.done = true
	f.cond.Broadcast()
}

func (f *Future[T]) SetErr(err error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.done {
		return
	}
	f.err = err
	f.done = true
	f.cond.Broadcast()
}
func (f *Future[T]) Get() (T, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for !f.done {
		f.cond.Wait()
	}
	return f.value, f.err
}

func (f *Future[T]) GetWithContext(ctx context.Context) (T, error) {
	stop := context.AfterFunc(ctx, func() {
		f.lock.Lock()
		defer f.lock.Unlock()
		f.cond.Broadcast()
	})
	defer stop()
	f.lock.Lock()
	defer f.lock.Unlock()
	for !f.done {
		if ctx.Err() != nil {
			return f.value, ctx.Err()
		}
		f.cond.Wait()
	}
	return f.value, f.err
}

func (f *Future[T]) GetWithTimeout(timeout time.Duration) (T, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.GetWithContext(timeoutCtx)
}

func (f *Future[T]) IsComplete() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.done
}
