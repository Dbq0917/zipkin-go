// Copyright 2022 The OpenZipkin Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package async

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	zipkin "github.com/openzipkin/zipkin-go"
)

var (
	// ErrPoolClosed is returned when a task is submitted to a Pool after Close.
	ErrPoolClosed = errors.New("async: pool is closed")

	// ErrPoolBusy is returned by TrySubmit when the pool queue is full.
	ErrPoolBusy = errors.New("async: pool queue is full")
)

// PanicHandler receives the recovered value of a panicking task. When no
// handler is configured a panic propagates and crashes the process, matching
// the behavior of an unguarded goroutine. When a handler is set the worker
// recovers and keeps serving subsequent tasks.
type PanicHandler func(recovered any)

// Pool runs tasks on a fixed set of long-lived, reused workers. Each task
// captures the submitting goroutine's context into its own immutable snapshot
// and restores only that snapshot during execution. Workers never store request
// state, so reusing them cannot leak one request's trace context into another.
type Pool struct {
	in      *Instrumenter
	onPanic PanicHandler
	tasks   chan func()
	done    chan struct{}
	wg      sync.WaitGroup
	once    sync.Once
	closed  atomic.Bool
}

// PoolOption customizes a Pool.
type PoolOption func(*Pool)

// WithPoolTracer instruments Go and GoErr tasks with child spans created by
// tracer. Submit and SubmitErr remain propagation-only regardless.
func WithPoolTracer(tracer *zipkin.Tracer, opts ...InstrumenterOption) PoolOption {
	return func(p *Pool) {
		p.in = NewInstrumenter(tracer, opts...)
	}
}

// WithPoolInstrumenter attaches an existing Instrumenter to the Pool.
func WithPoolInstrumenter(in *Instrumenter) PoolOption {
	return func(p *Pool) {
		p.in = in
	}
}

// WithPanicHandler recovers panicking tasks and forwards them to handler.
func WithPanicHandler(handler PanicHandler) PoolOption {
	return func(p *Pool) {
		p.onPanic = handler
	}
}

// NewPool creates a Pool with workers reused goroutines backed by a buffered
// task queue of queueSize. workers must be greater than zero. A zero queueSize
// makes the queue unbuffered, so Submit blocks until a worker is ready.
func NewPool(workers, queueSize int, opts ...PoolOption) *Pool {
	if workers <= 0 {
		panic("async: NewPool requires at least one worker")
	}
	p := &Pool{
		tasks: make(chan func(), queueSize),
		done:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			p.drain()
			return
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			p.execute(task)
		}
	}
}

// drain runs tasks already accepted before Close. New submissions are rejected
// once done is closed, so the queue only shrinks from here.
func (p *Pool) drain() {
	for {
		select {
		case task := <-p.tasks:
			p.execute(task)
		default:
			return
		}
	}
}

func (p *Pool) execute(task func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if p.onPanic == nil {
				panic(recovered)
			}
			p.onPanic(recovered)
		}
	}()
	task()
}

// Submit enqueues fn, propagating ctx into the worker without creating a span.
// The returned error only describes enqueueing; errors produced by fn must be
// handled inside fn or recorded via GoErr.
func (p *Pool) Submit(ctx context.Context, fn func(context.Context)) error {
	snapshot := Snapshot(ctx)
	return p.enqueue(true, func() { fn(snapshot) })
}

// SubmitErr is Submit for tasks returning an error. The task error is not
// propagated back to the submitter; use GoErr when it should be tagged on a
// span, or handle it inside fn.
func (p *Pool) SubmitErr(ctx context.Context, fn func(context.Context) error) error {
	snapshot := Snapshot(ctx)
	return p.enqueue(true, func() { _ = fn(snapshot) })
}

// Go enqueues fn under a child span named name. When the Pool has no
// configured tracer it behaves like Submit and only propagates the context.
func (p *Pool) Go(ctx context.Context, name string, fn func(context.Context)) error {
	return p.enqueue(true, p.task(ctx, name, fn))
}

// GoErr is Go for tasks returning an error. A non-nil error is tagged on the
// child span (when instrumentation is enabled) before being discarded.
func (p *Pool) GoErr(ctx context.Context, name string, fn func(context.Context) error) error {
	return p.enqueue(true, p.taskErr(ctx, name, fn))
}

// TrySubmit enqueues fn without blocking. It returns ErrPoolBusy if the queue
// is full and ErrPoolClosed if the Pool is closed.
func (p *Pool) TrySubmit(ctx context.Context, fn func(context.Context)) error {
	snapshot := Snapshot(ctx)
	return p.enqueue(false, func() { fn(snapshot) })
}

func (p *Pool) task(ctx context.Context, name string, fn func(context.Context)) func() {
	if p.in == nil {
		snapshot := Snapshot(ctx)
		return func() { fn(snapshot) }
	}
	return p.in.Wrap(ctx, name, fn)
}

func (p *Pool) taskErr(ctx context.Context, name string, fn func(context.Context) error) func() {
	if p.in == nil {
		snapshot := Snapshot(ctx)
		return func() { _ = fn(snapshot) }
	}
	wrapped := p.in.WrapErr(ctx, name, fn)
	return func() { _ = wrapped() }
}

func (p *Pool) enqueue(block bool, task func()) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}
	if block {
		select {
		case <-p.done:
			return ErrPoolClosed
		case p.tasks <- task:
			return nil
		}
	}
	select {
	case <-p.done:
		return ErrPoolClosed
	case p.tasks <- task:
		return nil
	default:
		return ErrPoolBusy
	}
}

// Close stops accepting new tasks and blocks until every queued and running
// task has finished. It is safe to call concurrently with submissions and to
// call more than once.
func (p *Pool) Close() {
	p.once.Do(func() {
		p.closed.Store(true)
		close(p.done)
	})
	p.wg.Wait()
}
