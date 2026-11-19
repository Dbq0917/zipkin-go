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
	"strconv"
	"sync"

	zipkin "github.com/openzipkin/zipkin-go"
)

// Executor schedules a task without blocking. *Pool satisfies this interface.
// Third-party worker pools can be adapted as long as they expose a non-blocking
// enqueue operation; blocking enqueue would let a saturated queue deadlock a
// batch that waits for its sub-tasks to complete.
type Executor interface {
	TrySubmit(context.Context, func(context.Context)) error
}

// Batch groups a set of sub-tasks under one parent span. The parent batch span
// is opened from the submitting goroutine's context, and every sub-task is
// wrapped with that span's context, so each sub-task appears as a child of the
// batch instead of an independent trace root. Add tasks before calling Run.
type Batch struct {
	in       *Instrumenter
	executor Executor
	tasks    []func(context.Context) error
}

// NewBatch creates a Batch. tracer may be nil to propagate context without
// creating spans, and executor may be nil to run the sub-tasks sequentially in
// the calling goroutine.
func NewBatch(tracer *zipkin.Tracer, executor Executor, opts ...InstrumenterOption) *Batch {
	return &Batch{
		in:       NewInstrumenter(tracer, opts...),
		executor: executor,
	}
}

// Add registers fn as a sub-task named name. It must be called before Run.
func (b *Batch) Add(name string, fn func(context.Context) error) {
	b.tasks = append(b.tasks, func(ctx context.Context) error {
		return b.in.WrapErr(ctx, name, fn)()
	})
}

// Run opens the batch span, executes every sub-task, waits for completion and
// finishes the span. Sub-tasks run on the configured Executor when present,
// otherwise sequentially in the caller's goroutine. It returns the join of all
// sub-task errors (nil when every sub-task succeeds) and tags the batch span on
// failure. Do not reuse a Batch after Run.
func (b *Batch) Run(ctx context.Context, name string) error {
	batchCtx, span := b.in.StartSpan(ctx, name)
	defer finish(span)

	if span != nil {
		span.Tag("batch.size", strconv.Itoa(len(b.tasks)))
	}

	var (
		wg     sync.WaitGroup
		errMu  sync.Mutex
		joined error
	)

	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		joined = errors.Join(joined, err)
		errMu.Unlock()
	}

	for _, task := range b.tasks {
		task := task
		if b.executor == nil {
			recordErr(task(batchCtx))
			continue
		}

		wg.Add(1)
		err := b.executor.TrySubmit(batchCtx, func(runCtx context.Context) {
			defer wg.Done()
			recordErr(task(runCtx))
		})
		if err != nil {
			wg.Done()
			if errors.Is(err, ErrPoolBusy) || errors.Is(err, ErrPoolClosed) {
				// Saturated or closed queues are back-pressure, not task
				// failures. Running inline avoids deadlock while preserving
				// the trace tree.
				recordErr(task(batchCtx))
				continue
			}
			recordErr(err)
			recordErr(task(batchCtx))
		}
	}

	wg.Wait()

	if joined != nil && span != nil {
		span.Tag(string(zipkin.TagError), joined.Error())
	}
	return joined
}
