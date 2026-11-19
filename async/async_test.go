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
	"testing"
	"time"

	zipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
)

func newTestTracer(t *testing.T) (*zipkin.Tracer, *recorder.ReporterRecorder) {
	t.Helper()
	rec := recorder.NewReporter()
	t.Cleanup(func() { _ = rec.Close() })
	tracer, err := zipkin.NewTracer(rec)
	if err != nil {
		t.Fatalf("unexpected tracer error: %v", err)
	}
	return tracer, rec
}

func startRoot(t *testing.T, tracer *zipkin.Tracer) (zipkin.Span, context.Context) {
	t.Helper()
	root, ctx := tracer.StartSpanFromContext(context.Background(), "root")
	t.Cleanup(root.Finish)
	return root, ctx
}

func TestSnapshotDetachedFromCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	tracer, _ := newTestTracer(t)
	root, ctx := startRoot(t, tracer)
	snapshot := Snapshot(ctx)
	cancel()

	if got := zipkin.SpanFromContext(snapshot); got == nil {
		t.Fatal("expected span preserved in snapshot")
	} else if got.Context().TraceID != root.Context().TraceID {
		t.Fatal("snapshot lost the parent span")
	}
	if err := snapshot.Err(); err != nil {
		t.Fatalf("snapshot must ignore caller cancellation, got: %v", err)
	}
}

func TestInstrumenterWrapsAsChildSpan(t *testing.T) {
	tracer, rec := newTestTracer(t)
	root, ctx := startRoot(t, tracer)

	done := make(chan struct{})
	NewInstrumenter(tracer).Wrap(ctx, "async-work", func(runCtx context.Context) {
		span := zipkin.SpanFromContext(runCtx)
		if span == nil {
			t.Error("expected span on worker context")
		}
		if span.Context().TraceID != root.Context().TraceID {
			t.Error("child span belongs to a different trace")
		}
		close(done)
	})()

	<-done
	root.Finish()

	spans := rec.Flush()
	if len(spans) != 2 {
		t.Fatalf("expected root and child spans, got %d", len(spans))
	}

	var child model.SpanModel
	for _, s := range spans {
		if s.Name == "async-work" {
			child = s
		}
	}
	if child.Name == "" {
		t.Fatal("async-work span not recorded")
	}
	if child.TraceID != root.Context().TraceID {
		t.Fatal("child span is a new trace root")
	}
	if child.ParentID == nil || *child.ParentID != root.Context().ID {
		t.Fatal("child span not linked to parent")
	}
}

func TestInstrumenterWrapsError(t *testing.T) {
	tracer, rec := newTestTracer(t)
	_, ctx := startRoot(t, tracer)

	wantErr := errors.New("boom")
	err := NewInstrumenter(tracer).WrapErr(ctx, "failing-work",
		func(context.Context) error { return wantErr })()
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped error, got %v", err)
	}

	var found bool
	for _, s := range rec.Flush() {
		if s.Name == "failing-work" && s.Tags[string(zipkin.TagError)] == "boom" {
			found = true
		}
	}
	if !found {
		t.Fatal("error not tagged on child span")
	}
}

func TestNilTracerPropagatesWithoutSpans(t *testing.T) {
	tracer, rec := newTestTracer(t)
	root, ctx := tracer.StartSpanFromContext(context.Background(), "root")

	var seen zipkin.Span
	NewInstrumenter(nil).Wrap(ctx, "no-span", func(runCtx context.Context) {
		seen = zipkin.SpanFromContext(runCtx)
	})()

	if seen == nil || seen.Context().TraceID != root.Context().TraceID {
		t.Fatal("parent context not propagated without tracer")
	}
	root.Finish()
	if spans := rec.Flush(); len(spans) != 1 {
		t.Fatalf("only the root span should be recorded, got %d", len(spans))
	}
}

func TestPoolReusedWorkersDoNotLeakContext(t *testing.T) {
	tracer, rec := newTestTracer(t)
	pool := NewPool(2, 4, WithPoolTracer(tracer))

	const requests = 50
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		root, ctx := tracer.StartSpanFromContext(context.Background(), "request")
		wg.Add(1)
		err := pool.Go(ctx, "callback", func(runCtx context.Context) {
			defer wg.Done()
			span := zipkin.SpanFromContext(runCtx)
			if span.Context().TraceID != root.Context().TraceID {
				t.Errorf("worker reused another request's context")
				return
			}
			child, childCtx := tracer.StartSpanFromContext(runCtx, "nested")
			defer child.Finish()
			if nested := zipkin.SpanFromContext(childCtx); nested.Context().TraceID !=
				root.Context().TraceID {
				t.Errorf("nested span left the request trace")
			}
		})
		if err != nil {
			t.Fatalf("unexpected submit error: %v", err)
		}
		root.Finish()
	}
	wg.Wait()
	pool.Close()

	spans := rec.Flush()
	if len(spans) != requests*3 {
		t.Fatalf("expected %d spans, got %d", requests*3, len(spans))
	}

	traceParents := make(map[model.TraceID]model.ID)
	for _, s := range spans {
		if s.Name == "request" {
			traceParents[s.TraceID] = s.ID
		}
	}
	if len(traceParents) != requests {
		t.Fatalf("expected %d distinct traces, got %d", requests, len(traceParents))
	}
	for _, s := range spans {
		if s.Name != "callback" && s.Name != "nested" {
			continue
		}
		if _, ok := traceParents[s.TraceID]; !ok {
			t.Fatalf("span %s became an independent root", s.Name)
		}
	}
}

func TestPoolTrySubmitAndCloseRejectTasks(t *testing.T) {
	pool := NewPool(1, 1)
	release := make(chan struct{})
	if err := pool.Submit(context.Background(), func(context.Context) {
		<-release
	}); err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}

	if err := pool.TrySubmit(context.Background(), func(context.Context) {}); !errors.Is(err, ErrPoolBusy) {
		t.Fatalf("expected ErrPoolBusy, got %v", err)
	}

	close(release)
	pool.Close()

	if err := pool.Submit(context.Background(), func(context.Context) {}); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
	if err := pool.TrySubmit(context.Background(), func(context.Context) {}); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPoolCloseDrainsQueuedTasks(t *testing.T) {
	pool := NewPool(1, 8)
	var mu sync.Mutex
	ran := 0

	for i := 0; i < 5; i++ {
		if err := pool.Submit(context.Background(), func(context.Context) {
			time.Sleep(time.Millisecond)
			mu.Lock()
			ran++
			mu.Unlock()
		}); err != nil {
			t.Fatalf("unexpected submit error: %v", err)
		}
	}

	pool.Close()
	if ran != 5 {
		t.Fatalf("Close must drain queued tasks, ran %d", ran)
	}
}

func TestPoolPanicHandler(t *testing.T) {
	recovered := make(chan any, 1)
	pool := NewPool(1, 2, WithPanicHandler(func(v any) {
		recovered <- v
	}))

	if err := pool.Submit(context.Background(), func(context.Context) {
		panic("kaboom")
	}); err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}

	select {
	case v := <-recovered:
		if v != "kaboom" {
			t.Fatalf("unexpected recovered value: %v", v)
		}
	case <-time.After(time.Second):
		t.Fatal("panic handler not invoked")
	}

	after := make(chan struct{})
	if err := pool.Submit(context.Background(), func(context.Context) {
		close(after)
	}); err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}
	select {
	case <-after:
	case <-time.After(time.Second):
		t.Fatal("worker did not recover after panic")
	}
	pool.Close()
}

func TestBatchLinksAllSubTasksToOneTree(t *testing.T) {
	tracer, rec := newTestTracer(t)
	root, ctx := startRoot(t, tracer)

	batch := NewBatch(tracer, nil)
	const subTasks = 5
	for i := 0; i < subTasks; i++ {
		batch.Add("sub-task", func(runCtx context.Context) error {
			if zipkin.SpanFromContext(runCtx) == nil {
				t.Error("sub-task missing span context")
			}
			return nil
		})
	}

	if err := batch.Run(ctx, "batch"); err != nil {
		t.Fatalf("unexpected batch error: %v", err)
	}
	root.Finish()

	spans := rec.Flush()
	if len(spans) != 1+1+subTasks {
		t.Fatalf("expected root, batch and sub-task spans, got %d", len(spans))
	}

	var batchSpan model.SpanModel
	for _, s := range spans {
		if s.Name == "batch" {
			batchSpan = s
		}
	}
	if batchSpan.TraceID != root.Context().TraceID {
		t.Fatal("batch span is not part of the request trace")
	}
	if batchSpan.ParentID == nil || *batchSpan.ParentID != root.Context().ID {
		t.Fatal("batch span not linked to request root")
	}
	if batchSpan.Tags["batch.size"] != "5" {
		t.Fatalf("unexpected batch.size tag: %q", batchSpan.Tags["batch.size"])
	}

	childCount := 0
	for _, s := range spans {
		if s.Name == "sub-task" {
			childCount++
			if s.TraceID != batchSpan.TraceID || s.ParentID == nil ||
				*s.ParentID != batchSpan.ID {
				t.Fatal("sub-task not linked to batch span")
			}
		}
	}
	if childCount != subTasks {
		t.Fatalf("expected %d sub-task spans, got %d", subTasks, childCount)
	}
}

func TestBatchRunsOnPoolAndJoinsErrors(t *testing.T) {
	tracer, _ := newTestTracer(t)
	_, ctx := startRoot(t, tracer)
	pool := NewPool(2, 8, WithPoolTracer(tracer))

	batch := NewBatch(tracer, pool)
	batch.Add("ok", func(context.Context) error { return nil })
	batch.Add("fail-a", func(context.Context) error { return errors.New("a") })
	batch.Add("fail-b", func(context.Context) error { return errors.New("b") })

	err := batch.Run(ctx, "batch-on-pool")
	pool.Close()

	if err == nil || err.Error() == "" {
		t.Fatal("expected joined sub-task errors")
	}
	if got := err.Error(); got != "a\nb" && got != "b\na" {
		t.Fatalf("unexpected joined error: %v", err)
	}
}

func TestBatchWithTinyQueueDoesNotDeadlock(t *testing.T) {
	tracer, rec := newTestTracer(t)
	root, ctx := startRoot(t, tracer)

	// One slow worker and a one-slot queue: far fewer slots than sub-tasks.
	pool := NewPool(1, 1, WithPoolTracer(tracer))
	batch := NewBatch(tracer, pool)

	const subTasks = 20
	var done int64
	for i := 0; i < subTasks; i++ {
		batch.Add("sub-task", func(context.Context) error {
			atomic.AddInt64(&done, 1)
			return nil
		})
	}

	finished := make(chan error, 1)
	go func() {
		finished <- batch.Run(ctx, "saturated-batch")
	}()

	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("unexpected batch error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("batch deadlocked on a saturated pool queue")
	}

	pool.Close()
	root.Finish()

	if atomic.LoadInt64(&done) != subTasks {
		t.Fatalf("expected %d sub-tasks, ran %d", subTasks, done)
	}

	children := 0
	for _, s := range rec.Flush() {
		if s.Name == "sub-task" && s.TraceID == root.Context().TraceID {
			children++
		}
	}
	if children != subTasks {
		t.Fatalf("expected %d linked sub-task spans, got %d", subTasks, children)
	}
}
