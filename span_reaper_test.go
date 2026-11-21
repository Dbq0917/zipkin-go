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

package zipkin

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
)

func TestSpanReaperFinishesAbandonedSpans(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	tr, err := NewTracer(rec, WithSpanReaper(100*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error creating tracer: %v", err)
	}
	defer tr.Close()

	const abandoned = 25
	for i := 0; i < abandoned; i++ {
		_ = tr.StartSpan("abandoned")
	}

	var reported int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reported += len(rec.Flush())
		if reported == abandoned {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("reaper did not finish all abandoned spans, have %d, want %d", reported, abandoned)
}

func TestSpanReaperDoesNotReapSpansFinishedBeforeTTL(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	tr, err := NewTracer(rec, WithSpanReaper(250*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error creating tracer: %v", err)
	}
	defer tr.Close()

	const total = 20
	for i := 0; i < total; i++ {
		sp := tr.StartSpan("normal")
		sp.Finish()
		// finishing twice must not report the span again
		sp.Finish()
	}

	if want, have := total, len(rec.Flush()); want != have {
		t.Fatalf("reported spans want %d, have %d", want, have)
	}

	// wait well past the TTL and ensure no extra span was reaped/reported
	time.Sleep(400 * time.Millisecond)
	if leftover := len(rec.Flush()); leftover != 0 {
		t.Fatalf("reaper reclaimed %d already finished spans", leftover)
	}

	if tracked := countTrackedSpans(tr); tracked != 0 {
		t.Fatalf("finished spans still tracked by reaper: %d", tracked)
	}
}

func TestSpanReaperTagsReapedSpansAsErrors(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	tr, err := NewTracer(rec, WithSpanReaper(50*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error creating tracer: %v", err)
	}
	defer tr.Close()

	_ = tr.StartSpan("abandoned")

	var spans []struct {
		errTag string
		hasTag bool
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, sm := range rec.Flush() {
			v, ok := sm.Tags[string(TagError)]
			spans = append(spans, struct {
				errTag string
				hasTag bool
			}{v, ok})
		}
		if len(spans) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if len(spans) != 1 {
		t.Fatalf("want 1 reported span, have %d", len(spans))
	}
	if !spans[0].hasTag || spans[0].errTag == "" {
		t.Fatalf("reaped span missing non-empty error tag, have %+v", spans[0])
	}
}

func TestSpanReaperDisabledByDefault(t *testing.T) {
	tr, err := NewTracer(recorder.NewReporter())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer tr.Close()

	if tr.reaper != nil {
		t.Fatal("reaper should be disabled by default")
	}

	sp := tr.StartSpan("leaked")
	impl := sp.(*spanImpl)
	time.Sleep(50 * time.Millisecond)
	if atomicLoadMustCollect(impl) == 0 {
		t.Fatal("span was finished without a reaper enabled")
	}
	sp.Finish()

	// Close on a tracer without a reaper must be a safe no-op
	tr.Close()
}

func TestWithSpanReaperRejectsInvalidTTL(t *testing.T) {
	if _, err := NewTracer(recorder.NewReporter(), WithSpanReaper(0)); !errors.Is(err, ErrInvalidSpanReaperTTL) {
		t.Fatalf("want ErrInvalidSpanReaperTTL, have %v", err)
	}
	if _, err := NewTracer(recorder.NewReporter(), WithSpanReaper(-time.Second)); !errors.Is(err, ErrInvalidSpanReaperTTL) {
		t.Fatalf("want ErrInvalidSpanReaperTTL, have %v", err)
	}
}

func TestTracerCloseIsIdempotent(t *testing.T) {
	tr, err := NewTracer(recorder.NewReporter(), WithSpanReaper(100*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tr.Close()
	tr.Close() // must not panic on a closed channel

	// a tracer without a reaper must also be closable repeatedly
	plain, _ := NewTracer(recorder.NewReporter())
	plain.Close()
	plain.Close()
}

func TestSpanReaperStopsBackgroundGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	tr, err := NewTracer(recorder.NewReporter(), WithSpanReaper(50*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if during := runtime.NumGoroutine(); during <= before {
		t.Fatalf("reaper goroutine not started: before=%d during=%d", before, during)
	}

	tr.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("reaper goroutine still running after Close: before=%d after=%d",
		before, runtime.NumGoroutine())
}

func TestReaperConcurrentFinishReportsOnce(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	tr, err := NewTracer(rec, WithSpanReaper(30*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer tr.Close()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(delay time.Duration) {
			defer wg.Done()
			sp := tr.StartSpan("racy")
			// race an explicit finish against the reaper sweep
			time.Sleep(delay)
			sp.Finish()
			sp.Finish()
		}(time.Duration(i) * time.Millisecond)
	}
	wg.Wait()

	var reported int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reported += len(rec.Flush())
		if reported == goroutines {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("want exactly %d reported spans, have %d", goroutines, reported)
}

func TestFinishOnRecoverNormalReturnDoesNotFinishSpan(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	tr, _ := NewTracer(rec)

	func() {
		sp := tr.StartSpan("normal")
		defer FinishOnRecover(sp)
		// normal path finishes explicitly, just like existing callers
		sp.Finish()
	}()

	if want, have := 1, len(rec.Flush()); want != have {
		t.Fatalf("reported spans want %d, have %d", want, have)
	}
}

func TestFinishOnRecoverPanicPathFinishesAndReraises(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	tr, _ := NewTracer(rec)

	boom := errors.New("boom")
	func() {
		defer func() {
			if r := recover(); r != boom {
				t.Fatalf("panic not re-raised, have %v", r)
			}
		}()

		sp := tr.StartSpan("panicking")
		defer FinishOnRecover(sp)
		panic(boom)
	}()

	spans := rec.Flush()
	if want, have := 1, len(spans); want != have {
		t.Fatalf("reported spans want %d, have %d", want, have)
	}
	if have, want := spans[0].Tags[string(TagError)], boom.Error(); have != want {
		t.Fatalf("error tag want %q, have %q", want, have)
	}

	// nil span must be a safe no-op instead of panicking
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("nil span panicked: %v", r)
			}
		}()
		FinishOnRecover(nil)
	}()
}

// TestSpanReaperReclaimsMemoryOnErrorPaths drives a large batch of spans
// through an error path without calling Finish, mirroring a caller that only
// finishes on the success path. Active spans are retained by the tracer's
// span registry while they are in flight; the reaper must release that
// retention once the TTL elapses so the batch becomes garbage collectable.
// The baseline holds the same batch in a registry that never reaps, which is
// the leak behavior the reaper is meant to prevent.
func TestSpanReaperReclaimsMemoryOnErrorPaths(t *testing.T) {
	const batch = 50000

	// baseline: active-span registry that never reaps (the reported leak)
	leakedHeap, leakedRegistry, leakedRec := retainedBatchHeap(t, batch, false)
	runtime.KeepAlive(leakedRegistry)
	runtime.KeepAlive(leakedRec)

	// fixed: reaper with a long TTL so the full batch accumulates first, then
	// is reclaimed in the background
	reapedHeap, reapedRegistry, rec := retainedBatchHeap(t, batch, true)
	runtime.KeepAlive(reapedRegistry)

	t.Logf("heap after %d error-path spans: never reaped=%d KiB, reaped=%d KiB; reported=%d",
		batch, leakedHeap/1024, reapedHeap/1024, atomic.LoadInt64(&rec.count))

	if want, have := int64(batch), atomic.LoadInt64(&rec.count); want != have {
		t.Fatalf("reaped reports want %d, have %d", want, have)
	}

	// the reaper must remove the dominant retention source; allow headroom for
	// allocator fragmentation and unrelated live objects.
	if reapedHeap > leakedHeap/2 {
		t.Fatalf("reaper did not reclaim abandoned span memory: %d KiB vs %d KiB",
			reapedHeap/1024, leakedHeap/1024)
	}
}

type discardReporter struct {
	count int64
}

func (d *discardReporter) Send(_ model.SpanModel) {
	atomic.AddInt64(&d.count, 1)
}

func (d *discardReporter) Close() error { return nil }

func retainedBatchHeap(t *testing.T, batch int, withReaper bool) (uint64, []*spanImpl, *discardReporter) {
	t.Helper()

	rec := &discardReporter{}
	var tr *Tracer
	if withReaper {
		// long TTL: the whole batch accumulates in the active registry before
		// any sweep reclaims it
		var err error
		tr, err = NewTracer(rec, WithSpanReaper(300*time.Millisecond))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	} else {
		var err error
		tr, err = NewTracer(rec)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// the registry mimics in-flight request bookkeeping that keeps spans alive
	// even when the caller bails out through an error path
	registry := make([]*spanImpl, 0, batch)
	for i := 0; i < batch; i++ {
		sp := tr.StartSpan("error-path")
		sp.Tag("operation", "failed")
		registry = append(registry, sp.(*spanImpl))
	}

	if withReaper {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if countTrackedSpans(tr) == 0 {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		if leftover := countTrackedSpans(tr); leftover != 0 {
			t.Fatalf("reaper left %d spans tracked", leftover)
		}
		// the tracer no longer retains the batch; drop the local registry too
		registry = nil
		tr.Close()
	}

	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc, registry, rec
}

func countTrackedSpans(tr *Tracer) int {
	if tr.reaper == nil {
		return 0
	}
	n := 0
	tr.reaper.active.Range(func(_, _ interface{}) bool {
		n++
		return true
	})
	return n
}

func atomicLoadMustCollect(s *spanImpl) int32 {
	return atomic.LoadInt32(&s.mustCollect)
}
