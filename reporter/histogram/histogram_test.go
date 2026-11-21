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

package histogram

import (
	"sync"
	"testing"
	"time"
)

func TestNewValidation(t *testing.T) {
	if _, err := New(0, 2.0); err != ErrInvalidBase {
		t.Fatalf("want ErrInvalidBase, got %v", err)
	}
	if _, err := New(time.Millisecond, 1.0); err != ErrInvalidFactor {
		t.Fatalf("want ErrInvalidFactor, got %v", err)
	}
	if _, err := New(time.Millisecond, 2.0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPowerOfTwoBucketIndex(t *testing.T) {
	h := NewPowerOfTwo()

	for _, tc := range []struct {
		name string
		d    time.Duration
		want int
	}{
		{"zero", 0, 0},
		{"underflow", 500 * time.Microsecond, 0},
		{"just below base", time.Millisecond - 1, 0},
		{"base boundary", time.Millisecond, 1},
		{"1.5ms", time.Millisecond + 500*time.Microsecond, 1},
		{"2ms boundary", 2 * time.Millisecond, 2},
		{"3ms", 3 * time.Millisecond, 2},
		{"4ms boundary", 4 * time.Millisecond, 3},
		{"1s", time.Second, 10},
		{"30s timeout", 30 * time.Second, 15},
		{"31s past timeout", 31 * time.Second, 15},
		{"1m", time.Minute, 16},
		{"3m long request", 3 * time.Minute, 18},
		{"5m long request", 5 * time.Minute, 19},
		{"10m", 10 * time.Minute, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := h.bucketIndex(int64(tc.d)); got != tc.want {
				t.Fatalf("duration %s: want bucket %d, got %d", tc.d, tc.want, got)
			}
		})
	}
}

func TestObserveLandsInMatchingRange(t *testing.T) {
	h := NewPowerOfTwo()

	durations := []time.Duration{
		100 * time.Microsecond,
		time.Millisecond,
		5 * time.Millisecond,
		50 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		30 * time.Second,
		3 * time.Minute,
		5 * time.Minute,
	}

	for _, d := range durations {
		h.Observe(d)

		idx := h.bucketIndex(int64(d))
		lower, upper, ok := h.BucketBounds(idx)
		if !ok {
			t.Fatalf("no bounds for bucket %d", idx)
		}
		if d < lower || d >= upper {
			t.Fatalf("duration %s not in its bucket range [%s, %s)", d, lower, upper)
		}
		if h.CountAt(d) != 1 {
			t.Fatalf("duration %s: want count 1 in bucket %d, got %d", d, idx, h.CountAt(d))
		}
	}

	if got, want := h.Total(), uint64(len(durations)); got != want {
		t.Fatalf("total observations: want %d, got %d", want, got)
	}
}

func TestLongRequestsDoNotCollapseIntoOneBucket(t *testing.T) {
	h := NewPowerOfTwo()

	for _, d := range []time.Duration{30 * time.Second, time.Minute, 3 * time.Minute, 5 * time.Minute} {
		h.Observe(d)
	}

	occupied := 0
	for _, c := range h.Counts() {
		if c > 0 {
			occupied++
		}
	}
	if occupied != 4 {
		t.Fatalf("want 4 distinct buckets for 30s/1m/3m/5m, got %d", occupied)
	}
}

func TestGenericFactorBucketIndex(t *testing.T) {
	h, err := New(time.Millisecond, 10.0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, tc := range []struct {
		d    time.Duration
		want int
	}{
		{500 * time.Microsecond, 0},
		{time.Millisecond, 1},
		{9 * time.Millisecond, 1},
		{10 * time.Millisecond, 2},
		{100 * time.Millisecond, 3},
		{time.Second, 4},
	} {
		if got := h.bucketIndex(int64(tc.d)); got != tc.want {
			t.Fatalf("duration %s: want bucket %d, got %d", tc.d, tc.want, got)
		}
	}
}

func TestGrowthBeyondInitialBuckets(t *testing.T) {
	h := NewPowerOfTwo()

	// 1 hour is bucket 19 with a 1ms base; push well past the preallocated
	// slice to exercise on-demand growth.
	giant := time.Duration(1) << 40 // ~305 hours in nanoseconds
	h.Observe(giant)

	if h.CountAt(giant) != 1 {
		t.Fatal("giant duration was not recorded after growth")
	}
	if h.Total() != 1 {
		t.Fatalf("total: want 1, got %d", h.Total())
	}

	lower, upper, ok := h.BucketBounds(h.bucketIndex(int64(giant)))
	if !ok || giant < lower || giant >= upper {
		t.Fatalf("giant duration %s not in grown range [%s, %s)", giant, lower, upper)
	}
}

func TestConcurrentObservations(t *testing.T) {
	h := NewPowerOfTwo()

	const goroutines = 16
	const perGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(base time.Duration) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				h.Observe(base + time.Duration(j)*time.Millisecond)
			}
		}(time.Duration(i) * time.Minute)
	}
	wg.Wait()

	if got, want := h.Total(), uint64(goroutines*perGoroutine); got != want {
		t.Fatalf("total observations: want %d, got %d", want, got)
	}
}

func TestCountsIsSnapshot(t *testing.T) {
	h := NewPowerOfTwo()
	h.Observe(time.Second)

	snapshot := h.Counts()
	snapshot[0] = 999

	if h.CountAt(500*time.Microsecond) != 0 {
		t.Fatal("mutating the snapshot must not affect the histogram")
	}
}
