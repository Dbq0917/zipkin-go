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

// Package histogram provides an exponential-bucket duration histogram that can
// be fed by finished Zipkin spans.
//
// Sampling decisions are made when a span starts, while a span's duration is
// only known once the underlying call returns. Requests that blow past a
// configured timeout (for example a 30s timeout whose call runs for several
// minutes) therefore end up with very large durations. A linear set of
// buckets with a fixed upper edge collapses all of those requests into the
// last bucket, hiding the long tail.
//
// The histogram here uses exponential buckets whose upper edges grow without
// bound, so a long request lands in the range it actually belongs to. Every
// observation performs a single lock-free counter increment: recording a span
// does not allocate and does not wait for other spans to finish.
package histogram

import (
	"errors"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

// defaultInitialBuckets covers durations up to base * 2^23 (about 2.3 hours
// with the default 1ms base) without ever taking the growth lock.
const defaultInitialBuckets = 24

var (
	// ErrInvalidBase is returned when the bucket base is not positive.
	ErrInvalidBase = errors.New("histogram: base must be greater than zero")
	// ErrInvalidFactor is returned when the growth factor is below 1.0001.
	ErrInvalidFactor = errors.New("histogram: factor must be >= 1.0001")
)

// Histogram counts durations in exponentially growing buckets.
//
// Bucket 0 is the underflow bucket and holds every duration below base. For
// i >= 1, bucket i covers the half-open range [base*factor^(i-1),
// base*factor^i). There is no overflow bucket: the counter slice grows on
// demand, so arbitrarily large durations keep landing in their own ranges.
type Histogram struct {
	base   uint64
	factor float64
	logF   float64

	// counts holds uint64 counters. The slice header is swapped atomically on
	// growth; individual counters are updated with atomic operations.
	counts atomic.Pointer[[]uint64]

	growthMu sync.Mutex
}

// New returns a Histogram whose first regular bucket starts at base and whose
// bucket upper edges grow by factor on every step.
func New(base time.Duration, factor float64) (*Histogram, error) {
	if base <= 0 {
		return nil, ErrInvalidBase
	}
	if factor < 1.0001 {
		return nil, ErrInvalidFactor
	}

	counts := make([]uint64, defaultInitialBuckets)
	h := &Histogram{
		base:   uint64(base),
		factor: factor,
		logF:   math.Log(factor),
	}
	h.counts.Store(&counts)
	return h, nil
}

// NewPowerOfTwo returns a Histogram with a 1ms base and factor 2, i.e. bucket
// upper edges at 1ms, 2ms, 4ms, 8ms, ... This keeps the hot path to a single
// integer bit operation plus one atomic increment.
func NewPowerOfTwo() *Histogram {
	h, err := New(time.Millisecond, 2.0)
	if err != nil {
		panic(err)
	}
	return h
}

// Observe records one duration. Non-positive durations land in the underflow
// bucket. The call is lock-free on the hot path and never allocates.
func (h *Histogram) Observe(d time.Duration) {
	h.ObserveNanos(int64(d))
}

// ObserveNanos records one duration expressed in nanoseconds.
func (h *Histogram) ObserveNanos(nanos int64) {
	idx := h.bucketIndex(nanos)

	counts := h.counts.Load()
	if idx >= len(*counts) {
		counts = h.grow(idx)
	}
	atomic.AddUint64(&(*counts)[idx], 1)
}

// bucketIndex maps a duration in nanoseconds to a bucket. Bucket 0 is the
// underflow bucket; regular buckets start at index 1.
func (h *Histogram) bucketIndex(nanos int64) int {
	if nanos < int64(h.base) {
		return 0
	}

	if h.factor == 2.0 {
		// value/base is in [1, +inf); floor(log2(value/base)) + 1 selects the
		// bucket whose upper edge is the next power of two multiple of base.
		return bits.Len64(uint64(nanos) / h.base)
	}

	idx := 1 + int(math.Floor(math.Log(float64(nanos)/float64(h.base))/h.logF))
	// Floating point rounding can nudge an exact bucket boundary into the
	// previous bucket; correct by comparing against the boundaries directly.
	for idx > 1 && float64(h.base)*math.Pow(h.factor, float64(idx-1)) > float64(nanos) {
		idx--
	}
	for float64(h.base)*math.Pow(h.factor, float64(idx)) <= float64(nanos) {
		idx++
	}
	return idx
}

// grow doubles the counter slice until it can hold idx and returns the
// authoritative slice. Only contended when a duration exceeds every range seen
// so far; ordinary observations stay clear of this lock.
func (h *Histogram) grow(idx int) *[]uint64 {
	h.growthMu.Lock()
	defer h.growthMu.Unlock()

	counts := h.counts.Load()
	if idx < len(*counts) {
		return counts
	}

	size := len(*counts)
	for size <= idx {
		size *= 2
	}

	grown := make([]uint64, size)
	copy(grown, *counts)
	h.counts.Store(&grown)
	return &grown
}

// Counts returns a snapshot copy of the per-bucket counters, indexed by bucket.
func (h *Histogram) Counts() []uint64 {
	counts := h.counts.Load()
	snapshot := make([]uint64, len(*counts))
	copy(snapshot, *counts)
	return snapshot
}

// CountAt returns the counter for the bucket d would be placed in.
func (h *Histogram) CountAt(d time.Duration) uint64 {
	idx := h.bucketIndex(int64(d))
	counts := h.counts.Load()
	if idx >= len(*counts) {
		return 0
	}
	return atomic.LoadUint64(&(*counts)[idx])
}

// Total returns the number of observations recorded across all buckets.
func (h *Histogram) Total() uint64 {
	counts := h.counts.Load()
	var total uint64
	for i := range *counts {
		total += atomic.LoadUint64(&(*counts)[i])
	}
	return total
}

// BucketBounds returns the inclusive lower and exclusive upper bounds of the
// bucket at idx. The underflow bucket (idx 0) has a zero lower bound.
func (h *Histogram) BucketBounds(idx int) (lower, upper time.Duration, ok bool) {
	if idx < 0 {
		return 0, 0, false
	}
	if idx == 0 {
		return 0, time.Duration(h.base), true
	}

	lower = time.Duration(float64(h.base) * math.Pow(h.factor, float64(idx-1)))
	upper = time.Duration(float64(h.base) * math.Pow(h.factor, float64(idx)))
	return lower, upper, true
}
