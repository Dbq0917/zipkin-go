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
	"sync"
	"time"
)

// ErrInvalidSpanReaperTTL is returned by WithSpanReaper when the provided
// time-to-live is not strictly positive.
var ErrInvalidSpanReaperTTL = errors.New("span reaper TTL must be greater than zero")

// defaultSpanReaperInterval defines how often the reaper scans for expired
// spans. It is capped below the TTL (WithSpanReaper) so that an abandoned span
// is reclaimed at most one sweep after its TTL elapses.
const defaultSpanReaperInterval = 50 * time.Millisecond

// spanReaper keeps track of spans that have been started but not yet finished.
// Spans whose lifetime exceeds the configured TTL are considered abandoned by
// their caller (e.g. the call panicked or returned through an error path
// without calling Finish) and are finished automatically. This prevents the
// process from accumulating orphan spans in memory and guarantees every
// collected span is eventually reported.
type spanReaper struct {
	ttl      time.Duration
	interval time.Duration
	active   sync.Map // map[*spanImpl]struct{}
	stop     chan struct{}
	done     chan struct{}
}

func newSpanReaper(ttl time.Duration) *spanReaper {
	interval := ttl / 4
	if interval <= 0 || interval > defaultSpanReaperInterval {
		interval = defaultSpanReaperInterval
	}
	return &spanReaper{
		ttl:      ttl,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// register starts tracking a span. The span is removed again when its own
// Finish/FinishedWithDuration runs, so spans that finish before the TTL are
// never touched by the reaper.
func (r *spanReaper) register(s *spanImpl) {
	r.active.Store(s, struct{}{})
}

// unregister stops tracking a span that was finished by its caller.
func (r *spanReaper) unregister(s *spanImpl) {
	r.active.Delete(s)
}

func (r *spanReaper) loop() {
	ticker := time.NewTicker(r.interval)
	defer func() {
		ticker.Stop()
		close(r.done)
	}()
	for {
		select {
		case <-ticker.C:
			r.sweep(time.Now())
		case <-r.stop:
			return
		}
	}
}

// sweep finishes every tracked span that has outlived the TTL. The finish call
// uses the same compare-and-swap as a regular Finish, so a span finished
// concurrently by its caller is reported exactly once.
func (r *spanReaper) sweep(now time.Time) {
	r.active.Range(func(key, _ interface{}) bool {
		s := key.(*spanImpl)
		if now.Sub(s.Timestamp) < r.ttl {
			return true
		}
		s.Tag(string(TagError), "abandoned span reaped after "+r.ttl.String())
		s.Finish()
		return true
	})
}

// close stops the background sweep and waits for it to exit.
func (r *spanReaper) close() {
	close(r.stop)
	<-r.done
}
