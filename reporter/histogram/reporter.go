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
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter"
)

// durationReporter wraps a reporter and feeds every finished span's duration
// into a Histogram before forwarding the span unchanged.
type durationReporter struct {
	next reporter.Reporter
	hist *Histogram
}

// NewReporter wraps next with a power-of-two duration histogram (1ms base,
// factor 2). The returned reporter and the histogram share state: inspect the
// histogram at any time to read the current duration distribution without
// waiting for in-flight spans to finish.
func NewReporter(next reporter.Reporter) (reporter.Reporter, *Histogram) {
	return NewReporterWithHistogram(next, NewPowerOfTwo())
}

// NewReporterWithHistogram wraps next with a caller-provided Histogram.
func NewReporterWithHistogram(next reporter.Reporter, hist *Histogram) (reporter.Reporter, *Histogram) {
	if next == nil {
		next = reporter.NewNoopReporter()
	}
	return &durationReporter{next: next, hist: hist}, hist
}

// Send records the span duration and forwards the span to the wrapped
// reporter. Recording is one atomic counter increment, so the per-span write
// overhead stays constant regardless of how long the span ran.
func (r *durationReporter) Send(span model.SpanModel) {
	r.hist.Observe(span.Duration)
	r.next.Send(span)
}

// Close closes the wrapped reporter.
func (r *durationReporter) Close() error {
	return r.next.Close()
}
