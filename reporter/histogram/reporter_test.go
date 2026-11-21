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

package histogram_test

import (
	"testing"
	"time"

	zipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/histogram"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
)

// TestSampledSpanDurationsLandInCorrectBuckets drives the tracer the same way
// the repository's sampling test cases do (BoundarySampler, sampling decision
// taken at span creation) and checks which bucket each finished span lands in.
// Durations are supplied through FinishedWithDuration so a multi-minute request
// does not require a real multi-minute test run.
func TestSampledSpanDurationsLandInCorrectBuckets(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	rep, hist := histogram.NewReporter(rec)

	sampler, err := zipkin.NewBoundarySampler(1.0, 0)
	if err != nil {
		t.Fatalf("unexpected sampler error: %v", err)
	}
	tracer, err := zipkin.NewTracer(rep, zipkin.WithSampler(sampler))
	if err != nil {
		t.Fatalf("unexpected tracer error: %v", err)
	}

	cases := []struct {
		name     string
		duration time.Duration
		bucket   int
	}{
		{"sub-millisecond", 500 * time.Microsecond, 0},
		{"fast request", 2 * time.Millisecond, 2},
		{"typical request", 250 * time.Millisecond, 8},
		{"one second", time.Second, 10},
		{"thirty second timeout", 30 * time.Second, 15},
		{"one minute past timeout", time.Minute, 16},
		{"three minute runaway", 3 * time.Minute, 18},
		{"five minute runaway", 5 * time.Minute, 19},
	}

	for _, tc := range cases {
		span := tracer.StartSpan(tc.name)
		span.FinishedWithDuration(tc.duration)
	}

	if got, want := len(rec.Flush()), len(cases); got != want {
		t.Fatalf("forwarded spans: want %d, got %d", want, got)
	}
	if got, want := hist.Total(), uint64(len(cases)); got != want {
		t.Fatalf("recorded observations: want %d, got %d", want, got)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := 0
			counts := hist.Counts()
			for i, count := range counts {
				if i == tc.bucket {
					idx = int(count)
				}
			}
			if idx != 1 {
				t.Fatalf("duration %s: want 1 observation in bucket %d, got %d",
					tc.duration, tc.bucket, idx)
			}

			lower, upper, ok := hist.BucketBounds(tc.bucket)
			if !ok {
				t.Fatalf("missing bounds for bucket %d", tc.bucket)
			}
			if tc.duration < lower || tc.duration >= upper {
				t.Fatalf("duration %s outside bucket %d range [%s, %s)",
					tc.duration, tc.bucket, lower, upper)
			}
		})
	}

	// The timeout cap is 30s but runaways take minutes. With linear buckets
	// they would all share the final bucket; here each long request keeps its
	// own range and the tail stays visible.
	occupied := 0
	for _, count := range hist.Counts() {
		if count > 0 {
			occupied++
		}
	}
	if occupied != len(cases) {
		t.Fatalf("want %d occupied buckets, got %d (long tail collapsed)",
			len(cases), occupied)
	}
}

func TestUnsampledSpansAreNotRecorded(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	rep, hist := histogram.NewReporter(rec)

	sampler, err := zipkin.NewBoundarySampler(0.0, 0)
	if err != nil {
		t.Fatalf("unexpected sampler error: %v", err)
	}
	tracer, err := zipkin.NewTracer(rep, zipkin.WithSampler(sampler))
	if err != nil {
		t.Fatalf("unexpected tracer error: %v", err)
	}

	span := tracer.StartSpan("unsampled")
	span.FinishedWithDuration(3 * time.Minute)

	if hist.Total() != 0 {
		t.Fatalf("unsampled span must not reach the reporter, got %d observations",
			hist.Total())
	}
	if len(rec.Flush()) != 0 {
		t.Fatal("unsampled span must not be forwarded")
	}
}

func TestReporterClosePropagates(t *testing.T) {
	rec := recorder.NewReporter()
	rep, _ := histogram.NewReporter(rec)

	if err := rep.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(rec.Flush()) != 0 {
		t.Fatal("Close should flush the wrapped recorder")
	}
}
