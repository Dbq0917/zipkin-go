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

package http

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/recorder"
)

type panickingRoundTripper struct{}

func (panickingRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	panic(errors.New("round trip blew up"))
}

// TestTransportPanicPathReportsSpan verifies a span started by the transport
// is still reported when the underlying RoundTripper panics, as long as the
// caller protects the span with zipkin.FinishOnRecover. The panic must be
// re-raised so existing failure behavior is preserved.
func TestTransportPanicPathReportsSpan(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	tracer, err := zipkin.NewTracer(rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tr, err := NewTransport(tracer, RoundTripper(panickingRoundTripper{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	req, _ := http.NewRequest("GET", "http://example.com", nil)

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the round trip panic to be re-raised")
			}
		}()

		sp, _ := tracer.StartSpanFromContext(req.Context(), "protected")
		defer zipkin.FinishOnRecover(sp)

		req = req.WithContext(zipkin.NewContext(req.Context(), sp))
		if _, rtErr := tr.RoundTrip(req); rtErr != nil {
			t.Fatalf("unexpected error return: %v", rtErr)
		}
	}()

	spans := rec.Flush()
	if want, have := 1, len(spans); want != have {
		t.Fatalf("reported spans want %d, have %d", want, have)
	}
	if have := spans[0].Tags["error"]; have == "" {
		t.Fatal("panic path span missing error tag")
	}
}

// TestReaperReclaimsSpanLeakedByPanic simulates callers that forget any
// deferred finish on the error path: the span is never finished explicitly,
// but the tracer's reaper still closes and reports it after the TTL.
func TestReaperReclaimsSpanLeakedByPanic(t *testing.T) {
	rec := recorder.NewReporter()
	defer rec.Close()

	tracer, err := zipkin.NewTracer(rec, zipkin.WithSpanReaper(100*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer tracer.Close()

	const leaked = 10
	var reported int
	for i := 0; i < leaked; i++ {
		func() {
			defer func() { _ = recover() }()
			_ = tracer.StartSpan("leaked-by-panic")
			panic(errors.New("abort"))
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reported += len(rec.Flush())
		if reported == leaked {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("reaper did not report all leaked spans, have %d, want %d", reported, leaked)
}
