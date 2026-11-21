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

/*
Package http implements a HTTP reporter to send spans to Zipkin V2 collectors.
*/
package http

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter"
)

// defaults
const (
	defaultTimeout       = 5 * time.Second // timeout for http request in seconds
	defaultBatchInterval = 1 * time.Second // BatchInterval in seconds
	defaultBatchSize     = 100
	defaultMaxBacklog    = 1000
)

// HTTPDoer will do a request to the Zipkin HTTP Collector
type HTTPDoer interface { // nolint: revive // keep as is, we don't want to break dependendants
	Do(req *http.Request) (*http.Response, error)
}

// httpReporter will send spans to a Zipkin HTTP Collector using Zipkin V2 API.
type httpReporter struct {
	url           string
	client        HTTPDoer
	logger        *log.Logger
	batchInterval time.Duration
	batchSize     int
	maxBacklog    int
	batchMtx      *sync.Mutex
	batch         []*model.SpanModel
	spanC         chan *model.SpanModel
	sendC         chan struct{}
	quit          chan struct{}
	shutdown      chan error
	reqCallback   RequestCallbackFn
	reqTimeout    time.Duration
	serializer    reporter.SpanSerializer
	// droppedTraceIDs holds the traces that lost at least one span to
	// backlog disposal. Once a trace is in this set every further span
	// belonging to it is refused as well, so the collector never receives
	// a partially reported trace (e.g. a parent whose children were dropped).
	// It only grows while the backlog is overflowing, which by itself means
	// the collector is under severe backpressure, so its size stays bounded
	// by the number of distinct traces involved in the overflow.
	droppedTraceIDs map[model.TraceID]struct{}
}

// Send implements reporter
func (r *httpReporter) Send(s model.SpanModel) {
	r.spanC <- &s
}

// Close implements reporter
func (r *httpReporter) Close() error {
	close(r.quit)
	return <-r.shutdown
}

func (r *httpReporter) loop() {
	var (
		nextSend   = time.Now().Add(r.batchInterval)
		ticker     = time.NewTicker(r.batchInterval / 10)
		tickerChan = ticker.C
	)
	defer ticker.Stop()

	for {
		select {
		case span := <-r.spanC:
			currentBatchSize := r.append(span)
			if currentBatchSize >= r.batchSize {
				nextSend = time.Now().Add(r.batchInterval)
				r.enqueueSend()
			}
		case <-tickerChan:
			if time.Now().After(nextSend) {
				nextSend = time.Now().Add(r.batchInterval)
				r.enqueueSend()
			}
		case <-r.quit:
			close(r.sendC)
			return
		}
	}
}

func (r *httpReporter) sendLoop() {
	for range r.sendC {
		_ = r.sendBatch()
	}
	r.shutdown <- r.sendBatch()
}

func (r *httpReporter) enqueueSend() {
	select {
	case r.sendC <- struct{}{}:
	default:
		// Do nothing if there's a pending send request already
	}
}

func (r *httpReporter) append(span *model.SpanModel) (newBatchSize int) {
	r.batchMtx.Lock()

	// A span from a trace we already started dropping can never be reported
	// as part of a whole trace, so refuse it immediately.
	if _, dropped := r.droppedTraceIDs[span.TraceID]; dropped {
		r.batchMtx.Unlock()
		return len(r.batch)
	}

	r.batch = append(r.batch, span)
	if len(r.batch) > r.maxBacklog {
		dispose := len(r.batch) - r.maxBacklog
		// Spans belonging to the same trace must never be disposed
		// individually, otherwise the collector receives partial traces
		// that look like downstream reporting failures. Mark the trace IDs
		// cut from the front as dropped, remove every span of those traces
		// currently in the backlog (including spans that arrived earlier),
		// and remember the IDs so later arrivals are refused too.
		disposedTraces := make(map[model.TraceID]struct{}, dispose)
		for _, disposedSpan := range r.batch[:dispose] {
			disposedTraces[disposedSpan.TraceID] = struct{}{}
		}
		kept := r.batch[:0]
		for _, backlogSpan := range r.batch {
			if _, drop := disposedTraces[backlogSpan.TraceID]; drop {
				continue
			}
			kept = append(kept, backlogSpan)
		}
		r.batch = kept
		for traceID := range disposedTraces {
			r.droppedTraceIDs[traceID] = struct{}{}
		}
		r.logger.Printf("backlog too long, disposing %d spans from %d traces", dispose, len(disposedTraces))
	}
	newBatchSize = len(r.batch)

	r.batchMtx.Unlock()
	return
}

func (r *httpReporter) sendBatch() error {
	// Select all current spans in the batch to be sent
	r.batchMtx.Lock()
	sendBatch := r.batch[:]
	r.batchMtx.Unlock()

	if len(sendBatch) == 0 {
		return nil
	}

	body, err := r.serializer.Serialize(sendBatch)
	if err != nil {
		r.logger.Printf("failed when marshalling the spans batch: %s\n", err.Error())
		return err
	}

	req, err := http.NewRequest("POST", r.url, bytes.NewReader(body))
	if err != nil {
		r.logger.Printf("failed when creating the request: %s\n", err.Error())
		return err
	}

	// By default we send b3:0 header to mitigate trace reporting amplification in
	// service mesh environments where the sidecar proxies might trace the call
	// we do here towards the Zipkin collector.
	req.Header.Set("b3", "0")

	req.Header.Set("Content-Type", r.serializer.ContentType())
	if r.reqCallback != nil {
		r.reqCallback(req)
	}

	ctx, cancel := context.WithTimeout(req.Context(), r.reqTimeout)
	defer cancel()

	resp, err := r.client.Do(req.WithContext(ctx))
	if err != nil {
		r.logger.Printf("failed to send the request: %s\n", err.Error())
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		r.logger.Printf("failed the request with status code %d\n", resp.StatusCode)
	}

	// Remove sent spans from the batch even if they were not saved
	r.batchMtx.Lock()
	r.batch = r.batch[len(sendBatch):]
	r.batchMtx.Unlock()

	return nil
}

// RequestCallbackFn receives the initialized request from the Collector before
// sending it over the wire. This allows one to plug in additional headers or
// do other customization.
type RequestCallbackFn func(*http.Request)

// ReporterOption sets a parameter for the HTTP Reporter
type ReporterOption func(r *httpReporter)

// Timeout sets maximum timeout for the http request through its context.
func Timeout(duration time.Duration) ReporterOption {
	return func(r *httpReporter) { r.reqTimeout = duration }
}

// BatchSize sets the maximum batch size, after which a collect will be
// triggered. The default batch size is 100 traces.
func BatchSize(n int) ReporterOption {
	return func(r *httpReporter) { r.batchSize = n }
}

// MaxBacklog sets the maximum backlog size. When batch size reaches this
// threshold, spans from the beginning of the batch will be disposed. Disposal
// is trace-atomic: every span sharing a trace ID with a disposed span is
// dropped as well, so the collector never receives a partially reported
// trace. The hot path is unchanged while the backlog fits, so the extra
// bookkeeping only happens once the backlog overflows.
func MaxBacklog(n int) ReporterOption {
	return func(r *httpReporter) { r.maxBacklog = n }
}

// BatchInterval sets the maximum duration we will buffer traces before
// emitting them to the collector. The default batch interval is 1 second.
func BatchInterval(d time.Duration) ReporterOption {
	return func(r *httpReporter) { r.batchInterval = d }
}

// Client sets a custom http client to use under the interface HTTPDoer
// which includes a `Do` method with same signature as the *http.Client
func Client(client HTTPDoer) ReporterOption {
	return func(r *httpReporter) { r.client = client }
}

// RequestCallback registers a callback function to adjust the reporter
// *http.Request before it sends the request to Zipkin.
func RequestCallback(rc RequestCallbackFn) ReporterOption {
	return func(r *httpReporter) { r.reqCallback = rc }
}

// Logger sets the logger used to report errors in the collection
// process.
func Logger(l *log.Logger) ReporterOption {
	return func(r *httpReporter) { r.logger = l }
}

// Serializer sets the serialization function to use for sending span data to
// Zipkin.
func Serializer(serializer reporter.SpanSerializer) ReporterOption {
	return func(r *httpReporter) {
		if serializer != nil {
			r.serializer = serializer
		}
	}
}

// NewReporter returns a new HTTP Reporter.
// url should be the endpoint to send the spans to, e.g.
// http://localhost:9411/api/v2/spans
func NewReporter(url string, opts ...ReporterOption) reporter.Reporter {
	r := httpReporter{
		url:             url,
		logger:          log.New(os.Stderr, "", log.LstdFlags),
		client:          &http.Client{},
		batchInterval:   defaultBatchInterval,
		batchSize:       defaultBatchSize,
		maxBacklog:      defaultMaxBacklog,
		batch:           []*model.SpanModel{},
		spanC:           make(chan *model.SpanModel),
		sendC:           make(chan struct{}, 1),
		quit:            make(chan struct{}, 1),
		shutdown:        make(chan error, 1),
		batchMtx:        &sync.Mutex{},
		droppedTraceIDs: map[model.TraceID]struct{}{},
		serializer:      reporter.JSONSerializer{},
		reqTimeout:      defaultTimeout,
	}

	for _, opt := range opts {
		opt(&r)
	}

	go r.loop()
	go r.sendLoop()

	return &r
}
