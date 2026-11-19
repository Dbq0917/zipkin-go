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

	// Skeleton spans are cheap, so allow a larger bounded backup queue for
	// them. If even that fills, whole traces are evicted.
	skeletonBacklogFactor = 4
	traceStateTTL         = 5 * time.Minute
	traceStatePrunePeriod = time.Minute
	minTraceStateEntries  = 1024
	traceStateEntryFactor = 8

	// droppedTag marks a span whose tags and annotations were shed because the
	// reporter backlog was full. Its trace and parent/child IDs are preserved.
	droppedTag = "zipkin.dropped"
)

// HTTPDoer will do a request to the Zipkin HTTP Collector
type HTTPDoer interface { // nolint: revive // keep as is, we don't want to break dependendants
	Do(req *http.Request) (*http.Response, error)
}

// httpReporter will send spans to a Zipkin HTTP Collector using Zipkin V2 API.
type httpReporter struct {
	url                string
	client             HTTPDoer
	logger             *log.Logger
	batchInterval      time.Duration
	batchSize          int
	maxBacklog         int
	maxSkeletonBacklog int
	batchMtx           *sync.Mutex
	batch              []*bufferedTrace
	traceByID          map[model.TraceID]*bufferedTrace
	detailedSpans      int
	skeletonSpans      int
	droppedTraces      map[model.TraceID]time.Time
	sentTraces         map[model.TraceID]time.Time
	lastTracePrune     time.Time
	spanC              chan *model.SpanModel
	sendC              chan struct{}
	quit               chan struct{}
	shutdown           chan error
	reqCallback        RequestCallbackFn
	reqTimeout         time.Duration
	serializer         reporter.SpanSerializer
}

type bufferedTrace struct {
	traceID  model.TraceID
	spans    []bufferedSpan
	detailed int
	skeleton int
}

type bufferedSpan struct {
	span     *model.SpanModel
	skeleton bool
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
	defer r.batchMtx.Unlock()

	now := time.Now()
	r.pruneTraceStateLocked(now)

	if _, dropped := r.droppedTraces[span.TraceID]; dropped {
		return r.batchSizeLocked()
	}

	trace := r.traceByID[span.TraceID]
	if trace == nil {
		trace = &bufferedTrace{traceID: span.TraceID}
		r.traceByID[span.TraceID] = trace
		r.batch = append(r.batch, trace)
	}

	trace.spans = append(trace.spans, bufferedSpan{span: span})
	trace.detailed++
	r.detailedSpans++

	r.shedLocked(now)
	return r.batchSizeLocked()
}

func (r *httpReporter) batchSizeLocked() int {
	return r.detailedSpans + r.skeletonSpans
}

func (r *httpReporter) shedLocked(now time.Time) {
	for r.detailedSpans > r.maxBacklog {
		trace := r.firstTraceWithDetailedLocked()
		if trace == nil {
			break
		}
		r.degradeTraceLocked(trace)
	}

	for r.skeletonSpans > r.maxSkeletonBacklog {
		trace := r.firstDroppableTraceLocked()
		if trace == nil {
			// All buffered traces already have spans in flight. Dropping one
			// would create a partial trace, but keeping everything would make
			// the backlog unbounded. Prefer the oldest trace in this terminal
			// overload case.
			trace = r.firstTraceWithSkeletonLocked()
		}
		if trace == nil {
			break
		}
		r.dropTraceLocked(trace, now)
	}
}

func (r *httpReporter) firstTraceWithDetailedLocked() *bufferedTrace {
	for _, trace := range r.batch {
		if trace.detailed > 0 {
			return trace
		}
	}
	return nil
}

func (r *httpReporter) firstDroppableTraceLocked() *bufferedTrace {
	for _, trace := range r.batch {
		if trace.skeleton == 0 {
			continue
		}
		if _, sent := r.sentTraces[trace.traceID]; !sent {
			return trace
		}
	}
	return nil
}

func (r *httpReporter) firstTraceWithSkeletonLocked() *bufferedTrace {
	for _, trace := range r.batch {
		if trace.skeleton > 0 {
			return trace
		}
	}
	return nil
}

func (r *httpReporter) degradeTraceLocked(trace *bufferedTrace) {
	if trace.detailed == 0 {
		return
	}

	degraded := 0
	for i := range trace.spans {
		if trace.spans[i].skeleton {
			continue
		}
		span := trace.spans[i].span
		span.Annotations = nil
		span.Tags = map[string]string{droppedTag: "true"}
		trace.spans[i].skeleton = true
		trace.detailed--
		trace.skeleton++
		r.detailedSpans--
		r.skeletonSpans++
		degraded++
	}

	r.logger.Printf("backlog too long, reduced %d spans in trace %s to structural skeletons", degraded, trace.traceID)
}

func (r *httpReporter) dropTraceLocked(trace *bufferedTrace, now time.Time) {
	dropped := trace.detailed + trace.skeleton
	r.removeTraceSliceLocked(trace)
	delete(r.traceByID, trace.traceID)
	r.detailedSpans -= trace.detailed
	r.skeletonSpans -= trace.skeleton
	r.droppedTraces[trace.traceID] = now
	r.trimTraceStateLocked()

	r.logger.Printf("backlog too long, dropping trace %s with %d spans", trace.traceID, dropped)
}

func (r *httpReporter) removeTraceSliceLocked(trace *bufferedTrace) {
	for i, candidate := range r.batch {
		if candidate != trace {
			continue
		}
		copy(r.batch[i:], r.batch[i+1:])
		r.batch[len(r.batch)-1] = nil
		r.batch = r.batch[:len(r.batch)-1]
		return
	}
}

func (r *httpReporter) pruneTraceStateLocked(now time.Time) {
	if !r.lastTracePrune.IsZero() && now.Sub(r.lastTracePrune) < traceStatePrunePeriod {
		return
	}
	r.lastTracePrune = now

	for traceID, droppedAt := range r.droppedTraces {
		if now.Sub(droppedAt) > traceStateTTL {
			delete(r.droppedTraces, traceID)
		}
	}
	for traceID, sentAt := range r.sentTraces {
		if now.Sub(sentAt) > traceStateTTL {
			delete(r.sentTraces, traceID)
		}
	}
	r.trimTraceStateLocked()
}

func (r *httpReporter) trimTraceStateLocked() {
	maxEntries := r.maxBacklog * traceStateEntryFactor
	if maxEntries < minTraceStateEntries {
		maxEntries = minTraceStateEntries
	}
	for len(r.droppedTraces) > maxEntries {
		for traceID := range r.droppedTraces {
			delete(r.droppedTraces, traceID)
			break
		}
	}
	for len(r.sentTraces) > maxEntries {
		for traceID := range r.sentTraces {
			delete(r.sentTraces, traceID)
			break
		}
	}
}

func (r *httpReporter) sendBatch() error {
	// Detach the batch before touching the network so the business goroutine
	// never waits for the collector.
	sendBatch, traces := r.takeBatch()

	if len(sendBatch) == 0 {
		return nil
	}

	body, err := r.serializer.Serialize(sendBatch)
	if err != nil {
		r.logger.Printf("failed when marshalling the spans batch: %s\n", err.Error())
		r.requeueBatch(traces)
		return err
	}

	req, err := http.NewRequest("POST", r.url, bytes.NewReader(body))
	if err != nil {
		r.logger.Printf("failed when creating the request: %s\n", err.Error())
		r.requeueBatch(traces)
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
		r.requeueBatch(traces)
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		r.logger.Printf("failed the request with status code %d\n", resp.StatusCode)
	}

	return nil
}

func (r *httpReporter) takeBatch() ([]*model.SpanModel, []*bufferedTrace) {
	r.batchMtx.Lock()
	defer r.batchMtx.Unlock()

	if len(r.batch) == 0 {
		return nil, nil
	}

	now := time.Now()
	r.pruneTraceStateLocked(now)

	batchSize := r.batchSizeLocked()
	sendBatch := make([]*model.SpanModel, 0, batchSize)
	traces := r.batch
	for _, trace := range traces {
		r.sentTraces[trace.traceID] = now
		for i := range trace.spans {
			sendBatch = append(sendBatch, trace.spans[i].span)
		}
	}
	r.trimTraceStateLocked()

	r.batch = nil
	r.traceByID = make(map[model.TraceID]*bufferedTrace)
	r.detailedSpans = 0
	r.skeletonSpans = 0

	return sendBatch, traces
}

func (r *httpReporter) requeueBatch(traces []*bufferedTrace) {
	if len(traces) == 0 {
		return
	}

	r.batchMtx.Lock()
	defer r.batchMtx.Unlock()

	now := time.Now()
	r.pruneTraceStateLocked(now)

	for _, trace := range traces {
		existing := r.traceByID[trace.traceID]
		if existing == nil {
			continue
		}
		trace.spans = append(trace.spans, existing.spans...)
		trace.detailed += existing.detailed
		trace.skeleton += existing.skeleton
		r.detailedSpans -= existing.detailed
		r.skeletonSpans -= existing.skeleton
		r.removeTraceSliceLocked(existing)
		delete(r.traceByID, trace.traceID)
	}

	r.batch = append(traces, r.batch...)
	for _, trace := range traces {
		r.traceByID[trace.traceID] = trace
		r.detailedSpans += trace.detailed
		r.skeletonSpans += trace.skeleton
	}

	r.shedLocked(now)
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

// MaxBacklog sets the maximum number of full-fidelity spans held before the
// reporter starts shedding span detail. Shedding is done per trace: tags and
// annotations are removed, but trace, span, parent IDs and endpoints are kept
// so the trace structure remains complete. Skeleton spans carry the
// zipkin.dropped=true tag. If the compact backlog also fills, whole traces are
// dropped, never individual spans.
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
		url:           url,
		logger:        log.New(os.Stderr, "", log.LstdFlags),
		client:        &http.Client{},
		batchInterval: defaultBatchInterval,
		batchSize:     defaultBatchSize,
		maxBacklog:    defaultMaxBacklog,
		batch:         []*bufferedTrace{},
		traceByID:     make(map[model.TraceID]*bufferedTrace),
		droppedTraces: make(map[model.TraceID]time.Time),
		sentTraces:    make(map[model.TraceID]time.Time),
		spanC:         make(chan *model.SpanModel),
		sendC:         make(chan struct{}, 1),
		quit:          make(chan struct{}, 1),
		shutdown:      make(chan error, 1),
		batchMtx:      &sync.Mutex{},
		serializer:    reporter.JSONSerializer{},
		reqTimeout:    defaultTimeout,
	}

	for _, opt := range opts {
		opt(&r)
	}
	r.maxSkeletonBacklog = r.maxBacklog * skeletonBacklogFactor
	if r.maxSkeletonBacklog < 0 {
		r.maxSkeletonBacklog = 0
	}

	go r.loop()
	go r.sendLoop()

	return &r
}
