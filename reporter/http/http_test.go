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

package http_test

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openzipkin/zipkin-go/idgenerator"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter"
	zipkinhttp "github.com/openzipkin/zipkin-go/reporter/http"
)

func generateSpans(n int) []*model.SpanModel {
	spans := make([]*model.SpanModel, n)
	idGen := idgenerator.NewRandom64()
	traceID := idGen.TraceID()

	for i := 0; i < n; i++ {
		spans[i] = &model.SpanModel{
			SpanContext: model.SpanContext{
				TraceID: traceID,
				ID:      idGen.SpanID(traceID),
			},
			Name:      "name",
			Kind:      model.Client,
			Timestamp: time.Now(),
		}
	}

	return spans
}

func newTestServer(t *testing.T, spans []*model.SpanModel, serializer reporter.SpanSerializer, onReceive func(int)) *httptest.Server {
	sofar := 0
	return httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected 'POST' request, got '%s'", r.Method)
		}

		aPayload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		var aSpans []*model.SpanModel
		err = json.Unmarshal(aPayload, &aSpans)
		if err != nil {
			t.Errorf("failed to parse json payload: %v", err)
		}
		eSpans := spans[sofar : sofar+len(aSpans)]
		sofar += len(aSpans)
		onReceive(len(aSpans))

		ePayload, err := serializer.Serialize(eSpans)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if !bytes.Equal(aPayload, ePayload) {
			t.Errorf("unexpected span payload\nhave %s\nwant %s", string(aPayload), string(ePayload))
		}
	}))
}

func TestSpanIsBeingReported(t *testing.T) {
	serializer := reporter.JSONSerializer{}

	var numSpans int64
	eNumSpans := 2
	spans := generateSpans(eNumSpans)
	ts := newTestServer(t, spans, serializer, func(num int) { atomic.AddInt64(&numSpans, int64(num)) })
	defer ts.Close()

	rep := zipkinhttp.NewReporter(ts.URL, zipkinhttp.Serializer(serializer))
	for _, span := range spans {
		rep.Send(*span)
	}
	rep.Close()

	aNumSpans := int(atomic.LoadInt64(&numSpans))
	if aNumSpans != eNumSpans {
		t.Errorf("unexpected number of spans received\nhave: %d, want: %d", aNumSpans, eNumSpans)
	}
}

func TestSpanIsReportedOnTime(t *testing.T) {
	serializer := reporter.JSONSerializer{}
	batchInterval := 200 * time.Millisecond

	var numSpans int64
	eNumSpans := 2
	spans := generateSpans(eNumSpans)
	ts := newTestServer(t, spans, serializer, func(num int) { atomic.AddInt64(&numSpans, int64(num)) })
	defer ts.Close()

	rep := zipkinhttp.NewReporter(ts.URL,
		zipkinhttp.Serializer(serializer),
		zipkinhttp.BatchInterval(batchInterval))

	for _, span := range spans {
		rep.Send(*span)
	}

	time.Sleep(3 * batchInterval / 2)

	aNumSpans := int(atomic.LoadInt64(&numSpans))
	if aNumSpans != eNumSpans {
		t.Errorf("unexpected number of spans received\nhave: %d, want: %d", aNumSpans, eNumSpans)
	}

	rep.Close()
}

func TestSpanIsReportedAfterBatchSize(t *testing.T) {
	serializer := reporter.JSONSerializer{}
	batchSize := 2

	var numSpans int64
	eNumSpans := 6
	spans := generateSpans(eNumSpans)
	ts := newTestServer(t, spans, serializer, func(num int) { atomic.AddInt64(&numSpans, int64(num)) })
	defer ts.Close()

	rep := zipkinhttp.NewReporter(ts.URL,
		zipkinhttp.Serializer(serializer),
		zipkinhttp.BatchSize(batchSize))

	for _, span := range spans[:batchSize] {
		rep.Send(*span)
	}

	time.Sleep(100 * time.Millisecond)

	aNumSpans := int(atomic.LoadInt64(&numSpans))
	if aNumSpans != batchSize {
		t.Errorf("unexpected number of spans received\nhave: %d, want: %d", aNumSpans, batchSize)
	}

	for _, span := range spans[batchSize:] {
		rep.Send(*span)
	}

	rep.Close()

	aNumSpans = int(atomic.LoadInt64(&numSpans))
	if aNumSpans != eNumSpans {
		t.Errorf("unexpected number of spans received\nhave: %d, want: %d", aNumSpans, eNumSpans)
	}
}

func TestSpanCustomHeaders(t *testing.T) {
	serializer := reporter.JSONSerializer{}

	hc := headerClient{
		headers: http.Header{
			"Key1": []string{"val1a", "val1b"},
			"Key2": []string{"val2"},
		},
	}
	var haveHeaders http.Header
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		haveHeaders = r.Header
	}))
	defer ts.Close()

	spans := generateSpans(1)

	rep := zipkinhttp.NewReporter(
		ts.URL,
		zipkinhttp.Serializer(serializer),
		zipkinhttp.Client(hc),
	)
	for _, span := range spans {
		rep.Send(*span)
	}
	rep.Close()

	for _, key := range []string{"Key1", "Key2"} {
		if want, have := hc.headers[key], haveHeaders[key]; !reflect.DeepEqual(want, have) {
			t.Errorf("header %s: want: %v, have: %v\n", key, want, have)
		}
	}
}

func TestB3SamplingHeader(t *testing.T) {
	serializer := reporter.JSONSerializer{}

	var haveHeaders map[string][]string
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		haveHeaders = r.Header
	}))
	defer ts.Close()

	spans := generateSpans(1)

	rep := zipkinhttp.NewReporter(
		ts.URL,
		zipkinhttp.Serializer(serializer),
	)
	for _, span := range spans {
		rep.Send(*span)
	}
	rep.Close()

	if want, have := []string{"0"}, haveHeaders["B3"]; !reflect.DeepEqual(want, have) {
		t.Errorf("B3 header: want: %v, have %v", want, have)
	}
}

func TestBacklogKeepsTraceStructure(t *testing.T) {
	var (
		mtx  sync.Mutex
		got  []*model.SpanModel
		seen = make(chan struct{}, 1)
	)
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		var spans []*model.SpanModel
		if err := json.NewDecoder(r.Body).Decode(&spans); err != nil {
			t.Errorf("failed to decode spans: %v", err)
			return
		}
		mtx.Lock()
		got = append(got, spans...)
		mtx.Unlock()
		select {
		case seen <- struct{}{}:
		default:
		}
	}))
	defer ts.Close()

	rep := zipkinhttp.NewReporter(
		ts.URL,
		zipkinhttp.BatchInterval(time.Hour),
		zipkinhttp.MaxBacklog(2),
		zipkinhttp.Logger(log.New(io.Discard, "", 0)),
	)

	traceIDs := []model.TraceID{{Low: 1}, {Low: 2}, {Low: 3}, {Low: 4}}
	for i, traceID := range traceIDs {
		parentID := model.ID(i*2 + 1)
		childID := model.ID(i*2 + 2)
		rep.Send(model.SpanModel{
			SpanContext: model.SpanContext{
				TraceID: traceID,
				ID:      parentID,
			},
			Kind:      model.Server,
			Timestamp: time.Now(),
			Tags:      map[string]string{"detail": "parent"},
		})
		rep.Send(model.SpanModel{
			SpanContext: model.SpanContext{
				TraceID:  traceID,
				ID:       childID,
				ParentID: &parentID,
			},
			Kind:      model.Client,
			Timestamp: time.Now(),
			Annotations: []model.Annotation{
				{Timestamp: time.Now(), Value: "child detail"},
			},
		})
	}
	rep.Close()

	<-seen
	mtx.Lock()
	reported := append([]*model.SpanModel(nil), got...)
	mtx.Unlock()

	byTrace := groupSpansByTrace(reported)
	for _, traceID := range traceIDs {
		spans := byTrace[traceID]
		if len(spans) != 2 {
			t.Fatalf("trace %s has %d spans, want 2", traceID, len(spans))
		}
		ids := map[model.ID]bool{}
		for _, span := range spans {
			ids[span.ID] = true
		}
		for _, span := range spans {
			if span.ParentID != nil && !ids[*span.ParentID] {
				t.Fatalf("trace %s has dangling parent %s", traceID, *span.ParentID)
			}
		}
	}
}

func TestBacklogDropsWholeTracesOnly(t *testing.T) {
	var (
		mtx sync.Mutex
		got []*model.SpanModel
	)
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		var spans []*model.SpanModel
		if err := json.NewDecoder(r.Body).Decode(&spans); err != nil {
			t.Errorf("failed to decode spans: %v", err)
			return
		}
		mtx.Lock()
		got = append(got, spans...)
		mtx.Unlock()
	}))
	defer ts.Close()

	rep := zipkinhttp.NewReporter(
		ts.URL,
		zipkinhttp.BatchInterval(time.Hour),
		zipkinhttp.MaxBacklog(1),
		zipkinhttp.Logger(log.New(io.Discard, "", 0)),
	)

	const traceCount = 12
	for i := 0; i < traceCount; i++ {
		traceID := model.TraceID{Low: uint64(i + 1)}
		parentID := model.ID(i*2 + 1)
		childID := model.ID(i*2 + 2)
		rep.Send(model.SpanModel{
			SpanContext: model.SpanContext{TraceID: traceID, ID: parentID},
			Timestamp:   time.Now(),
		})
		rep.Send(model.SpanModel{
			SpanContext: model.SpanContext{
				TraceID:  traceID,
				ID:       childID,
				ParentID: &parentID,
			},
			Timestamp: time.Now(),
		})
	}

	// The oldest trace must already be tombstoned. This late span must not be
	// reported on its own and create an orphan.
	lateTraceID := model.TraceID{Low: 1}
	rep.Send(model.SpanModel{
		SpanContext: model.SpanContext{TraceID: lateTraceID, ID: 1000},
		Timestamp:   time.Now(),
	})
	rep.Close()

	mtx.Lock()
	reported := append([]*model.SpanModel(nil), got...)
	mtx.Unlock()

	byTrace := groupSpansByTrace(reported)
	complete := 0
	dropped := 0
	for i := 0; i < traceCount; i++ {
		traceID := model.TraceID{Low: uint64(i + 1)}
		switch len(byTrace[traceID]) {
		case 0:
			dropped++
		case 2:
			complete++
		default:
			t.Fatalf("trace %s was partially reported: %d spans", traceID, len(byTrace[traceID]))
		}
	}
	if dropped == 0 {
		t.Fatal("expected at least one whole trace to be dropped")
	}
	if complete == 0 {
		t.Fatal("expected at least one whole trace to be reported")
	}
	if len(byTrace[lateTraceID]) != 0 {
		t.Fatalf("late span for dropped trace was reported: %d spans", len(byTrace[lateTraceID]))
	}
}

func TestSendDoesNotBlockOnCollector(t *testing.T) {
	client := &blockingClient{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	rep := zipkinhttp.NewReporter(
		"http://collector.invalid/api/v2/spans",
		zipkinhttp.BatchInterval(time.Hour),
		zipkinhttp.BatchSize(1),
		zipkinhttp.MaxBacklog(1),
		zipkinhttp.Client(client),
		zipkinhttp.Logger(log.New(io.Discard, "", 0)),
	)

	traceID := model.TraceID{Low: 1}
	rep.Send(model.SpanModel{
		SpanContext: model.SpanContext{TraceID: traceID, ID: 1},
		Timestamp:   time.Now(),
	})

	select {
	case <-client.started:
	case <-time.After(2 * time.Second):
		close(client.release)
		t.Fatal("reporter did not start network send")
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			id := model.ID(i + 2)
			rep.Send(model.SpanModel{
				SpanContext: model.SpanContext{TraceID: traceID, ID: id, ParentID: idPtr(1)},
				Timestamp:   time.Now(),
			})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		close(client.release)
		t.Fatal("Send blocked while the collector was slow")
	}

	close(client.release)
	if err := rep.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

type blockingClient struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingClient) Do(req *http.Request) (*http.Response, error) {
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader(nil)),
	}, nil
}

func groupSpansByTrace(spans []*model.SpanModel) map[model.TraceID][]*model.SpanModel {
	byTrace := make(map[model.TraceID][]*model.SpanModel)
	for _, span := range spans {
		byTrace[span.TraceID] = append(byTrace[span.TraceID], span)
	}
	return byTrace
}

func idPtr(id model.ID) *model.ID {
	return &id
}

type headerClient struct {
	client  http.Client
	headers map[string][]string
}

func (h headerClient) Do(req *http.Request) (*http.Response, error) {
	for key, item := range h.headers {
		for _, val := range item {
			req.Header.Add(key, val)
		}
	}
	return h.client.Do(req)
}
