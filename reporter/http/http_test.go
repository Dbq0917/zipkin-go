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
	"io/ioutil"
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

func TestConcurrentReporting(t *testing.T) {
	serializer := reporter.JSONSerializer{}

	var (
		numSpans int64
		dupSpans int64
		seenIDs  sync.Map
	)

	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		aPayload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		var aSpans []*model.SpanModel
		if err := json.Unmarshal(aPayload, &aSpans); err != nil {
			t.Errorf("failed to parse json payload: %v", err)
			return
		}

		for _, span := range aSpans {
			if _, loaded := seenIDs.LoadOrStore(span.ID, struct{}{}); loaded {
				atomic.AddInt64(&dupSpans, 1)
			}
		}
		atomic.AddInt64(&numSpans, int64(len(aSpans)))
	}))
	defer ts.Close()

	// Small batch size and batch interval so both the size-based and the
	// time-based flush triggers fire concurrently while spans arrive.
	rep := zipkinhttp.NewReporter(ts.URL,
		zipkinhttp.Serializer(serializer),
		zipkinhttp.BatchSize(7),
		zipkinhttp.BatchInterval(5*time.Millisecond),
	)

	const (
		producers        = 8
		spansPerProducer = 50
	)
	eNumSpans := producers * spansPerProducer

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			spans := generateSpans(spansPerProducer)
			for i, span := range spans {
				span.ID = model.ID(p*spansPerProducer + i + 1)
				rep.Send(*span)
			}
		}(p)
	}
	wg.Wait()
	rep.Close()

	if aNumSpans := int(atomic.LoadInt64(&numSpans)); aNumSpans != eNumSpans {
		t.Errorf("unexpected number of spans received\nhave: %d, want: %d", aNumSpans, eNumSpans)
	}
	if aDupSpans := int(atomic.LoadInt64(&dupSpans)); aDupSpans != 0 {
		t.Errorf("unexpected number of duplicate spans received\nhave: %d, want: 0", aDupSpans)
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
