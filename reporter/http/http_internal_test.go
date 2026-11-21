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
	"io/ioutil"
	"log"
	"sync"
	"testing"

	"github.com/openzipkin/zipkin-go/idgenerator"
	"github.com/openzipkin/zipkin-go/model"
)

func BenchmarkAppendUnderBacklogLimit(b *testing.B) {
	const maxBacklog = 1000
	idGen := idgenerator.NewRandom64()
	r := newBacklogReporter(maxBacklog)

	// prefill up to the limit minus one so normal appends never overflow
	for len(r.batch) < maxBacklog-1 {
		r.append(&model.SpanModel{
			SpanContext: model.SpanContext{
				TraceID: idGen.TraceID(),
				ID:      idGen.SpanID(model.TraceID{}),
			},
		})
	}

	span := &model.SpanModel{
		SpanContext: model.SpanContext{
			TraceID: idGen.TraceID(),
			ID:      idGen.SpanID(model.TraceID{}),
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// replace the last slot each iteration so the backlog stays full and
		// never enters the disposal branch
		r.batch = r.batch[:maxBacklog-1]
		r.append(span)
	}
}

func newBacklogReporter(maxBacklog int) *httpReporter {
	return &httpReporter{
		maxBacklog:      maxBacklog,
		batchMtx:        &sync.Mutex{},
		batch:           []*model.SpanModel{},
		droppedTraceIDs: map[model.TraceID]struct{}{},
		logger:          log.New(ioutil.Discard, "", 0),
	}
}

func traceSpans(idGen idgenerator.IDGenerator, n int) []*model.SpanModel {
	traceID := idGen.TraceID()
	spans := make([]*model.SpanModel, n)
	for i := range spans {
		spans[i] = &model.SpanModel{
			SpanContext: model.SpanContext{
				TraceID: traceID,
				ID:      idGen.SpanID(traceID),
			},
		}
	}
	return spans
}

// TestAppendDisposesWholeTraces simulates a single large burst that forces
// one big disposal cut, including the reported ordering where the parent span
// of a trace is already inside the kept backlog while its child spans fall
// into the disposed region.
func TestAppendDisposesWholeTraces(t *testing.T) {
	const (
		maxBacklog = 12
		spanPerTr  = 4
	)
	idGen := idgenerator.NewRandom64()
	r := newBacklogReporter(maxBacklog)

	// prefill the backlog with whole traces up to the limit
	for i := 0; i < maxBacklog/spanPerTr; i++ {
		for _, span := range traceSpans(idGen, spanPerTr) {
			r.append(span)
		}
	}

	// one burst of two traces (8 spans): the cut at maxBacklog lands inside
	// the burst and would split traces under positional disposal.
	burst := append(traceSpans(idGen, spanPerTr), traceSpans(idGen, spanPerTr)...)
	for _, span := range burst {
		r.append(span)
	}

	counts := map[model.TraceID]int{}
	for _, span := range r.batch {
		counts[span.TraceID]++
	}
	for traceID, n := range counts {
		if n != 0 && n != spanPerTr {
			t.Fatalf("trace %s split by disposal: kept %d of %d spans", traceID, n, spanPerTr)
		}
	}
	if len(r.batch) > maxBacklog {
		t.Fatalf("backlog not bounded: %d spans, limit %d", len(r.batch), maxBacklog)
	}
}

// TestAppendParentKeptChildDroppedScenario reproduces the exact incident
// pattern: parent span arrived first and sits inside the kept region, its
// child spans arrive later and land in the disposed region. Both must go.
func TestAppendParentKeptChildDroppedScenario(t *testing.T) {
	const maxBacklog = 2
	idGen := idgenerator.NewRandom64()
	r := newBacklogReporter(maxBacklog)

	traceID := idGen.TraceID()
	parent := &model.SpanModel{SpanContext: model.SpanContext{TraceID: traceID, ID: idGen.SpanID(traceID)}}
	filler := traceSpans(idGen, maxBacklog-1) // unrelated trace filling the rest of the backlog

	for _, span := range filler {
		r.append(span)
	}
	r.append(parent) // backlog now at maxBacklog

	children := make([]*model.SpanModel, 3)
	for i := range children {
		children[i] = &model.SpanModel{SpanContext: model.SpanContext{TraceID: traceID, ID: idGen.SpanID(traceID)}}
		r.append(children[i]) // each one triggers disposal; parent must not survive alone
	}

	for _, span := range r.batch {
		if span.TraceID == traceID {
			t.Fatal("parent survived backlog disposal while its children were dropped")
		}
	}
}
