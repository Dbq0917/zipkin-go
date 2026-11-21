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
	"sync/atomic"
	"testing"
	"time"

	"github.com/openzipkin/zipkin-go/model"
)

type benchReporter struct{ count int64 }

func (b *benchReporter) Send(model.SpanModel) { atomic.AddInt64(&b.count, 1) }
func (b *benchReporter) Close() error         { return nil }

func BenchmarkStartFinishWithoutReaper(b *testing.B) {
	tr, _ := NewTracer(&benchReporter{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp := tr.StartSpan("bench")
		sp.Finish()
	}
}

func BenchmarkStartFinishWithReaper(b *testing.B) {
	tr, _ := NewTracer(&benchReporter{}, WithSpanReaper(time.Minute))
	defer tr.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sp := tr.StartSpan("bench")
		sp.Finish()
	}
}

func BenchmarkFinishOnRecoverNormalPath(b *testing.B) {
	tr, _ := NewTracer(&benchReporter{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			sp := tr.StartSpan("bench")
			defer FinishOnRecover(sp)
			sp.Finish()
		}()
	}
}
