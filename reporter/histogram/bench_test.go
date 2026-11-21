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
	"testing"
	"time"
)

func BenchmarkObserve(b *testing.B) {
	h := NewPowerOfTwo()
	durations := []time.Duration{
		100 * time.Microsecond,
		time.Millisecond,
		50 * time.Millisecond,
		time.Second,
		30 * time.Second,
		3 * time.Minute,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Observe(durations[i%len(durations)])
	}
}

func BenchmarkObserveGiant(b *testing.B) {
	h := NewPowerOfTwo()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Every observation lands past the preallocated buckets, exercising the
		// contended-growth path as a worst case.
		h.Observe(time.Duration(1) << 40)
	}
}
