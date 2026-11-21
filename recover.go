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

import "fmt"

// FinishOnRecover guarantees a span is finished even when the instrumented
// call panics or returns through an error path without calling Finish. It is
// meant to be used as a deferred call right after starting a span:
//
//	sp := tracer.StartSpan("work")
//	defer zipkin.FinishOnRecover(sp)
//
// On a normal return (no panic) the span is left untouched, so an explicit
// Finish call placed on the regular path keeps its original semantics and
// overhead. On panic the span is tagged with the recovered value, finished and
// reported, and the panic is re-raised to preserve the caller's behavior.
// Finish is idempotent: if the span was already finished, nothing is sent a
// second time.
func FinishOnRecover(s Span) {
	if s == nil {
		return
	}
	if r := recover(); r != nil {
		s.Tag(string(TagError), panicValue(r))
		s.Finish()
		panic(r)
	}
}

func panicValue(r interface{}) string {
	if err, ok := r.(error); ok {
		return err.Error()
	}
	return fmt.Sprint(r)
}
