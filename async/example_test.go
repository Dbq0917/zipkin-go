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

package async_test

import (
	"context"
	"fmt"

	zipkin "github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/async"
	"github.com/openzipkin/zipkin-go/reporter"
)

func ExampleInstrumenter_Wrap() {
	tracer, _ := zipkin.NewTracer(reporter.NewNoopReporter())
	in := async.NewInstrumenter(tracer)

	root, ctx := tracer.StartSpanFromContext(context.Background(), "request")
	defer root.Finish()

	// The business callback keeps its func(context.Context) signature. Its
	// context is captured here and restored on whichever goroutine runs it.
	callback := in.Wrap(ctx, "async-callback", func(runCtx context.Context) {
		span := zipkin.SpanFromContext(runCtx)
		fmt.Println(span != nil)
	})

	// A reused worker only ever sees this task's own immutable snapshot.
	callback()

	// Output:
	// true
}

func ExamplePool_Go() {
	tracer, _ := zipkin.NewTracer(reporter.NewNoopReporter())
	pool := async.NewPool(4, 16, async.WithPoolTracer(tracer))
	defer pool.Close()

	root, ctx := tracer.StartSpanFromContext(context.Background(), "request")
	defer root.Finish()

	if err := pool.Go(ctx, "worker-callback", func(runCtx context.Context) {
		_ = zipkin.SpanFromContext(runCtx)
	}); err != nil {
		fmt.Println(err)
	}

	// Output:
}

func ExampleBatch_Run() {
	tracer, _ := zipkin.NewTracer(reporter.NewNoopReporter())
	pool := async.NewPool(2, 8, async.WithPoolTracer(tracer))
	defer pool.Close()

	root, ctx := tracer.StartSpanFromContext(context.Background(), "request")
	defer root.Finish()

	batch := async.NewBatch(tracer, pool)
	for i := 0; i < 3; i++ {
		batch.Add("sub-task", func(runCtx context.Context) error {
			_ = zipkin.SpanFromContext(runCtx)
			return nil
		})
	}

	if err := batch.Run(ctx, "batch"); err != nil {
		fmt.Println(err)
	}

	// Output:
}
