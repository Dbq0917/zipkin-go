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

package async

import (
	"context"

	zipkin "github.com/openzipkin/zipkin-go"
)

// Snapshot freezes the caller's context into an immutable value that can cross
// a goroutine boundary. The returned context is detached from the caller's
// cancellation/deadline: a request that returns early must not cancel work it
// deliberately delegated to a worker. Capture it on the submitting goroutine
// and pass the snapshot into the task closure; never store a request context in
// package level or worker level mutable state.
func Snapshot(ctx context.Context) context.Context {
	if span := zipkin.SpanFromContext(ctx); span != nil {
		return zipkin.NewContext(context.Background(), span)
	}
	return context.Background()
}

// Instrumenter attaches asynchronous work to the trace active at submission
// time. The zero value is not usable; create one with NewInstrumenter.
type Instrumenter struct {
	tracer *zipkin.Tracer
	opts   []zipkin.SpanOption
}

// InstrumenterOption customizes the spans created by an Instrumenter.
type InstrumenterOption func(*Instrumenter)

// WithSpanOptions adds Zipkin SpanOptions applied to every asynchronous span.
func WithSpanOptions(opts ...zipkin.SpanOption) InstrumenterOption {
	return func(in *Instrumenter) {
		in.opts = append(in.opts, opts...)
	}
}

// NewInstrumenter returns an Instrumenter backed by tracer. A nil tracer
// produces an Instrumenter that only propagates the context without creating
// spans.
func NewInstrumenter(tracer *zipkin.Tracer, opts ...InstrumenterOption) *Instrumenter {
	in := &Instrumenter{tracer: tracer}
	for _, opt := range opts {
		opt(in)
	}
	return in
}

// Wrap captures ctx at submission time and returns a task that restores the
// captured context and, when a tracer is configured, runs fn inside a child
// span named name. The returned func carries its own immutable snapshot, so it
// is safe to run on a reused worker without leaking context into another task.
func (in *Instrumenter) Wrap(ctx context.Context, name string, fn func(context.Context)) func() {
	snapshot := Snapshot(ctx)
	return func() {
		in.run(snapshot, name, fn)
	}
}

// WrapErr is Wrap for tasks that return an error. A non-nil error is tagged on
// the asynchronous span and returned unchanged.
func (in *Instrumenter) WrapErr(ctx context.Context, name string, fn func(context.Context) error) func() error {
	snapshot := Snapshot(ctx)
	return func() error {
		return in.runErr(snapshot, name, fn)
	}
}

// StartSpan opens a child span on the captured context and returns the context
// that carries it together with the span. It is used by building blocks that
// need the span reference (for example Batch). When no tracer is configured the
// context is returned unchanged with a nil span.
func (in *Instrumenter) StartSpan(ctx context.Context, name string) (context.Context, zipkin.Span) {
	if in.tracer == nil {
		return ctx, nil
	}
	span, childCtx := in.tracer.StartSpanFromContext(ctx, name, in.opts...)
	return childCtx, span
}

func (in *Instrumenter) run(ctx context.Context, name string, fn func(context.Context)) {
	runCtx, span := in.StartSpan(ctx, name)
	defer finish(span)
	fn(runCtx)
}

func (in *Instrumenter) runErr(ctx context.Context, name string, fn func(context.Context) error) error {
	runCtx, span := in.StartSpan(ctx, name)
	defer finish(span)
	err := fn(runCtx)
	if err != nil && span != nil {
		span.Tag(string(zipkin.TagError), err.Error())
	}
	return err
}

func finish(span zipkin.Span) {
	if span != nil {
		span.Finish()
	}
}
