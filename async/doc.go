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

// Package async propagates the Zipkin tracing context across goroutines and
// reused worker pools.
//
// Go stores request scoped values inside context.Context instead of thread
// locals. When work is handed to another goroutine (a callback, a worker pool,
// a batch fan-out) the executing goroutine starts from context.Background(),
// so any span started there has no parent and becomes a new root. The request
// is then rendered as several unrelated trace trees.
//
// The helpers in this package follow one rule: capture the caller's context at
// submission time, store an immutable snapshot inside the task closure, and
// restore that snapshot when the task runs. A worker never reads mutable
// goroutine-local state, and every task carries its own captured context, so a
// reused worker cannot leak one request's context into the next request.
//
// Two layers are provided.
//
// Snapshot and Instrumenter are the low level primitives. Snapshot makes a
// context safe to cross a goroutine boundary; Instrumenter additionally opens
// a child span for the asynchronous work and re-attaches it to the parent
// trace.
//
// Pool and Batch are the high level building blocks. Pool wraps a bounded set
// of reused workers and guarantees per-task context isolation. Batch fans a set
// of sub-tasks out under a single batch span so each sub-task becomes a child
// instead of an independent root.
//
// Business function signatures do not need to change: existing func() and
// func() error values are wrapped at the call site.
package async
