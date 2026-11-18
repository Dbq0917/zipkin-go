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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Sampler functions return if a Zipkin span should be sampled, based on its
// traceID.
type Sampler func(id uint64) bool

// NeverSample will always return false. If used by a service it will not allow
// the service to start traces but will still allow the service to participate
// in traces started upstream.
func NeverSample(_ uint64) bool { return false }

// AlwaysSample will always return true. If used by a service it will always start
// traces if no upstream trace has been propagated. If an incoming upstream trace
// is not sampled the service will adhere to this and only propagate the context.
func AlwaysSample(_ uint64) bool { return true }

// NewModuloSampler provides a generic type Sampler.
func NewModuloSampler(mod uint64) Sampler {
	if mod < 2 {
		return AlwaysSample
	}
	return func(id uint64) bool {
		return (id % mod) == 0
	}
}

// NewBoundarySampler is appropriate for high-traffic instrumentation who
// provision random trace ids, and make the sampling decision only once.
// It defends against nodes in the cluster selecting exactly the same ids.
func NewBoundarySampler(rate float64, salt int64) (Sampler, error) {
	if err := validateBoundaryRate(rate); err != nil {
		return nil, err
	}
	if rate == 0.0 {
		return NeverSample, nil
	}
	if rate == 1.0 {
		return AlwaysSample, nil
	}

	var (
		// convert rate into a proportional boundary where values below it are sampled
		//   (e.g., 1% rate ≈ first 1% of space)
		boundary = uint64(rate * (1 << 63))
		usalt    = uint64(salt)
	)
	return func(id uint64) bool {
		// XOR with salt provides deterministic randomization
		//   right shift ensures uniform distribution across [0, 2^63)
		return ((id ^ usalt) >> 1) < boundary
	}, nil
}

func validateBoundaryRate(rate float64) error {
	if rate != 0.0 && rate != 1.0 && (rate < 0.0001 || rate > 1) {
		return fmt.Errorf("rate should be 0.0 or between 0.0001 and 1: was %f", rate)
	}
	return nil
}

// DynamicSampler is a boundary Sampler whose rate can be updated at runtime.
// The sampling decision stays deterministic per trace id for a given rate, so
// all instances holding the same rate and salt make identical decisions for
// the same trace. Typical usage is to create one DynamicSampler at startup,
// hand its Sample method to WithSampler, and call SetRate from a config
// center watch callback or a background polling goroutine. Reading the
// current rate on the request path is a single atomic load.
type DynamicSampler struct {
	salt     uint64
	boundary atomic.Uint64
}

// NewDynamicSampler returns a DynamicSampler initialized with the given rate
// and salt. The salt must be identical across all instances for cluster-wide
// consistent sampling decisions.
func NewDynamicSampler(rate float64, salt int64) (*DynamicSampler, error) {
	if err := validateBoundaryRate(rate); err != nil {
		return nil, err
	}
	s := &DynamicSampler{salt: uint64(salt)}
	s.boundary.Store(boundaryForRate(rate))
	return s, nil
}

// SetRate updates the sampling rate. It is safe for concurrent use with
// in-flight sampling decisions.
func (s *DynamicSampler) SetRate(rate float64) error {
	if err := validateBoundaryRate(rate); err != nil {
		return err
	}
	s.boundary.Store(boundaryForRate(rate))
	return nil
}

// Rate returns the currently effective sampling rate.
func (s *DynamicSampler) Rate() float64 {
	return float64(s.boundary.Load()) / (1 << 63)
}

// Sample implements the Sampler function signature and can be passed
// directly to WithSampler.
func (s *DynamicSampler) Sample(id uint64) bool {
	return ((id ^ s.salt) >> 1) < s.boundary.Load()
}

func boundaryForRate(rate float64) uint64 {
	if rate <= 0.0 {
		return 0
	}
	if rate >= 1.0 {
		return 1 << 63
	}
	return uint64(rate * (1 << 63))
}

// NewCountingSampler is appropriate for low-traffic instrumentation or
// those who do not provision random trace ids. It is not appropriate for
// collectors as the sampling decision isn't idempotent (consistent based
// on trace id).
func NewCountingSampler(rate float64) (Sampler, error) {
	if rate == 0.0 {
		return NeverSample, nil
	}
	if rate == 1.0 {
		return AlwaysSample, nil
	}
	if rate < 0.01 || rate > 1 {
		return nil, fmt.Errorf("rate should be 0.0 or between 0.01 and 1: was %f", rate)
	}
	var (
		i         = 0
		outOf100  = int(rate*100 + math.Copysign(0.5, rate*100)) // for rounding float to int conversion instead of truncation
		decisions = randomBitSet(100, outOf100, rand.New(rand.NewSource(time.Now().UnixNano())))
		mtx       = &sync.Mutex{}
	)

	return func(_ uint64) bool {
		mtx.Lock()
		result := decisions[i]
		i++
		if i == 100 {
			i = 0
		}
		mtx.Unlock()
		return result
	}, nil
}

/**
 * Reservoir sampling algorithm borrowed from Stack Overflow.
 *
 * http://stackoverflow.com/questions/12817946/generate-a-random-bitset-with-n-1s
 */
func randomBitSet(size int, cardinality int, rnd *rand.Rand) []bool {
	result := make([]bool, size)
	chosen := make([]int, cardinality)
	var i int
	for i = 0; i < cardinality; i++ {
		chosen[i] = i
		result[i] = true
	}
	for ; i < size; i++ {
		j := rnd.Intn(i + 1)
		if j < cardinality {
			result[chosen[j]] = false
			result[i] = true
			chosen[j] = i
		}
	}
	return result
}
