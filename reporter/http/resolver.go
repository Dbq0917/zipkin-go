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
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// defaultResolverTTL is how long a resolved set of addresses is reused before
// the host is resolved again. The operating system resolver still applies its
// own caching on top of this layer.
const defaultResolverTTL = 5 * time.Minute

// lookupIPFn resolves a host name into a list of IP addresses.
type lookupIPFn func(ctx context.Context, network, host string) ([]net.IP, error)

// dialContextFn dials the given network/address, mirroring the
// net.Dialer.DialContext signature.
type dialContextFn func(ctx context.Context, network, address string) (net.Conn, error)

type cachedResolution struct {
	addrs      []net.IP
	resolvedAt time.Time
}

// hostResolver caches the resolved addresses of a single host for a bounded
// time. Instead of resolving once at startup and pinning the process to the
// resulting IP forever, the cache is refreshed after ttl, so a changed DNS
// record is picked up automatically without paying the resolution cost on
// every report.
type hostResolver struct {
	host     string
	ttl      time.Duration
	lookupIP lookupIPFn
	dial     dialContextFn
	logger   *log.Logger
	onChange func()

	mu     sync.RWMutex
	cached *cachedResolution
}

func newHostResolver(host string, ttl time.Duration, lookup lookupIPFn, dial dialContextFn, logger *log.Logger, onChange func()) *hostResolver {
	if lookup == nil {
		var systemResolver net.Resolver
		lookup = systemResolver.LookupIP
	}
	if dial == nil {
		var dialer net.Dialer
		dial = dialer.DialContext
	}
	return &hostResolver{
		host:     host,
		ttl:      ttl,
		lookupIP: lookup,
		dial:     dial,
		logger:   logger,
		onChange: onChange,
	}
}

func (c *cachedResolution) expired(ttl time.Duration, now time.Time) bool {
	return c == nil || now.Sub(c.resolvedAt) >= ttl
}

// addresses returns the resolved IP addresses, reusing the cached result while
// it is still fresh. On a lookup failure the last known addresses are kept and
// returned so a transient DNS outage does not break reporting; the failure is
// logged either way. Only when no address has ever been resolved successfully
// does the lookup error propagate to the caller.
func (h *hostResolver) addresses(ctx context.Context) ([]net.IP, error) {
	h.mu.RLock()
	fresh := h.cached != nil && !h.cached.expired(h.ttl, time.Now())
	if fresh {
		addrs := h.cached.addrs
		h.mu.RUnlock()
		return addrs, nil
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()

	// Another goroutine may have refreshed the entry while we waited.
	if h.cached != nil && !h.cached.expired(h.ttl, time.Now()) {
		return h.cached.addrs, nil
	}

	addrs, err := h.lookupIP(ctx, "ip", h.host)
	if err != nil || len(addrs) == 0 {
		if err == nil {
			err = errors.New("dns lookup returned no addresses")
		}
		if h.cached != nil {
			h.logger.Printf("dns lookup for %q failed, reusing stale addresses %s: %s",
				h.host, formatIPs(h.cached.addrs), err)
			return h.cached.addrs, nil
		}
		h.logger.Printf("dns lookup for %q failed with no cached address available: %s", h.host, err)
		return nil, err
	}

	if !sameAddrs(h.cached, addrs) {
		h.logger.Printf("dns resolution for %q changed: %s -> %s",
			h.host, formatCachedAddrs(h.cached), formatIPs(addrs))
		if h.onChange != nil {
			h.onChange()
		}
	}
	h.cached = &cachedResolution{addrs: addrs, resolvedAt: time.Now()}
	return h.cached.addrs, nil
}

// dialContext replaces the host part of the address requested by the HTTP
// transport with a freshly resolved address, trying each returned IP in order.
func (h *hostResolver) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}

	addrs, err := h.addresses(ctx)
	if err != nil {
		return nil, err
	}

	var errs []error
	for _, ip := range addrs {
		conn, dialErr := h.dial(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		errs = append(errs, dialErr)
	}
	return nil, errors.Join(errs...)
}

// newResolvingTransport builds an http.Transport whose DialContext resolves the
// URL's host through a TTL-bounded cache. It returns a nil transport when the
// endpoint is an IP literal, as there is nothing to resolve.
func newResolvingTransport(endpointURL string, ttl time.Duration, lookup lookupIPFn, dial dialContextFn, logger *log.Logger) (*http.Transport, error) {
	parsedURL, err := url.Parse(endpointURL)
	if err != nil {
		return nil, err
	}

	host := parsedURL.Hostname()
	if net.ParseIP(host) != nil {
		return nil, nil
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	// A pooled, long-lived connection would bypass DialContext and therefore
	// never observe a DNS change. Each report batch opens a fresh connection
	// so the TTL-bounded cache is consulted; in-cache hits avoid any lookup.
	transport.DisableKeepAlives = true
	resolver := newHostResolver(host, ttl, lookup, dial, logger, transport.CloseIdleConnections)
	transport.DialContext = resolver.dialContext
	return transport, nil
}

func sameAddrs(cached *cachedResolution, addrs []net.IP) bool {
	if cached == nil || len(cached.addrs) != len(addrs) {
		return false
	}
	for i := range addrs {
		if !cached.addrs[i].Equal(addrs[i]) {
			return false
		}
	}
	return true
}

func formatCachedAddrs(cached *cachedResolution) string {
	if cached == nil {
		return "<none>"
	}
	return formatIPs(cached.addrs)
}

func formatIPs(addrs []net.IP) string {
	parts := make([]string, len(addrs))
	for i, addr := range addrs {
		parts[i] = addr.String()
	}
	return strings.Join(parts, ",")
}
