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
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter"
)

const (
	testOldIP = "127.0.0.1"
	testNewIP = "127.0.0.2"
)

func newCountingServer(t *testing.T) (*httptest.Server, *int64) {
	t.Helper()
	var received int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		atomic.AddInt64(&received, 1)
	}))
	t.Cleanup(server.Close)
	return server, &received
}

func serverPort(t *testing.T, server *httptest.Server) string {
	t.Helper()
	parsedURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse server url %q: %s", server.URL, err)
	}
	return parsedURL.Port()
}

func sendOneSpan(t *testing.T, rep reporter.Reporter) {
	t.Helper()
	rep.Send(model.SpanModel{Name: "name"})
}

// TestResolverSwitchesAddressAfterTTL reuses the existing reporting flow with
// the collector host mapped to one of two loopback IPs, each representing a
// different collector port. After the TTL elapses the lookup returns the new
// "address" and subsequent batches must arrive at the new port without a
// restart.
func TestResolverSwitchesAddressAfterTTL(t *testing.T) {
	oldServer, oldReceived := newCountingServer(t)
	newServer, newReceived := newCountingServer(t)
	oldPort := serverPort(t, oldServer)
	newPort := serverPort(t, newServer)

	const ttl = 200 * time.Millisecond
	var stage int32
	lookup := func(_ context.Context, _, _ string) ([]net.IP, error) {
		if atomic.LoadInt32(&stage) == 0 {
			return []net.IP{net.ParseIP(testOldIP)}, nil
		}
		return []net.IP{net.ParseIP(testNewIP)}, nil
	}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			t.Errorf("unexpected dial address %q: %s", address, err)
			return nil, err
		}
		port := oldPort
		if host == testNewIP {
			port = newPort
		}
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, net.JoinHostPort(testOldIP, port))
	}

	var logBuffer bytes.Buffer
	rep := NewReporter("http://collector.example/api/v2/spans",
		Serializer(reporter.JSONSerializer{}),
		BatchInterval(20*time.Millisecond),
		ResolverTTL(ttl),
		withLookupFn(lookup),
		withDialFn(dial),
		Logger(log.New(&logBuffer, "", 0)),
	)
	t.Cleanup(func() { _ = rep.Close() })

	sendOneSpan(t, rep)
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(oldReceived) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt64(oldReceived) == 0 {
		t.Fatal("old collector never received a span before the address change")
	}

	// Point the name at the new collector and wait long enough for the TTL to
	// expire plus at least one batch interval to flush a fresh report.
	switchAt := time.Now()
	atomic.StoreInt32(&stage, 1)

	deadline = time.Now().Add(3 * time.Second)
	for atomic.LoadInt64(newReceived) == 0 && time.Now().Before(deadline) {
		sendOneSpan(t, rep)
		time.Sleep(10 * time.Millisecond)
	}

	switchDuration := time.Since(switchAt)
	if atomic.LoadInt64(newReceived) == 0 {
		t.Fatalf("new collector never received a span; logs:\n%s", logBuffer.String())
	}

	// The switch must be driven by the TTL, not by a per-batch lookup: allow
	// one TTL plus slack for the next flush, but require it to actually wait.
	if switchDuration < ttl {
		t.Errorf("address switched after %s, before the %s TTL expired; cache is not respected",
			switchDuration, ttl)
	}
	if switchDuration > ttl+500*time.Millisecond {
		t.Errorf("address switch took %s, expected within ~%s plus flush slack", switchDuration, ttl)
	}

	t.Logf("collector address switched %s -> %s in %s (ttl %s)",
		oldPort, newPort, switchDuration, ttl)

	if !bytes.Contains(logBuffer.Bytes(), []byte("dns resolution")) {
		t.Errorf("expected the address change to be logged; logs:\n%s", logBuffer.String())
	}
}

// TestResolverCachesWithinTTL ensures the host is resolved once and reused for
// every batch while the cache is fresh, instead of resolving on each report.
func TestResolverCachesWithinTTL(t *testing.T) {
	server, received := newCountingServer(t)
	port := serverPort(t, server)

	var lookups int32
	lookup := func(_ context.Context, _, _ string) ([]net.IP, error) {
		atomic.AddInt32(&lookups, 1)
		return []net.IP{net.ParseIP(testOldIP)}, nil
	}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, net.JoinHostPort(testOldIP, port))
	}

	rep := NewReporter("http://collector.example/api/v2/spans",
		Serializer(reporter.JSONSerializer{}),
		BatchInterval(20*time.Millisecond),
		ResolverTTL(time.Minute),
		withLookupFn(lookup),
		withDialFn(dial),
	)
	t.Cleanup(func() { _ = rep.Close() })

	for i := 0; i < 5; i++ {
		sendOneSpan(t, rep)
		time.Sleep(30 * time.Millisecond)
	}

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(received) < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt64(received) < 5 {
		t.Fatalf("only %d of 5 spans were received", atomic.LoadInt64(received))
	}
	if got := atomic.LoadInt32(&lookups); got != 1 {
		t.Errorf("expected a single dns lookup within the TTL, got %d", got)
	}
}

// TestResolverKeepsStaleAddressOnLookupFailure verifies that a failing lookup
// is visible in the logs but reporting continues against the last known
// address, and that a lookup error with no cached address is surfaced.
func TestResolverKeepsStaleAddressOnLookupFailure(t *testing.T) {
	server, received := newCountingServer(t)
	port := serverPort(t, server)

	var failing int32
	lookup := func(_ context.Context, _, _ string) ([]net.IP, error) {
		if atomic.LoadInt32(&failing) == 1 {
			return nil, errors.New("simulated dns outage")
		}
		return []net.IP{net.ParseIP(testOldIP)}, nil
	}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, net.JoinHostPort(testOldIP, port))
	}

	var logBuffer bytes.Buffer
	rep := NewReporter("http://collector.example/api/v2/spans",
		Serializer(reporter.JSONSerializer{}),
		BatchInterval(20*time.Millisecond),
		ResolverTTL(30*time.Millisecond),
		withLookupFn(lookup),
		withDialFn(dial),
		Logger(log.New(&logBuffer, "", 0)),
	)
	t.Cleanup(func() { _ = rep.Close() })

	sendOneSpan(t, rep)
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(received) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt64(received) == 0 {
		t.Fatal("collector never received the initial span")
	}

	atomic.StoreInt32(&failing, 1)
	beforeFailure := atomic.LoadInt64(received)
	for i := 0; i < 5; i++ {
		sendOneSpan(t, rep)
		time.Sleep(40 * time.Millisecond)
	}

	deadline = time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(received) < beforeFailure+5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := atomic.LoadInt64(received) - beforeFailure; got < 5 {
		t.Errorf("expected reporting to continue on stale address, missing %d spans", 5-got)
	}
	if !bytes.Contains(logBuffer.Bytes(), []byte("dns lookup")) ||
		!bytes.Contains(logBuffer.Bytes(), []byte("reusing stale addresses")) {
		t.Errorf("expected the lookup failure to be logged; logs:\n%s", logBuffer.String())
	}

	// A failing lookup before anything was cached must return the error.
	var coldLog bytes.Buffer
	coldResolver := newHostResolver("collector.example", time.Minute,
		func(context.Context, string, string) ([]net.IP, error) {
			return nil, errors.New("simulated dns outage")
		}, nil, log.New(&coldLog, "", 0), nil)
	if _, err := coldResolver.addresses(context.Background()); err == nil {
		t.Error("expected the cold lookup error to be returned")
	}
	if !bytes.Contains(coldLog.Bytes(), []byte("no cached address available")) {
		t.Errorf("expected the cold lookup failure to be logged; logs:\n%s", coldLog.String())
	}
}
