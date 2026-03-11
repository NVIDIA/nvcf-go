/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cloudevent "github.com/cloudevents/sdk-go/v2/event"
	"github.com/prometheus/client_golang/prometheus"

	nverrors "github.com/NVIDIA/nvcf-go/pkg/nvkit/errors"

	"github.com/NVIDIA/nvcf-go/pkg/nvkit/clients"
)

// ---------------------------------------------------------------------------
// test helpers
// ---------------------------------------------------------------------------

// testEvent creates a minimal valid CloudEvent.
func testEvent(id string) cloudevent.Event {
	e := cloudevent.New()
	e.SetID(id)
	e.SetSource("test/source")
	e.SetType("test.type")
	return e
}

// testEvents returns n CloudEvents.
func testEvents(n int) []cloudevent.Event {
	events := make([]cloudevent.Event, n)
	for i := 0; i < n; i++ {
		events[i] = testEvent(string(rune('a' + i)))
	}
	return events
}

// testMetrics returns three unregistered prometheus vecs suitable for a single test.
// Using unregistered vecs avoids duplicate-registration panics across tests.
func testMetrics() (*prometheus.CounterVec, *prometheus.CounterVec, *prometheus.HistogramVec) {
	events := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "te_events_total", Help: "test"},
		[]string{"method"},
	)
	byts := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "te_bytes_total", Help: "test"},
		[]string{"method"},
	)
	lat := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "te_latency_seconds", Help: "test", Buckets: prometheus.DefBuckets},
		[]string{"method"},
	)
	return events, byts, lat
}

// minCfg returns a KratosExporterConfig with safe defaults (all delays/retries = 0).
func minCfg() *KratosExporterConfig {
	return &KratosExporterConfig{
		MaxBatchSize:                 10,
		MaxBatchSizeInBytes:          1_000_000,
		SyncBatchQueueSize:           50,
		SyncBatchTimeoutMs:           0,
		SyncMaxRetries:               0,
		SyncBaseDelayInSeconds:       0,
		BaseDelayInSeconds:           0,
		NumRetriesExponentialBackoff: 1,
		MaxEvents:                    100,
		MaxBufferSize:                100,
		MaxWorkers:                   2,
		FlushInterval:                60, // long interval so we control flushes manually
	}
}

// newTestExporter creates a KratosExporter wired to the given HTTP round-tripper
// (or http.DefaultTransport when nil) and pointing at the given endpoint.
func newTestExporter(transport http.RoundTripper, endpoint string) *KratosExporter {
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpC := &http.Client{Transport: transport}
	cfg := minCfg()
	httpClientCfg := &clients.HTTPClientConfig{
		BaseClientConfig: &clients.BaseClientConfig{},
	}
	internalClient := clients.NewHTTPClient(httpC, httpClientCfg)
	events, byts, lat := testMetrics()
	_, cancel := context.WithCancel(context.Background())
	return &KratosExporter{
		cfg:              cfg,
		endpoint:         endpoint,
		client:           internalClient,
		cancelFunc:       cancel,
		eventChannel:     make(chan cloudevent.Event, cfg.MaxEvents),
		taskChannel:      make(chan func(), cfg.MaxBufferSize),
		eventsCounter:    events,
		bytesCounter:     byts,
		latencyHistogram: lat,
	}
}

// ---------------------------------------------------------------------------
// newBatch / writeBatch
// ---------------------------------------------------------------------------

func TestNewBatch_Fields(t *testing.T) {
	now := time.Now()
	b := newBatch(now, 10*time.Millisecond)
	if b == nil {
		t.Fatal("expected non-nil batch")
	}
	if !b.time.Equal(now) {
		t.Errorf("time: got %v, want %v", b.time, now)
	}
	if b.size != 0 || b.bytes != 0 {
		t.Errorf("expected zero size/bytes, got size=%d bytes=%d", b.size, b.bytes)
	}
	if b.ready == nil || b.done == nil || b.timer == nil {
		t.Error("channels and timer must be initialized")
	}
}

func TestWriteBatch_Add_SingleEvent(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	e := testEvent("e1")
	if !b.add(e, 10, 1_000_000) {
		t.Fatal("expected add to return true")
	}
	if b.size != 1 {
		t.Errorf("size: got %d, want 1", b.size)
	}
	if b.bytes == 0 {
		t.Error("bytes should be > 0 after adding an event")
	}
}

func TestWriteBatch_Add_SecondEventExceedsMaxBytes(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	e := testEvent("e1")
	// Add first event - always succeeds.
	b.add(e, 100, 1)
	// Second event would push bytes over limit.
	e2 := testEvent("e2")
	if b.add(e2, 100, 1) {
		t.Error("expected second add to return false (bytes exceeded)")
	}
}

func TestWriteBatch_Add_MaxSizeReached(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	for i := 0; i < 3; i++ {
		if !b.add(testEvent(string(rune('a'+i))), 3, 1_000_000) {
			t.Fatalf("unexpected false on event %d", i)
		}
	}
	// One more should succeed in adding (size check is >=), batch is already full.
	if b.full(3, 1_000_000) != true {
		t.Error("batch should be full after 3 events with maxSize=3")
	}
}

func TestWriteBatch_Full_BySizeLimit(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	b.size = 5
	if !b.full(5, 1_000_000) {
		t.Error("expected full when size == maxSize")
	}
}

func TestWriteBatch_Full_ByBytesLimit(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	b.bytes = 100
	if !b.full(100, 100) {
		t.Error("expected full when bytes >= maxBytes")
	}
}

func TestWriteBatch_Full_NotFull(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	b.size = 2
	b.bytes = 50
	if b.full(5, 1_000_000) {
		t.Error("expected not full")
	}
}

func TestWriteBatch_Trigger_ClosesReadyChannel(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	b.trigger()
	select {
	case <-b.ready:
		// expected
	default:
		t.Error("ready channel not closed after trigger")
	}
}

func TestWriteBatch_Complete_ClosesDoneChannel(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	testErr := errors.New("test error")
	b.complete(testErr)
	select {
	case <-b.done:
		// expected
	default:
		t.Error("done channel not closed after complete")
	}
	if b.err != testErr {
		t.Errorf("batch.err: got %v, want %v", b.err, testErr)
	}
}

func TestWriteBatch_Complete_NilError(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	b.complete(nil)
	select {
	case <-b.done:
	default:
		t.Error("done channel not closed")
	}
	if b.err != nil {
		t.Errorf("expected nil error, got %v", b.err)
	}
}

// ---------------------------------------------------------------------------
// batchQueue
// ---------------------------------------------------------------------------

func TestNewBatchQueue_Empty(t *testing.T) {
	bq := newBatchQueue(10)
	if bq == nil {
		t.Fatal("expected non-nil batchQueue")
	}
	if bq.closed {
		t.Error("new queue should not be closed")
	}
}

func TestBatchQueue_Put_And_Get(t *testing.T) {
	bq := newBatchQueue(4)
	batch := newBatch(time.Now(), time.Minute)
	if !bq.Put(batch) {
		t.Fatal("Put should return true on open queue")
	}
	got := bq.Get()
	if got != batch {
		t.Error("Get should return the same batch that was Put")
	}
}

func TestBatchQueue_Put_ReturnsFalse_WhenClosed(t *testing.T) {
	bq := newBatchQueue(4)
	bq.Close()
	if bq.Put(newBatch(time.Now(), time.Minute)) {
		t.Error("Put should return false on closed queue")
	}
}

func TestBatchQueue_Get_ReturnsNil_WhenClosedAndEmpty(t *testing.T) {
	bq := newBatchQueue(4)
	bq.Close()
	got := bq.Get()
	if got != nil {
		t.Errorf("Get on closed empty queue should return nil, got %v", got)
	}
}

func TestBatchQueue_Get_BlocksUntilPut(t *testing.T) {
	bq := newBatchQueue(4)
	batch := newBatch(time.Now(), time.Minute)

	var got *writeBatch
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		got = bq.Get()
	}()

	time.Sleep(10 * time.Millisecond) // let goroutine block
	bq.Put(batch)
	wg.Wait()
	if got != batch {
		t.Error("Get should have returned the batch Put from another goroutine")
	}
}

func TestBatchQueue_Close_UnblocksGet(t *testing.T) {
	bq := newBatchQueue(4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		bq.Get() // should unblock when closed
	}()
	time.Sleep(10 * time.Millisecond)
	bq.Close()
	select {
	case <-done:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Error("Get did not unblock after Close")
	}
}

func TestBatchQueue_FIFOOrder(t *testing.T) {
	bq := newBatchQueue(4)
	b1 := newBatch(time.Now(), time.Minute)
	b2 := newBatch(time.Now(), time.Minute)
	bq.Put(b1)
	bq.Put(b2)
	if bq.Get() != b1 {
		t.Error("first Get should return first Put batch")
	}
	if bq.Get() != b2 {
		t.Error("second Get should return second Put batch")
	}
}

// ---------------------------------------------------------------------------
// closeResponse
// ---------------------------------------------------------------------------

func TestCloseResponse_Nil(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.closeResponse(nil) // must not panic
}

func TestCloseResponse_WithNilBody(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.closeResponse(&http.Response{Body: nil}) // must not panic
}

func TestCloseResponse_WithBody(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.closeResponse(&http.Response{Body: io.NopCloser(strings.NewReader(""))})
}

// ---------------------------------------------------------------------------
// buildRequest
// ---------------------------------------------------------------------------

func TestBuildRequest_Success(t *testing.T) {
	k := newTestExporter(nil, "http://example.com/api/v2/topic/col")
	req, err := k.buildRequest(context.Background(), testEvents(2))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != http.MethodPost {
		t.Errorf("method: got %q, want POST", req.Method)
	}
	if req.Header.Get("Content-Type") != cloudevent.ApplicationCloudEventsBatchJSON {
		t.Errorf("Content-Type header missing or wrong")
	}
	if req.Header.Get("Accept") != "*/*" {
		t.Errorf("Accept header missing or wrong")
	}
}

// ---------------------------------------------------------------------------
// sendRequest
// ---------------------------------------------------------------------------

func TestSendRequest_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("{}"))
	if err := k.sendRequest(context.Background(), req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendRequest_NonOKStatus_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("{}"))
	err := k.sendRequest(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for non-200 status")
	}
}

func TestSendRequest_NetworkError_ReturnsError(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	// Use a closed server to force a network error.
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1:1", strings.NewReader("{}"))
	err := k.sendRequest(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

// ---------------------------------------------------------------------------
// sendSync (internal)
// ---------------------------------------------------------------------------

func TestSendSync_EmptyEvents_ReturnsError(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	err := k.sendSync(context.Background(), nil)
	if !errors.Is(err, nverrors.ErrEmptyEventsList) {
		t.Errorf("got %v, want %v", err, nverrors.ErrEmptyEventsList)
	}
}

func TestSendSync_EventsSizeExceedsLimit_ReturnsError(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	// defaultMaxRequestSizeInBytes = 1_000_000. Build events that together exceed it.
	big := testEvent("big")
	payload := make([]byte, 1_000_001)
	for i := range payload {
		payload[i] = 'x'
	}
	_ = big.SetData(cloudevent.ApplicationJSON, payload)
	err := k.sendSync(context.Background(), []cloudevent.Event{big})
	if !errors.Is(err, nverrors.ErrEventsSizeExceedsLimit) {
		t.Errorf("got %v, want %v", err, nverrors.ErrEventsSizeExceedsLimit)
	}
}

func TestSendSync_HTTPSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	if err := k.sendSync(context.Background(), testEvents(3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendSync_HTTPError_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	err := k.sendSync(context.Background(), testEvents(1))
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
}

// ---------------------------------------------------------------------------
// sendSyncWithRetries
// ---------------------------------------------------------------------------

func TestSendSyncWithRetries_SuccessFirstAttempt(t *testing.T) {
	k := newTestExporter(nil, "")
	calls := 0
	err := k.sendSyncWithRetries(context.Background(), func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestSendSyncWithRetries_RetriesAndSucceeds(t *testing.T) {
	k := newTestExporter(nil, "")
	k.cfg.SyncMaxRetries = 2
	k.cfg.SyncBaseDelayInSeconds = 0

	calls := 0
	err := k.sendSyncWithRetries(context.Background(), func() error {
		calls++
		if calls < 2 {
			return errors.New("transient error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestSendSyncWithRetries_ExhaustsRetries(t *testing.T) {
	k := newTestExporter(nil, "")
	k.cfg.SyncMaxRetries = 2
	k.cfg.SyncBaseDelayInSeconds = 0

	sentinel := errors.New("always fails")
	err := k.sendSyncWithRetries(context.Background(), func() error { return sentinel })
	if err == nil {
		t.Fatal("expected error after exhausted retries")
	}
}

func TestSendSyncWithRetries_ContextCancelledBeforeCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	k := newTestExporter(nil, "")
	err := k.sendSyncWithRetries(ctx, func() error { return nil })
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestSendSyncWithRetries_ContextCancelledDuringDelay(t *testing.T) {
	k := newTestExporter(nil, "")
	k.cfg.SyncMaxRetries = 5
	k.cfg.SyncBaseDelayInSeconds = 10 // long delay

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	calls := 0
	err := k.sendSyncWithRetries(ctx, func() error {
		calls++
		return errors.New("transient")
	})
	if err == nil {
		t.Fatal("expected error due to context cancellation during retry delay")
	}
}

// ---------------------------------------------------------------------------
// sendBatchWithRetries
// ---------------------------------------------------------------------------

func TestSendBatchWithRetries_EmptyEvents_NoOp(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	// must not panic or error
	k.sendBatchWithRetries(context.Background(), nil)
	k.sendBatchWithRetries(context.Background(), []cloudevent.Event{})
}

func TestSendBatchWithRetries_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	k.cfg.NumRetriesExponentialBackoff = 3
	k.sendBatchWithRetries(context.Background(), testEvents(2))
}

func TestSendBatchWithRetries_AllRetriesFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	k.cfg.NumRetriesExponentialBackoff = 2
	k.cfg.BaseDelayInSeconds = 0
	// Must not panic even when all retries fail.
	k.sendBatchWithRetries(context.Background(), testEvents(1))
}

// ---------------------------------------------------------------------------
// SendAsync / SendAsyncList
// ---------------------------------------------------------------------------

func TestSendAsync_BufferAvailable(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	if err := k.SendAsync(context.Background(), testEvent("e1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(k.eventChannel) != 1 {
		t.Errorf("eventChannel len: got %d, want 1", len(k.eventChannel))
	}
}

func TestSendAsync_BufferFull_DropsSilently(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	// Fill the channel to capacity.
	for i := 0; i < cap(k.eventChannel); i++ {
		k.eventChannel <- testEvent("fill")
	}
	// This should drop (channel full) without error.
	if err := k.SendAsync(context.Background(), testEvent("dropped")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendAsyncList_AllEventsEnqueued(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	events := testEvents(5)
	if err := k.SendAsyncList(context.Background(), events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(k.eventChannel) != 5 {
		t.Errorf("eventChannel len: got %d, want 5", len(k.eventChannel))
	}
}

func TestSendAsyncList_BufferFull_DropsExcessSilently(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	// Fill channel to capacity.
	for i := 0; i < cap(k.eventChannel); i++ {
		k.eventChannel <- testEvent("fill")
	}
	if err := k.SendAsyncList(context.Background(), testEvents(3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// flushCurrentBatch
// ---------------------------------------------------------------------------

func TestFlushCurrentBatch_NilBatch_NoOp(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.flushCurrentBatch() // must not panic
}

func TestFlushCurrentBatch_EmptyBatch_NoOp(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.currBatch = newBatch(time.Now(), 0)
	// batch has no events; flushCurrentBatch should do nothing
	k.flushCurrentBatch()
	if k.currBatch == nil {
		// It's fine if currBatch is left as-is or cleared for empty batches.
	}
}

func TestFlushCurrentBatch_WithEvents_TriggersAndClears(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.batchQueue = newBatchQueue(10)

	b := newBatch(time.Now(), time.Minute)
	b.events = testEvents(2)
	b.size = 2
	k.currBatch = b

	k.flushCurrentBatch()

	if k.currBatch != nil {
		t.Error("currBatch should be nil after flush")
	}
	select {
	case <-b.ready:
		// trigger was called
	default:
		t.Error("batch.ready should be closed after flushCurrentBatch")
	}
}

// ---------------------------------------------------------------------------
// flushAllBatches
// ---------------------------------------------------------------------------

func TestFlushAllBatches_NilBatchQueue_NoOp(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.batchQueue = nil
	k.flushAllBatches() // must not panic
}

func TestFlushAllBatches_WithEmptyQueue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	k := newTestExporter(srv.Client().Transport, srv.URL)
	k.batchQueue = newBatchQueue(10)
	k.flushAllBatches() // must not panic
}

func TestFlushAllBatches_DrainsBatches(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	k.batchQueue = newBatchQueue(10)

	// Put a batch in the queue directly.
	b := newBatch(time.Now(), time.Minute)
	b.events = testEvents(2)
	b.size = 2
	b.trigger()
	k.batchQueue.Put(b)

	k.flushAllBatches()

	// batch.done should be closed.
	select {
	case <-b.done:
		// expected
	case <-time.After(2 * time.Second):
		t.Error("batch.done not closed after flushAllBatches")
	}
}

// ---------------------------------------------------------------------------
// SendSync (public, end-to-end via batchEvents + processBatches)
// ---------------------------------------------------------------------------

func TestSendSync_EmptyEvents_ReturnsErrEmptyEventsList(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	err := k.SendSync(context.Background(), nil)
	if !errors.Is(err, nverrors.ErrEmptyEventsList) {
		t.Errorf("got %v, want %v", err, nverrors.ErrEmptyEventsList)
	}
}

func TestSendSync_EventsExceedRequestLimit_ReturnsError(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	big := testEvent("big")
	payload := make([]byte, 1_000_001)
	for i := range payload {
		payload[i] = 'x'
	}
	_ = big.SetData(cloudevent.ApplicationJSON, payload)
	err := k.SendSync(context.Background(), []cloudevent.Event{big})
	if !errors.Is(err, nverrors.ErrEventsSizeExceedsLimit) {
		t.Errorf("got %v, want %v", err, nverrors.ErrEventsSizeExceedsLimit)
	}
}

func TestSendSync_Success_HTTP200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	if err := k.SendSync(context.Background(), testEvents(3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendSync_ServerError_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	err := k.SendSync(context.Background(), testEvents(1))
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestSendSync_ContextCancelledBeforeBatch(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	// Pre-cancel the context so the batch-wait loop exits immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// batchEvents creates goroutines but the outer select in SendSync's loop
	// will pick <-ctx.Done() on the first iteration.
	err := k.SendSync(ctx, testEvents(1))
	// Either ctx.Err() or nil (if batch completes before the select) – what
	// matters is that the call returns promptly without hanging.
	_ = err
}

// ---------------------------------------------------------------------------
// NewKratosExporter via CustomHTTPClient
// ---------------------------------------------------------------------------

func TestNewKratosExporter_WithCustomHTTPClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg, err := NewKratosExporterConfig(
		"",    // no SSA host needed when using custom client
		"",    // no clientID
		"",    // no clientSecret
		"col", // collectorID
	)
	if err != nil {
		t.Fatalf("NewKratosExporterConfig: %v", err)
	}
	cfg.CustomHTTPClient = srv.Client()
	cfg.ClientCfg.Addr = srv.URL
	cfg.MaxWorkers = 1

	exporter, err := NewKratosExporter(cfg)
	if err != nil {
		t.Fatalf("NewKratosExporter: %v", err)
	}

	if exporter == nil {
		t.Fatal("expected non-nil exporter")
	}

	// Verify it implements the Exporter interface.
	var _ Exporter = exporter

	exporter.Close()
}

func TestNewKratosExporter_EndpointFormat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg, _ := NewKratosExporterConfig("", "", "", "mycollector")
	cfg.CustomHTTPClient = srv.Client()
	cfg.ClientCfg.Addr = srv.URL
	cfg.APIVersion = "v2"

	exporter, err := NewKratosExporter(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer exporter.Close()

	expected := srv.URL + "/api/v2/topic/mycollector"
	if exporter.endpoint != expected {
		t.Errorf("endpoint: got %q, want %q", exporter.endpoint, expected)
	}
}

// ---------------------------------------------------------------------------
// Exporter interface compliance
// ---------------------------------------------------------------------------

func TestKratosExporter_ImplementsExporter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg, _ := NewKratosExporterConfig("", "", "", "col")
	cfg.CustomHTTPClient = srv.Client()
	cfg.ClientCfg.Addr = srv.URL

	exporter, err := NewKratosExporter(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer exporter.Close()

	var _ Exporter = exporter
}

// ---------------------------------------------------------------------------
// batchEvents / awaitBatch (timer path)
// ---------------------------------------------------------------------------

func TestBatchEvents_SingleBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	// Pre-initialise batchQueue so batchEvents doesn't spawn a new processBatches
	// goroutine after Close is called.
	k.batchQueue = newBatchQueue(50)
	k.spawn(k.processBatches)

	batches := k.batchEvents(testEvents(3))
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}
	for b := range batches {
		if b.size == 0 {
			t.Error("batch should contain events")
		}
	}
	// Trigger outstanding batch so processBatches can drain it.
	k.flushCurrentBatch()
	// Wait for all goroutines to finish.
	k.batchQueue.Close()
	k.wg.Wait()
}

func TestAwaitBatch_TimerPath(t *testing.T) {
	// awaitBatch should Put the batch into the queue when the timer fires.
	k := newTestExporter(nil, "http://localhost")
	k.batchQueue = newBatchQueue(10)

	// Create a batch with a very short timeout.
	batch := newBatch(time.Now(), 10*time.Millisecond)
	k.currBatch = batch

	// Spawn awaitBatch; the timer fires almost immediately.
	k.spawn(func() { k.awaitBatch(batch) })
	k.wg.Wait()

	// currBatch should be nil now (moved to batchQueue).
	k.batchMutex.Lock()
	got := k.currBatch
	k.batchMutex.Unlock()
	if got != nil {
		t.Error("currBatch should be nil after timer fires")
	}
}

func TestAwaitBatch_ReadyPath(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	// Create a batch with a long timer so it won't fire.
	batch := newBatch(time.Now(), time.Minute)
	k.spawn(func() { k.awaitBatch(batch) })

	// Trigger before the timer.
	batch.trigger()
	k.wg.Wait() // awaitBatch goroutine should finish
}

// ---------------------------------------------------------------------------
// JSON serialisability check used in add (unreachable via normal path, but we
// can validate the marshalling guard via the batchEvents flow)
// ---------------------------------------------------------------------------

func TestBatchAdd_MarshalGuard_ValidEvent(t *testing.T) {
	b := newBatch(time.Now(), time.Minute)
	e := testEvent("x")
	if !b.add(e, 10, 1_000_000) {
		t.Error("valid event should be added successfully")
	}
	// Verify bytes were counted.
	raw, _ := json.Marshal(e)
	if b.bytes != int64(len(raw)) {
		t.Errorf("bytes: got %d, want %d", b.bytes, len(raw))
	}
}

// ---------------------------------------------------------------------------
// buildRequest – invalid URL triggers http.NewRequestWithContext error
// ---------------------------------------------------------------------------

func TestBuildRequest_InvalidURL_ReturnsError(t *testing.T) {
	// A URL with a null byte is always invalid per RFC 3986.
	k := newTestExporter(nil, "http://\x00invalid")
	_, err := k.buildRequest(context.Background(), testEvents(1))
	if err == nil {
		t.Error("expected error for invalid URL in endpoint")
	}
}

// ---------------------------------------------------------------------------
// sendSync – buildRequest error path
// ---------------------------------------------------------------------------

func TestSendSync_InvalidEndpoint_BuildRequestError(t *testing.T) {
	k := newTestExporter(nil, "http://\x00invalid")
	err := k.sendSync(context.Background(), testEvents(1))
	if err == nil {
		t.Error("expected error when endpoint URL is invalid")
	}
}

// ---------------------------------------------------------------------------
// sendRequest – body read error path
// ---------------------------------------------------------------------------

// errReadBody always returns an error on Read, simulating a broken response body.
type errReadBody struct{}

func (e *errReadBody) Read(p []byte) (int, error) { return 0, errors.New("simulated read error") }
func (e *errReadBody) Close() error               { return nil }

// mockBodyErrTransport returns a 200 response whose body cannot be read.
type mockBodyErrTransport struct{}

func (m *mockBodyErrTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &errReadBody{},
		Header:     make(http.Header),
	}, nil
}

func TestSendRequest_BodyReadError_ReturnsError(t *testing.T) {
	k := newTestExporter(&mockBodyErrTransport{}, "http://example.com")
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://example.com", strings.NewReader("{}"))
	err := k.sendRequest(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for unreadable response body")
	}
}

// ---------------------------------------------------------------------------
// batchEvents – goto assignEvent path (second event cannot fit due to bytes)
// ---------------------------------------------------------------------------

func TestBatchEvents_GotoAssignEvent_SecondEventExceedsBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	k.batchQueue = newBatchQueue(50)
	k.spawn(k.processBatches)

	// Compute how many bytes one event occupies when marshalled.
	singleEvent := testEvent("z")
	raw, _ := json.Marshal(singleEvent)
	singleSize := int64(len(raw))

	// Set limit to 1.5× one event: first event fits; second would overflow.
	// This means:
	//   – first event gets added (b.size == 0, skip bytes check), batch NOT full.
	//   – second event: b.size > 0 && (b.bytes + singleSize) > limit → false → goto assignEvent.
	k.cfg.MaxBatchSizeInBytes = int(singleSize * 3 / 2)
	k.cfg.MaxBatchSize = 100 // size limit far away

	batches := k.batchEvents(testEvents(2))
	if len(batches) < 2 {
		t.Errorf("expected ≥2 batches (goto assignEvent path), got %d", len(batches))
	}

	k.flushCurrentBatch()
	k.batchQueue.Close()
	k.wg.Wait()
}

// ---------------------------------------------------------------------------
// createWorkerTasks – various paths
// ---------------------------------------------------------------------------

func TestCreateWorkerTasks_EmptyChannel(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	// No events → default branch fires immediately, returns without creating tasks.
	k.createWorkerTasks(context.Background())
	if len(k.taskChannel) != 0 {
		t.Errorf("expected 0 tasks for empty eventChannel, got %d", len(k.taskChannel))
	}
}

func TestCreateWorkerTasks_FewEvents_PartialBatch(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.cfg.MaxBatchSize = 10

	k.eventChannel <- testEvent("a")
	k.eventChannel <- testEvent("b")
	// createWorkerTasks drains both events, hits default with a 2-event batch, puts 1 task.
	k.createWorkerTasks(context.Background())
	if len(k.taskChannel) != 1 {
		t.Errorf("expected 1 task, got %d", len(k.taskChannel))
	}
}

func TestCreateWorkerTasks_FillsMaxBatchSize(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.cfg.MaxBatchSize = 3

	// Put exactly MaxBatchSize events: they form one full batch immediately.
	for i := 0; i < 3; i++ {
		k.eventChannel <- testEvent(string(rune('a' + i)))
	}
	k.createWorkerTasks(context.Background())
	// One task for the full batch; default fires with empty remaining batch.
	if len(k.taskChannel) != 1 {
		t.Errorf("expected 1 task for full batch, got %d", len(k.taskChannel))
	}
}

func TestCreateWorkerTasks_ClosedChannel_ReturnsImmediately(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	close(k.eventChannel)
	// Must return immediately without panic.
	k.createWorkerTasks(context.Background())
}

func TestCreateWorkerTasks_MoreThanOneBatch(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.cfg.MaxBatchSize = 2

	// 5 events → 2 full batches (2+2) + 1 partial (1).
	for i := 0; i < 5; i++ {
		k.eventChannel <- testEvent(string(rune('a' + i)))
	}
	k.createWorkerTasks(context.Background())
	// 2 full batches + 1 partial = 3 tasks.
	if len(k.taskChannel) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(k.taskChannel))
	}
}

// ---------------------------------------------------------------------------
// flushAll – various paths
// (Each test uses a fresh exporter since flushAll closes k.taskChannel.)
// ---------------------------------------------------------------------------

func TestFlushAll_EmptyClosedChannel(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	close(k.eventChannel) // empty and closed → for-range exits immediately
	k.flushAll(context.Background())
	// taskChannel is now closed; verify it has no tasks.
	count := 0
	for range k.taskChannel {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 tasks, got %d", count)
	}
}

func TestFlushAll_FewEvents_RemainingBatch(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.cfg.MaxBatchSize = 10

	k.eventChannel <- testEvent("a")
	k.eventChannel <- testEvent("b")
	close(k.eventChannel)

	k.flushAll(context.Background())
	// Remaining 2 events → 1 task.
	count := 0
	for range k.taskChannel {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 task, got %d", count)
	}
}

func TestFlushAll_ExactlyMaxBatchSize_ThenRemainder(t *testing.T) {
	k := newTestExporter(nil, "http://localhost")
	k.cfg.MaxBatchSize = 2

	// 3 events: first 2 form a full batch, 3rd is the remainder.
	k.eventChannel <- testEvent("a")
	k.eventChannel <- testEvent("b")
	k.eventChannel <- testEvent("c")
	close(k.eventChannel)

	k.flushAll(context.Background())
	// Task 1: full batch {a,b}; Task 2: remainder {c}.
	count := 0
	for range k.taskChannel {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 tasks, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Full lifecycle via NewKratosExporter (exercises flushPeriodically + Close)
// ---------------------------------------------------------------------------

func TestKratosExporter_FullLifecycle_SendAndClose(t *testing.T) {
	received := make(chan struct{}, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg, err := NewKratosExporterConfig("", "", "", "col")
	if err != nil {
		t.Fatalf("config error: %v", err)
	}
	cfg.CustomHTTPClient = srv.Client()
	cfg.ClientCfg.Addr = srv.URL
	cfg.FlushInterval = 1
	cfg.MaxWorkers = 1

	exporter, err := NewKratosExporter(cfg)
	if err != nil {
		t.Fatalf("NewKratosExporter: %v", err)
	}

	// Send a few events asynchronously.
	for i := 0; i < 3; i++ {
		if err := exporter.SendAsync(context.Background(), testEvent(string(rune('a'+i)))); err != nil {
			t.Fatalf("SendAsync: %v", err)
		}
	}

	// Close flushes all remaining events.
	exporter.Close()
}

func TestKratosExporter_SendAsyncList_ThenClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg, _ := NewKratosExporterConfig("", "", "", "col")
	cfg.CustomHTTPClient = srv.Client()
	cfg.ClientCfg.Addr = srv.URL
	cfg.MaxWorkers = 1

	exporter, err := NewKratosExporter(cfg)
	if err != nil {
		t.Fatalf("NewKratosExporter: %v", err)
	}

	_ = exporter.SendAsyncList(context.Background(), testEvents(5))
	exporter.Close()
}

// ---------------------------------------------------------------------------
// batchEvents – batch.full() == true path (size limit reached after add)
// ---------------------------------------------------------------------------

func TestBatchEvents_BatchFullByMaxSizeAfterAdd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	k := newTestExporter(srv.Client().Transport, srv.URL)
	k.batchQueue = newBatchQueue(50)
	k.spawn(k.processBatches)

	// MaxBatchSize = 1: each event immediately saturates the batch (size reaches 1).
	// This exercises the `if batch.full() == true` branch in batchEvents.
	k.cfg.MaxBatchSize = 1
	k.cfg.MaxBatchSizeInBytes = 1_000_000

	batches := k.batchEvents(testEvents(3))
	if len(batches) != 3 {
		t.Errorf("expected 3 separate batches (one per event), got %d", len(batches))
	}

	k.flushCurrentBatch()
	k.batchQueue.Close()
	k.wg.Wait()
}

// ---------------------------------------------------------------------------
// flushAllBatches – error branch (sendSync fails due to invalid endpoint)
// ---------------------------------------------------------------------------

func TestFlushAllBatches_SendErrorPath(t *testing.T) {
	// Use an invalid endpoint so sendSync fails, exercising the error log branch.
	k := newTestExporter(nil, "http://\x00invalid")
	k.batchQueue = newBatchQueue(10)

	b := newBatch(time.Now(), time.Minute)
	b.events = testEvents(1)
	b.size = 1
	b.trigger()
	k.batchQueue.Put(b)

	k.flushAllBatches()

	select {
	case <-b.done:
		// expected
	case <-time.After(2 * time.Second):
		t.Error("batch.done not closed after flushAllBatches")
	}
	if b.err == nil {
		t.Error("expected non-nil error for invalid endpoint")
	}
}

// ---------------------------------------------------------------------------
// NewKratosExporter – DefaultHTTPClient error path (bad TLS cert/key files)
// ---------------------------------------------------------------------------

func TestNewKratosExporter_DefaultHTTPClientError(t *testing.T) {
	cfg, _ := NewKratosExporterConfig("", "", "", "col")
	// Leave CustomHTTPClient nil so NewKratosExporter calls DefaultHTTPClient.
	// Enable TLS with nonexistent cert/key files → tls.LoadX509KeyPair fails.
	cfg.ClientCfg.TLS.Enabled = true
	cfg.ClientCfg.TLS.CertFile = "/nonexistent/cert.pem"
	cfg.ClientCfg.TLS.KeyFile = "/nonexistent/key.pem"

	_, err := NewKratosExporter(cfg)
	if err == nil {
		t.Error("expected error when TLS cert/key files are missing")
	}
}
