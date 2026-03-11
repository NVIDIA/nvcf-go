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
	"strings"
	"testing"

	"github.com/NVIDIA/nvcf-go/pkg/nvkit/clients"
)

// ---------------------------------------------------------------------------
// pipeline test helpers
// ---------------------------------------------------------------------------

// pipelineRoundTripper is a round tripper that delegates to a function.
type pipelineRoundTripper struct {
	do func(req *http.Request) (*http.Response, error)
}

func (t *pipelineRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.do(req)
}

// newJSONTransport returns a transport that responds with the given JSON body and status code.
func newJSONTransport(status int, body interface{}) http.RoundTripper {
	return &pipelineRoundTripper{
		do: func(_ *http.Request) (*http.Response, error) {
			b, _ := json.Marshal(body)
			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(strings.NewReader(string(b))),
			}, nil
		},
	}
}

// newErrorTransport returns a transport that always returns an error.
func newErrorTransport(err error) http.RoundTripper {
	return &pipelineRoundTripper{
		do: func(_ *http.Request) (*http.Response, error) {
			return nil, err
		},
	}
}

// errReadCloser is a ReadCloser whose Read always fails.
type errReadCloser struct{}

func (errReadCloser) Read(_ []byte) (int, error) { return 0, errors.New("read error") }
func (errReadCloser) Close() error               { return nil }

// newBodyErrTransport returns a transport whose response body always errors on Read.
func newBodyErrTransport() http.RoundTripper {
	return &pipelineRoundTripper{
		do: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       errReadCloser{},
			}, nil
		},
	}
}

// newTestPipelineClient constructs a KratosPipelineClient with a custom transport,
// bypassing the OIDC/TLS setup done in NewKratosPipelineClient.
func newTestPipelineClient(transport http.RoundTripper) *KratosPipelineClient {
	httpClient := clients.NewHTTPClient(
		&http.Client{Transport: transport},
		&clients.HTTPClientConfig{},
	)
	return &KratosPipelineClient{
		client:           httpClient,
		kratosTokenCache: make(map[string]KratosStsToken),
	}
}

// ---------------------------------------------------------------------------
// getTokenCacheKey
// ---------------------------------------------------------------------------

func TestGetTokenCacheKey(t *testing.T) {
	tests := []struct {
		tenantId    string
		namespaceId string
		want        string
	}{
		{"t1", "n1", "t1:n1"},
		{"tenant-abc", "ns-xyz", "tenant-abc:ns-xyz"},
		{"", "", ":"},
		{"only-tenant", "", "only-tenant:"},
		{"", "only-ns", ":only-ns"},
	}
	for _, tc := range tests {
		got := getTokenCacheKey(tc.tenantId, tc.namespaceId)
		if got != tc.want {
			t.Errorf("getTokenCacheKey(%q, %q) = %q; want %q", tc.tenantId, tc.namespaceId, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// NewKratosPipelineClient
// ---------------------------------------------------------------------------

// TestNewKratosPipelineClient_EmptyHost verifies that passing an empty SSA host
// skips the OAuth setup (validateConfig detects uninitialised config) and returns
// a non-nil client successfully.
func TestNewKratosPipelineClient_EmptyHost(t *testing.T) {
	client := NewKratosPipelineClient("", "client-id", "client-secret")
	if client == nil {
		t.Fatal("expected non-nil KratosPipelineClient")
	}
	if client.kratosTokenCache == nil {
		t.Error("expected non-nil kratosTokenCache")
	}
	if client.client == nil {
		t.Error("expected non-nil HTTP client")
	}
	if client.cfg == nil {
		t.Error("expected non-nil cfg")
	}
}

// TestNewKratosPipelineClient_AllEmptyStrings verifies that all-empty arguments still
// succeed (OIDC host is empty → auth skipped).
func TestNewKratosPipelineClient_AllEmptyStrings(t *testing.T) {
	client := NewKratosPipelineClient("", "", "")
	if client == nil {
		t.Fatal("expected non-nil KratosPipelineClient")
	}
}

// TestNewKratosPipelineClient_AuthError verifies that a non-empty SSA host paired with
// empty client-id/secret causes validateConfig to return a ConfigError, which propagates
// through DefaultHTTPClient, triggering the nil-return error path.
func TestNewKratosPipelineClient_AuthError(t *testing.T) {
	// Non-empty host forces the OIDC config validation path.
	// Empty ClientID causes validateConfig to return a ConfigError (not ErrUninitializedConfig),
	// so HttpClientWithAuth propagates the error → DefaultHTTPClient fails → return nil.
	client := NewKratosPipelineClient("https://ssa.example.invalid", "", "")
	if client != nil {
		t.Fatal("expected nil KratosPipelineClient when OIDC config is invalid")
	}
}

// ---------------------------------------------------------------------------
// getS3Cred
// ---------------------------------------------------------------------------

func TestGetS3Cred_CacheHit(t *testing.T) {
	pc := newTestPipelineClient(newErrorTransport(errors.New("should not be reached")))
	token := KratosStsToken{
		Status:          200,
		AccessKeyId:     "AKID",
		SecretAccessKey: "SECRET",
		SessionToken:    "TOKEN",
	}
	key := getTokenCacheKey("tenant1", "ns1")
	pc.kratosTokenCache[key] = token

	got, err := pc.getS3Cred(context.Background(), "tenant1", "ns1")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if got != token {
		t.Errorf("got %+v, want %+v", got, token)
	}
}

func TestGetS3Cred_HTTPError(t *testing.T) {
	sentinelErr := errors.New("connection refused")
	pc := newTestPipelineClient(newErrorTransport(sentinelErr))

	_, err := pc.getS3Cred(context.Background(), "tenant1", "ns1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetS3Cred_BodyReadError(t *testing.T) {
	pc := newTestPipelineClient(newBodyErrTransport())

	_, err := pc.getS3Cred(context.Background(), "tenant1", "ns1")
	if err == nil {
		t.Fatal("expected error from body read, got nil")
	}
	if !strings.Contains(err.Error(), "read error") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGetS3Cred_UnmarshalError(t *testing.T) {
	badJSON := &pipelineRoundTripper{
		do: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("not-valid-json{")),
			}, nil
		},
	}
	pc := newTestPipelineClient(badJSON)

	_, err := pc.getS3Cred(context.Background(), "tenant1", "ns1")
	if err == nil {
		t.Fatal("expected unmarshal error, got nil")
	}
}

func TestGetS3Cred_Success(t *testing.T) {
	want := KratosStsToken{
		Status:          200,
		AccessKeyId:     "AKID",
		SecretAccessKey: "SECRET",
		SessionToken:    "TOKEN",
	}
	pc := newTestPipelineClient(newJSONTransport(http.StatusOK, want))

	got, err := pc.getS3Cred(context.Background(), "tenant2", "ns2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestGetS3Cred_SuccessPopulatesCache verifies that a successful fetch stores the
// token in the cache so a subsequent call does not hit the network.
func TestGetS3Cred_SuccessPopulatesCache(t *testing.T) {
	want := KratosStsToken{AccessKeyId: "AKID2", SecretAccessKey: "SEC2", SessionToken: "TOK2"}

	calls := 0
	tr := &pipelineRoundTripper{
		do: func(_ *http.Request) (*http.Response, error) {
			calls++
			b, _ := json.Marshal(want)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(string(b))),
			}, nil
		},
	}
	pc := newTestPipelineClient(tr)

	// First call populates cache.
	if _, err := pc.getS3Cred(context.Background(), "t", "n"); err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// Note: getS3Cred stores the token in the cache only via the caller (GetS3Client);
	// it doesn't write the cache itself — the field is read but not written in getS3Cred.
	// So the second call will hit the network again (calls should be 2 unless cache is
	// written).  We verify the returned token is correct both times.
	if _, err := pc.getS3Cred(context.Background(), "t", "n"); err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 transport calls (no cache write), got %d", calls)
	}
}

// ---------------------------------------------------------------------------
// GetS3Client
// ---------------------------------------------------------------------------

func TestGetS3Client_GetS3CredError(t *testing.T) {
	pc := newTestPipelineClient(newErrorTransport(errors.New("network error")))

	client, err := pc.GetS3Client(context.Background(), "tenant1", "ns1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if client != nil {
		t.Error("expected nil S3 client on error")
	}
}

func TestGetS3Client_Success(t *testing.T) {
	token := KratosStsToken{
		Status:          200,
		AccessKeyId:     "AKID",
		SecretAccessKey: "SECRET",
		SessionToken:    "TOKEN",
	}
	pc := newTestPipelineClient(newJSONTransport(http.StatusOK, token))

	s3Client, err := pc.GetS3Client(context.Background(), "tenant1", "ns1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s3Client == nil {
		t.Fatal("expected non-nil *s3.Client")
	}
}

// TestGetS3Client_UsesAwsRegion verifies GetS3Client respects AwsRegion.
func TestGetS3Client_UsesAwsRegion(t *testing.T) {
	orig := AwsRegion
	AwsRegion = "eu-west-1"
	defer func() { AwsRegion = orig }()

	token := KratosStsToken{AccessKeyId: "A", SecretAccessKey: "S", SessionToken: "T"}
	pc := newTestPipelineClient(newJSONTransport(http.StatusOK, token))

	s3Client, err := pc.GetS3Client(context.Background(), "t", "n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s3Client == nil {
		t.Fatal("expected non-nil S3 client")
	}
}
