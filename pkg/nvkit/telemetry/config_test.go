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
	"errors"
	"testing"

	"github.com/NVIDIA/nvcf-go/pkg/nvkit/auth"
	nverrors "github.com/NVIDIA/nvcf-go/pkg/nvkit/errors"
	"github.com/spf13/cobra"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// baseConfig returns a minimal KratosExporterConfig with a populated ClientCfg
// suitable for applying options directly without going through a constructor.
func baseConfig() *KratosExporterConfig {
	cfg, _ := NewKratosExporterConfig("host", "id", "secret", "collector")
	return cfg
}

// ---------------------------------------------------------------------------
// NewKratosExporterConfig
// ---------------------------------------------------------------------------

func TestNewKratosExporterConfig_Defaults(t *testing.T) {
	cfg, err := NewKratosExporterConfig("ssa-host", "client-id", "client-secret", "col-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.CollectorID != "col-1" {
		t.Errorf("CollectorID: got %q, want %q", cfg.CollectorID, "col-1")
	}
	if cfg.NumRetries != defaultKratosHTTPRetryAttempts {
		t.Errorf("NumRetries: got %d, want %d", cfg.NumRetries, defaultKratosHTTPRetryAttempts)
	}
	if cfg.FlushInterval != defaultFlushInterval {
		t.Errorf("FlushInterval: got %d, want %d", cfg.FlushInterval, defaultFlushInterval)
	}
	if cfg.MaxBatchSize != defaultMaxBatchSize {
		t.Errorf("MaxBatchSize: got %d, want %d", cfg.MaxBatchSize, defaultMaxBatchSize)
	}
	if cfg.APIVersion != defaultAPIVersion {
		t.Errorf("APIVersion: got %q, want %q", cfg.APIVersion, defaultAPIVersion)
	}
	if cfg.BaseDelayInSeconds != defaultBaseDelayInSeconds {
		t.Errorf("BaseDelayInSeconds: got %d, want %d", cfg.BaseDelayInSeconds, defaultBaseDelayInSeconds)
	}
	if cfg.MaxEvents != defaultMaxEvents {
		t.Errorf("MaxEvents: got %d, want %d", cfg.MaxEvents, defaultMaxEvents)
	}
	if cfg.MaxWorkers != defaultMaxWorkers {
		t.Errorf("MaxWorkers: got %d, want %d", cfg.MaxWorkers, defaultMaxWorkers)
	}
	if cfg.NumRetriesExponentialBackoff != defaultRetriesForExponentialBackoff {
		t.Errorf("NumRetriesExponentialBackoff: got %d, want %d", cfg.NumRetriesExponentialBackoff, defaultRetriesForExponentialBackoff)
	}
	if cfg.MaxBufferSize != defaultMaxBufferSize {
		t.Errorf("MaxBufferSize: got %d, want %d", cfg.MaxBufferSize, defaultMaxBufferSize)
	}
	if cfg.SyncMaxRetries != defaultSyncMaxRetries {
		t.Errorf("SyncMaxRetries: got %d, want %d", cfg.SyncMaxRetries, defaultSyncMaxRetries)
	}
	if cfg.MaxBatchSizeInBytes != defaultMaxBatchSizeInBytes {
		t.Errorf("MaxBatchSizeInBytes: got %d, want %d", cfg.MaxBatchSizeInBytes, defaultMaxBatchSizeInBytes)
	}
	if cfg.SyncBatchTimeoutMs != defaultSyncBatchTimeoutMs {
		t.Errorf("SyncBatchTimeoutMs: got %d, want %d", cfg.SyncBatchTimeoutMs, defaultSyncBatchTimeoutMs)
	}
	if cfg.SyncBatchQueueSize != defaultSyncBatchQueueSize {
		t.Errorf("SyncBatchQueueSize: got %d, want %d", cfg.SyncBatchQueueSize, defaultSyncBatchQueueSize)
	}
	if cfg.ClientCfg == nil {
		t.Fatal("ClientCfg must not be nil")
	}
	if cfg.ClientCfg.Addr != kratosProdEndpoint {
		t.Errorf("ClientCfg.Addr: got %q, want %q", cfg.ClientCfg.Addr, kratosProdEndpoint)
	}
	if cfg.ClientCfg.AuthnCfg.OIDCConfig.Host != "ssa-host" {
		t.Errorf("OIDCConfig.Host: got %q, want ssa-host", cfg.ClientCfg.AuthnCfg.OIDCConfig.Host)
	}
	if cfg.ClientCfg.AuthnCfg.OIDCConfig.ClientID != "client-id" {
		t.Errorf("OIDCConfig.ClientID: got %q, want client-id", cfg.ClientCfg.AuthnCfg.OIDCConfig.ClientID)
	}
	if cfg.ClientCfg.AuthnCfg.OIDCConfig.ClientSecret != "client-secret" {
		t.Errorf("OIDCConfig.ClientSecret: got %q, want client-secret", cfg.ClientCfg.AuthnCfg.OIDCConfig.ClientSecret)
	}
}

func TestNewKratosExporterConfig_WithOption(t *testing.T) {
	cfg, err := NewKratosExporterConfig("host", "id", "secret", "col", WithNumRetries(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.NumRetries != 1 {
		t.Errorf("NumRetries: got %d, want 1", cfg.NumRetries)
	}
}

func TestNewKratosExporterConfig_OptionError_ReturnsError(t *testing.T) {
	_, err := NewKratosExporterConfig("host", "id", "secret", "col", WithNumRetries(-1))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// NewKratosExporterConfigWithCredentialsFile
// ---------------------------------------------------------------------------

func TestNewKratosExporterConfigWithCredentialsFile_Defaults(t *testing.T) {
	cfg, err := NewKratosExporterConfigWithCredentialsFile("ssa-host", "/path/creds.json", "col-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.CollectorID != "col-2" {
		t.Errorf("CollectorID: got %q, want col-2", cfg.CollectorID)
	}
	if cfg.ClientCfg.AuthnCfg.OIDCConfig.Host != "ssa-host" {
		t.Errorf("OIDCConfig.Host: got %q", cfg.ClientCfg.AuthnCfg.OIDCConfig.Host)
	}
	if cfg.ClientCfg.AuthnCfg.OIDCConfig.CredentialsFile != "/path/creds.json" {
		t.Errorf("CredentialsFile: got %q", cfg.ClientCfg.AuthnCfg.OIDCConfig.CredentialsFile)
	}
	if cfg.NumRetries != defaultKratosHTTPRetryAttempts {
		t.Errorf("NumRetries: got %d", cfg.NumRetries)
	}
	if cfg.FlushInterval != defaultFlushInterval {
		t.Errorf("FlushInterval: got %d", cfg.FlushInterval)
	}
}

func TestNewKratosExporterConfigWithCredentialsFile_WithOption(t *testing.T) {
	cfg, err := NewKratosExporterConfigWithCredentialsFile("host", "creds.json", "col", WithFlushInterval(10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FlushInterval != 10 {
		t.Errorf("FlushInterval: got %d, want 10", cfg.FlushInterval)
	}
}

func TestNewKratosExporterConfigWithCredentialsFile_OptionError_ReturnsError(t *testing.T) {
	_, err := NewKratosExporterConfigWithCredentialsFile("host", "creds.json", "col", WithFlushInterval(-1))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// WithTLS
// ---------------------------------------------------------------------------

func TestWithTLS_SetsConfig(t *testing.T) {
	cfg := baseConfig()
	tls := auth.TLSConfigOptions{Enabled: true, CertFile: "cert.pem", KeyFile: "key.pem"}
	if err := WithTLS(tls)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ClientCfg.TLS.Enabled != true {
		t.Errorf("TLS.Enabled: got false, want true")
	}
	if cfg.ClientCfg.TLS.CertFile != "cert.pem" {
		t.Errorf("TLS.CertFile: got %q", cfg.ClientCfg.TLS.CertFile)
	}
	if cfg.ClientCfg.TLS.KeyFile != "key.pem" {
		t.Errorf("TLS.KeyFile: got %q", cfg.ClientCfg.TLS.KeyFile)
	}
}

// ---------------------------------------------------------------------------
// WithRefreshConfig
// ---------------------------------------------------------------------------

func TestWithRefreshConfig_SetsConfig(t *testing.T) {
	cfg := baseConfig()
	rc := auth.RefreshConfig{Interval: 30}
	if err := WithRefreshConfig(rc)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ClientCfg.AuthnCfg.RefreshConfig == nil {
		t.Fatal("RefreshConfig should not be nil after setting")
	}
	if cfg.ClientCfg.AuthnCfg.RefreshConfig.Interval != 30 {
		t.Errorf("RefreshConfig.Interval: got %d, want 30", cfg.ClientCfg.AuthnCfg.RefreshConfig.Interval)
	}
}

// ---------------------------------------------------------------------------
// WithNumRetries
// ---------------------------------------------------------------------------

func TestWithNumRetries_Valid(t *testing.T) {
	cfg := baseConfig()
	if err := WithNumRetries(0)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.NumRetries != 0 {
		t.Errorf("NumRetries: got %d, want 0", cfg.NumRetries)
	}
}

func TestWithNumRetries_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithNumRetries(5)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.NumRetries != 5 {
		t.Errorf("NumRetries: got %d, want 5", cfg.NumRetries)
	}
}

func TestWithNumRetries_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithNumRetries(-1)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

// ---------------------------------------------------------------------------
// WithMaxBatchSize
// ---------------------------------------------------------------------------

func TestWithMaxBatchSize_Zero(t *testing.T) {
	cfg := baseConfig()
	if err := WithMaxBatchSize(0)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxBatchSize != 0 {
		t.Errorf("MaxBatchSize: got %d, want 0", cfg.MaxBatchSize)
	}
}

func TestWithMaxBatchSize_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithMaxBatchSize(500)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxBatchSize != 500 {
		t.Errorf("MaxBatchSize: got %d, want 500", cfg.MaxBatchSize)
	}
}

func TestWithMaxBatchSize_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithMaxBatchSize(-1)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

// ---------------------------------------------------------------------------
// WithFlushInterval
// ---------------------------------------------------------------------------

func TestWithFlushInterval_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithFlushInterval(10)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FlushInterval != 10 {
		t.Errorf("FlushInterval: got %d, want 10", cfg.FlushInterval)
	}
}

func TestWithFlushInterval_Zero_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithFlushInterval(0)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

func TestWithFlushInterval_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithFlushInterval(-5)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

// ---------------------------------------------------------------------------
// WithMaxEvents
// ---------------------------------------------------------------------------

func TestWithMaxEvents_Zero(t *testing.T) {
	cfg := baseConfig()
	if err := WithMaxEvents(0)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxEvents != 0 {
		t.Errorf("MaxEvents: got %d, want 0", cfg.MaxEvents)
	}
}

func TestWithMaxEvents_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithMaxEvents(2000)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxEvents != 2000 {
		t.Errorf("MaxEvents: got %d, want 2000", cfg.MaxEvents)
	}
}

func TestWithMaxEvents_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithMaxEvents(-1)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

// ---------------------------------------------------------------------------
// WithBaseDelayInSeconds
// ---------------------------------------------------------------------------

func TestWithBaseDelayInSeconds_Zero(t *testing.T) {
	cfg := baseConfig()
	if err := WithBaseDelayInSeconds(0)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BaseDelayInSeconds != 0 {
		t.Errorf("BaseDelayInSeconds: got %d, want 0", cfg.BaseDelayInSeconds)
	}
}

func TestWithBaseDelayInSeconds_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithBaseDelayInSeconds(15)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BaseDelayInSeconds != 15 {
		t.Errorf("BaseDelayInSeconds: got %d, want 15", cfg.BaseDelayInSeconds)
	}
}

func TestWithBaseDelayInSeconds_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithBaseDelayInSeconds(-1)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

// ---------------------------------------------------------------------------
// WithMaxWorkers
// ---------------------------------------------------------------------------

func TestWithMaxWorkers_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithMaxWorkers(3)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxWorkers != 3 {
		t.Errorf("MaxWorkers: got %d, want 3", cfg.MaxWorkers)
	}
}

func TestWithMaxWorkers_Zero_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithMaxWorkers(0)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

func TestWithMaxWorkers_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithMaxWorkers(-3)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

// ---------------------------------------------------------------------------
// WithNumRetriesExponentialBackoff
// ---------------------------------------------------------------------------

func TestWithNumRetriesExponentialBackoff_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithNumRetriesExponentialBackoff(7)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.NumRetriesExponentialBackoff != 7 {
		t.Errorf("NumRetriesExponentialBackoff: got %d, want 7", cfg.NumRetriesExponentialBackoff)
	}
}

func TestWithNumRetriesExponentialBackoff_Zero_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithNumRetriesExponentialBackoff(0)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

func TestWithNumRetriesExponentialBackoff_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithNumRetriesExponentialBackoff(-1)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

// ---------------------------------------------------------------------------
// WithMaxBufferSize
// ---------------------------------------------------------------------------

func TestWithMaxBufferSize_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithMaxBufferSize(200)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxBufferSize != 200 {
		t.Errorf("MaxBufferSize: got %d, want 200", cfg.MaxBufferSize)
	}
}

func TestWithMaxBufferSize_Zero_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithMaxBufferSize(0)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

func TestWithMaxBufferSize_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithMaxBufferSize(-10)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

// ---------------------------------------------------------------------------
// WithSyncMaxRetries
// ---------------------------------------------------------------------------

func TestWithSyncMaxRetries_Zero(t *testing.T) {
	cfg := baseConfig()
	if err := WithSyncMaxRetries(0)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SyncMaxRetries != 0 {
		t.Errorf("SyncMaxRetries: got %d, want 0", cfg.SyncMaxRetries)
	}
}

func TestWithSyncMaxRetries_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithSyncMaxRetries(5)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SyncMaxRetries != 5 {
		t.Errorf("SyncMaxRetries: got %d, want 5", cfg.SyncMaxRetries)
	}
}

func TestWithSyncMaxRetries_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithSyncMaxRetries(-1)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

// ---------------------------------------------------------------------------
// WithSyncBaseDelayInSeconds
// ---------------------------------------------------------------------------

func TestWithSyncBaseDelayInSeconds_Zero(t *testing.T) {
	cfg := baseConfig()
	if err := WithSyncBaseDelayInSeconds(0)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SyncBaseDelayInSeconds != 0 {
		t.Errorf("SyncBaseDelayInSeconds: got %d, want 0", cfg.SyncBaseDelayInSeconds)
	}
}

func TestWithSyncBaseDelayInSeconds_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithSyncBaseDelayInSeconds(8)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SyncBaseDelayInSeconds != 8 {
		t.Errorf("SyncBaseDelayInSeconds: got %d, want 8", cfg.SyncBaseDelayInSeconds)
	}
}

func TestWithSyncBaseDelayInSeconds_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithSyncBaseDelayInSeconds(-2)(cfg)
	if !errors.Is(err, nverrors.ErrNegativeValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNegativeValue)
	}
}

// ---------------------------------------------------------------------------
// WithMaxBatchSizeInBytes
// ---------------------------------------------------------------------------

func TestWithMaxBatchSizeInBytes_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithMaxBatchSizeInBytes(512000)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxBatchSizeInBytes != 512000 {
		t.Errorf("MaxBatchSizeInBytes: got %d, want 512000", cfg.MaxBatchSizeInBytes)
	}
}

func TestWithMaxBatchSizeInBytes_Zero_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithMaxBatchSizeInBytes(0)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

func TestWithMaxBatchSizeInBytes_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithMaxBatchSizeInBytes(-1)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

// ---------------------------------------------------------------------------
// WithSyncBatchTimeoutMs
// ---------------------------------------------------------------------------

func TestWithSyncBatchTimeoutMs_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithSyncBatchTimeoutMs(100)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SyncBatchTimeoutMs != 100 {
		t.Errorf("SyncBatchTimeoutMs: got %d, want 100", cfg.SyncBatchTimeoutMs)
	}
}

func TestWithSyncBatchTimeoutMs_Zero_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithSyncBatchTimeoutMs(0)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

func TestWithSyncBatchTimeoutMs_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithSyncBatchTimeoutMs(-50)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

// ---------------------------------------------------------------------------
// WithSyncBatchQueueSize
// ---------------------------------------------------------------------------

func TestWithSyncBatchQueueSize_Positive(t *testing.T) {
	cfg := baseConfig()
	if err := WithSyncBatchQueueSize(25)(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SyncBatchQueueSize != 25 {
		t.Errorf("SyncBatchQueueSize: got %d, want 25", cfg.SyncBatchQueueSize)
	}
}

func TestWithSyncBatchQueueSize_Zero_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithSyncBatchQueueSize(0)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

func TestWithSyncBatchQueueSize_Negative_ReturnsError(t *testing.T) {
	cfg := baseConfig()
	err := WithSyncBatchQueueSize(-1)(cfg)
	if !errors.Is(err, nverrors.ErrNonPositiveValue) {
		t.Errorf("got %v, want %v", err, nverrors.ErrNonPositiveValue)
	}
}

// ---------------------------------------------------------------------------
// WithEndpoint
// ---------------------------------------------------------------------------

func TestWithEndpoint_NonEmpty_SetsEndpoint(t *testing.T) {
	cfg := baseConfig()
	if err := WithEndpoint("https://custom.endpoint.com")(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ClientCfg.Addr != "https://custom.endpoint.com" {
		t.Errorf("Addr: got %q, want https://custom.endpoint.com", cfg.ClientCfg.Addr)
	}
}

func TestWithEndpoint_Empty_SetsDefaultProdEndpoint(t *testing.T) {
	cfg := baseConfig()
	// First set a non-default address.
	cfg.ClientCfg.Addr = "https://overridden.endpoint.com"
	if err := WithEndpoint("")(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ClientCfg.Addr != kratosProdEndpoint {
		t.Errorf("Addr: got %q, want %q", cfg.ClientCfg.Addr, kratosProdEndpoint)
	}
}

// ---------------------------------------------------------------------------
// AddCommandFlags
// ---------------------------------------------------------------------------

func TestAddCommandFlags_NilCmd_ReturnsFalse(t *testing.T) {
	cfg := baseConfig()
	if cfg.AddCommandFlags(nil) {
		t.Error("expected false for nil cmd")
	}
}

func TestAddCommandFlags_NilCfg_ReturnsFalse(t *testing.T) {
	cmd := &cobra.Command{}
	var cfg *KratosExporterConfig
	if cfg.AddCommandFlags(cmd) {
		t.Error("expected false for nil cfg")
	}
}

func TestAddCommandFlags_Valid_ReturnsTrue(t *testing.T) {
	cfg := baseConfig()
	cmd := &cobra.Command{}
	if !cfg.AddCommandFlags(cmd) {
		t.Error("expected true for valid cmd and cfg")
	}
	// Verify the flag was registered.
	if cmd.Flags().Lookup("collector-id") == nil {
		t.Error("expected collector-id flag to be registered")
	}
}
