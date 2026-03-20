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

package clients

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/jellydator/ttlcache/v3"

	"github.com/NVIDIA/nvcf-go/pkg/nvkit/auth"
	"github.com/NVIDIA/nvcf-go/pkg/nvkit/clients/pdp_types"
	"github.com/NVIDIA/nvcf-go/pkg/nvkit/errors"
)

const (
	maxCacheDurationSec = 1 * 24 * 60 * 60 // 1 day
	maxCacheSize        = 1000
)

// --------------------------------------------------------

// UAMAuthorizer defines the contract to be qualified as an UAM authorizer
type UAMAuthorizer interface {
	Evaluate(ctx context.Context, request *pdp_types.RuleRequest) (*pdp_types.RuleResponse, error)
}

// UAMAuthZClientInterface wraps the UAMAuthorizer and provides additional client interface functions
type UAMAuthZClientInterface interface {
	UAMAuthorizer
	PolicyConfig() *auth.UAMPolicyConfig
}

// UAMAuthZClient is a UAM authorization client that evaluates requests against UAM hosted policies
type UAMAuthZClient struct {
	cfg       *BaseClientConfig
	policyCfg *auth.UAMPolicyConfig
	client    UAMAuthorizer
	cache     *ttlcache.Cache[string, pdp_types.RuleResponse]
}

// AuthZClientOption allows for more advanced options during client creation
type AuthZClientOption func(u *UAMAuthZClient) error

// AuthzCacheConfig captures configuration to enable caching during authz checks
//
// NOTE: This is for more advanced use-cases where the policy decisions are not very dynamic
// and not deemed to have critical side-effects.
type AuthzCacheConfig struct {
	CacheDuration string `mapstructure:"duration"`
	CacheSize     int    `mapstructure:"size"`

	// NOTE: This is used mainly for testing.
	// The default behavior of the cache is to not extend the TTL on each hit
	// to make sure that any frequently hit entry doesn't serve stale data,
	// and has opportunity to evaluate and refresh the cached entry.
	extendTTLOnHit bool
}

// WithCachingEnabled - enables caching behavior to improve request latency.
// When caching is enabled, `size` results from evaluate are cached for `duration` time.
// This is useful for clients that have policies in place that are not very reactive
// and the dependencies do not change frequently.
func WithCachingEnabled(cacheCfg AuthzCacheConfig) AuthZClientOption {
	return func(c *UAMAuthZClient) error {
		return c.initializeCache(cacheCfg)
	}
}

func (c *UAMAuthZClient) initializeCache(cacheCfg AuthzCacheConfig) error {
	duration, err := time.ParseDuration(cacheCfg.CacheDuration)
	if err != nil {
		return &errors.ConfigError{
			FieldName: "cache-duration",
			Message:   err.Error(),
		}
	}
	if duration <= 0 || (duration.Seconds() > maxCacheDurationSec) {
		return &errors.ConfigError{
			FieldName: "cache-duration",
			Message:   fmt.Sprintf("invalid - valid-range: 0s-%ds", maxCacheDurationSec),
		}
	} else if cacheCfg.CacheSize <= 0 || cacheCfg.CacheSize > maxCacheSize {
		return &errors.ConfigError{
			FieldName: "cache-size",
			Message:   fmt.Sprintf("invalid - valid-range: 0-%d", maxCacheSize),
		}
	}

	opts := []ttlcache.Option[string, pdp_types.RuleResponse]{
		ttlcache.WithTTL[string, pdp_types.RuleResponse](duration),
		ttlcache.WithCapacity[string, pdp_types.RuleResponse](uint64(cacheCfg.CacheSize)),
	}
	if !cacheCfg.extendTTLOnHit {
		opts = append(opts, ttlcache.WithDisableTouchOnHit[string, pdp_types.RuleResponse]())
	}

	c.cache = ttlcache.New(opts...)

	return nil
}

func (c *UAMAuthZClient) PolicyConfig() *auth.UAMPolicyConfig {
	return c.policyCfg
}

func (c *UAMAuthZClient) Evaluate(ctx context.Context, request *pdp_types.RuleRequest) (*pdp_types.RuleResponse, error) {
	if c.cache == nil {
		return c.client.Evaluate(ctx, request)
	}
	var resp *pdp_types.RuleResponse
	var err error

	reqHash := getRuleRequestHash(request)
	cachedItem := c.cache.Get(reqHash)
	if cachedItem != nil {
		cachedResp := cachedItem.Value()
		resp = &cachedResp
		return resp, nil
	}

	resp, err = c.client.Evaluate(ctx, request)
	if err != nil {
		return nil, err
	}
	c.cache.Set(reqHash, *resp, ttlcache.DefaultTTL)

	return resp, nil
}

func getRuleRequestHash(request *pdp_types.RuleRequest) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(request.String())))
}

func NewUAMAuthzClient(config *BaseClientConfig, policyCfg *auth.UAMPolicyConfig, opts ...AuthZClientOption) (UAMAuthZClientInterface, error) {
	if config == nil {
		return nil, errors.ErrBadConfig
	}

	switch config.Type {
	case string(ClientTypeGRPC):
		zap.L().Error("gRPC client requested")
		return nil, errors.ErrBadConfig
	case string(ClientTypeHTTP):
		return newHTTPAuthzClient(config, policyCfg, opts...)
	}
	return nil, errors.ErrInvalidConfig
}

func newHTTPAuthzClient(config *BaseClientConfig, policyCfg *auth.UAMPolicyConfig, opts ...AuthZClientOption) (UAMAuthZClientInterface, error) {
	httpClient, err := DefaultHTTPClient(&HTTPClientConfig{config, 0}, func(_ string, r *http.Request) string {
		return "http.authz.uam"
	})
	if err != nil {
		return nil, err
	}

	authzClient := &UAMAuthZClient{
		cfg:       config,
		policyCfg: policyCfg,
		client: &authzHTTPClient{
			client: httpClient,
		},
	}

	for _, opt := range opts {
		if err = opt(authzClient); err != nil {
			return nil, err
		}
	}

	return authzClient, err
}

type authzHTTPClient struct {
	client *HTTPClient
}

func (a *authzHTTPClient) Evaluate(ctx context.Context, request *pdp_types.RuleRequest) (*pdp_types.RuleResponse, error) {
	reqBytes, err := json.Marshal(request)
	if err != nil {
		zap.L().Error("Input read error", zap.Error(err))
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.client.Config.Addr+GetUAMEvaluationURL(request.Namespace, request.RuleName), bytes.NewReader(reqBytes))
	if err != nil {
		zap.L().Error("Request create error", zap.Error(err))
		return nil, err
	}
	ctx, span := otel.GetTracerProvider().Tracer("uam").Start(ctx, "evaluate")
	defer span.End()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")

	resp, err := a.client.Client(ctx).Do(req)
	if err != nil {
		zap.L().Error("Evaluate client error", zap.Error(err))
		return nil, err
	}
	defer resp.Body.Close()
	var respBytes []byte
	if respBytes, err = io.ReadAll(resp.Body); err != nil {
		zap.L().Error("Response read error", zap.Error(err), zap.Int("Status code", resp.StatusCode))
		return nil, fmt.Errorf("cannot read response body: %+v", err)
	}
	if resp.StatusCode != http.StatusOK {
		zap.L().Error("Invalid response status", zap.Int("Status code", resp.StatusCode))
		zap.L().Debug("Response", zap.Any("Headers", resp.Header), zap.ByteString("Body", respBytes))
		return nil, fmt.Errorf("invalid response status: %d", resp.StatusCode)
	}

	ruleResponse := &pdp_types.RuleResponse{}
	if err = json.Unmarshal(respBytes, ruleResponse); err != nil {
		zap.L().Error("Response unmarshal error", zap.Error(err))
		return nil, fmt.Errorf("cannot unmarshal response: %+v", err)
	}
	return ruleResponse, nil
}
