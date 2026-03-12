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
	"fmt"
	"io"
	"net/http"

	"github.com/NVIDIA/nvcf-go/pkg/nvkit/auth"
	"github.com/NVIDIA/nvcf-go/pkg/nvkit/clients"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
)

const (
	KratosPipelineSSATokenEndpoint = "https://w6rojyggn16dpp37xuunjjnvczxdhjrkobq393rkkae.ssa.nvidia.com" //nolint:gosec
	KratosAPIEndpoint              = "https://api.kratos.nvidia.com"
	SsaTokenRefreshTime            = 850
)

var AwsRegion = "us-west-2"
var BulkSyncReadScopes = []string{"bulkuploads-read"}

type KratosPipelineClient struct {
	cfg              *clients.BaseClientConfig
	client           *clients.HTTPClient
	kratosTokenCache map[string]KratosStsToken
}

type KratosStsToken struct {
	Status          int    `json:"status"`
	AccessKeyId     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`
}

// Create a Kratos pipeline client
func NewKratosPipelineClient(kratosSsaHost string, ssaClientId string, ssaClientSecret string) *KratosPipelineClient {
	cfg := &clients.BaseClientConfig{
		Type: "http",
		Addr: KratosAPIEndpoint,
		TLS:  auth.TLSConfigOptions{},
		AuthnCfg: &auth.AuthnConfig{
			OIDCConfig: &auth.ProviderConfig{
				Host:         kratosSsaHost,
				ClientID:     ssaClientId,
				ClientSecret: ssaClientSecret,
				Scopes:       BulkSyncReadScopes,
			},
			RefreshConfig: &auth.RefreshConfig{
				Interval: SsaTokenRefreshTime,
			},
		},
	}
	httpClientConfig := &clients.HTTPClientConfig{
		BaseClientConfig: cfg,
		NumRetries:       defaultKratosHTTPRetryAttempts,
	}
	internalHttpClient, err := clients.DefaultHTTPClient(httpClientConfig, func(_ string, r *http.Request) string {
		return "kratos.pipeline"
	})
	if err != nil {
		zap.L().Error("Error creating Kratos pipeline http client", zap.Error(err))
		return nil
	}
	client := &KratosPipelineClient{
		cfg:              cfg,
		client:           internalHttpClient,
		kratosTokenCache: make(map[string]KratosStsToken),
	}
	return client
}

func getTokenCacheKey(tenantId string, namespaceId string) string {
	return fmt.Sprintf("%s:%s", tenantId, namespaceId)
}

// Get the AWS credential from Kratos RESTful API
// See also https://api.kratos.nvidia.com/swagger-ui/index.html#/BulkUpload/getSTSToken
func (p *KratosPipelineClient) getS3Cred(ctx context.Context, tenantID string, namespaceID string) (KratosStsToken, error) {
	cacheKey := getTokenCacheKey(tenantID, namespaceID)
	stsToken, exist := p.kratosTokenCache[cacheKey]
	if exist {
		return stsToken, nil
	}
	s3CredApi := fmt.Sprintf("%s/v1/tenants/%s/teams/%s/sts/bulkuploads", KratosAPIEndpoint, tenantID, namespaceID)
	req, err := http.NewRequest("GET", s3CredApi, nil)
	if err != nil {
		zap.L().Error("Failed to create request for STS credendtials from Kratos bulkuploads API", zap.Error(err))
		return KratosStsToken{}, err
	}
	resp, err := p.client.Client(ctx).Do(req)
	if err != nil {
		zap.L().Error("Failed to fetch STS credendtials from Kratos bulkuploads API", zap.Error(err))
		return KratosStsToken{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		zap.L().Error("Failed to read response from Kratos bulkuploads API", zap.Error(err))
		return KratosStsToken{}, err
	}
	err = json.Unmarshal(body, &stsToken)
	if err != nil {
		zap.L().Error("Failed to unmarshal STS credendtials from Kratos bulkuploads API", zap.Error(err))
	}
	return stsToken, err
}

// Create the s3 session, s3 object with the specified tenant ID and namespace ID.
func (p *KratosPipelineClient) GetS3Client(ctx context.Context, tenantId string, namespaceId string) (*s3.Client, error) {
	token, err := p.getS3Cred(ctx, tenantId, namespaceId)
	if err != nil {
		return nil, err
	}
	appCreds := credentials.NewStaticCredentialsProvider(token.AccessKeyId, token.SecretAccessKey, token.SessionToken)
	s3cfg, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(AwsRegion), awsConfig.WithCredentialsProvider(appCreds))
	if err != nil {
		return nil, err
	}
	s3Client := s3.NewFromConfig(s3cfg)
	return s3Client, nil
}
