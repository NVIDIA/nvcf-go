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

package secret

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileFetcherWithInterval(t *testing.T) {
	dir := os.TempDir()
	t.Cleanup(func() { os.Remove(dir) })
	parentCtx := context.Background()

	// set both static and file NGC API key sources
	ctx, cancel := context.WithCancel(parentCtx)
	t.Cleanup(cancel)
	ngcAPIKeyFile := filepath.Join(dir, "api-key")
	require.NoError(t, os.WriteFile(ngcAPIKeyFile, []byte{}, 0755))
	fetcher, err := NewFileFetcher(ctx, ngcAPIKeyFile, WithForceFileRefreshInterval(time.Millisecond), func(ff *FileFetcher) {
		ff.ignoreFileWatcherEvents = true
	})
	assert.NoError(t, err)
	assert.NotNil(t, fetcher)

	// ensure the token fetch works eventually
	apiKeyPrefix := "an-api-key-value-"
	require.NoError(t, os.WriteFile(ngcAPIKeyFile, []byte(apiKeyPrefix+"0"), 0755))
	require.Eventually(t, func() bool {
		token, err := fetcher.FetchData(parentCtx)
		if err != nil {
			t.Logf("failed to fetch the token, error: %v", err)
			return false
		}
		if !strings.HasPrefix(string(token), apiKeyPrefix) {
			t.Logf("token %s does not yet have prefix", token)
			return false
		}
		return true
	}, 1*time.Second, 5*time.Millisecond)

	// Start a job queue to query the fetcher in multiple threads to attempt to get a data race
	workerCtx, workerCtxCancel := context.WithCancel(parentCtx)
	t.Cleanup(workerCtxCancel)
	for i := 0; i < 5; i++ {
		go func() {
			ticker := time.NewTicker(10 * time.Microsecond)
			for {
				select {
				case <-ticker.C:
					token, err := fetcher.FetchData(workerCtx)
					assert.NoError(t, err)
					assert.True(t, strings.HasPrefix(string(token), apiKeyPrefix))
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}

	type testTemplate struct {
		WriteByRename bool
		Title         string
	}

	tests := []testTemplate{
		{
			WriteByRename: false,
			Title:         "write directly to file",
		},
		{
			WriteByRename: true,
			Title:         "write to file by rename",
		},
	}
	for _, test := range tests {
		t.Run(test.Title, func(t *testing.T) {
			// Change the value 5 times with a slight pause in-between
			// in this case we're going to write the file without a rename
			lastWorker := 5
			for i := 1; i <= lastWorker; i++ {
				time.Sleep(5 * time.Millisecond)
				apiKeyVal := fmt.Sprintf("%s%d", apiKeyPrefix, i)
				if test.WriteByRename {
					// Move the file rather than writing this exposes a
					// potential issue with watching an individual file
					// in that the atomic move will break the test
					// See https://github.com/fsnotify/fsnotify?tab=readme-ov-file#watching-a-file-doesnt-work-well
					tmpFile, err := os.CreateTemp("", "test-secret-new-api-key-*")
					require.NoError(t, err)
					t.Cleanup(func() { os.Remove(tmpFile.Name()) })
					require.NoError(t, os.Chmod(tmpFile.Name(), 0755))
					require.NoError(t, os.WriteFile(tmpFile.Name(), []byte(apiKeyVal), 0755))
					require.NoError(t, os.Rename(tmpFile.Name(), ngcAPIKeyFile))
				} else {
					require.NoError(t, os.WriteFile(ngcAPIKeyFile, []byte(apiKeyVal), 0755))
				}
			}
			workerCtxCancel()

			expectedAPIKey := fmt.Sprintf("%s%d", apiKeyPrefix, lastWorker)
			verifyFinalAPIKeyFunc := func() bool {
				apiKey, err := fetcher.FetchData(ctx)
				if err != nil {
					t.Logf("failed to retrieve token, error: %v", err)
					return false
				}
				if expectedAPIKey != string(apiKey) {
					t.Logf("final apikey value does not match, got=%s, want=%s", expectedAPIKey, apiKey)
					return false
				}

				return true
			}
			require.Eventuallyf(t, verifyFinalAPIKeyFunc, 50*time.Millisecond, 5*time.Millisecond, "final apiKey did not match expected value %s", expectedAPIKey)
		})
	}

}

func TestFileFetcherUpdateFile(t *testing.T) {
	ctx := context.Background()
	f := t.TempDir() + "/test.txt"
	var called atomic.Int32
	expectedResults := [][]byte{
		[]byte("test file contents"),
		[]byte("test file modified"),
	}
	updateFunc := func(ctx context.Context, r io.Reader) {
		if called.Load() < int32(len(expectedResults)) {
			b, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, expectedResults[called.Load()], b)
		}
		called.Add(1)
	}

	assert.NoError(t, os.WriteFile(f, expectedResults[0], 0755))
	fetcher, err := NewFileFetcher(ctx, f, WithOnFileUpdateListener(updateFunc), WithForceFileRefreshInterval(5*time.Hour))
	require.NoError(t, err)
	assert.NotNil(t, fetcher)

	// Wait for the file watcher to initialize and process the initial file
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, called.Load(), int32(1), "initial file should be processed at least once")
	}, 5*time.Second, 5*time.Millisecond)

	assert.NoError(t, os.WriteFile(f, expectedResults[1], 0755))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, called.Load(), int32(len(expectedResults)))
	}, 1*time.Second, 5*time.Millisecond)
}

func TestFileFetcherFileDirExistFailures(t *testing.T) {
	tests := []struct {
		description string
		opts        []FileFetcherOption
		makeDir     bool
		errExpected bool
	}{
		{
			description: "File Doesn't Exist, No Error",
			opts:        []FileFetcherOption{WithAllowInitialFailure(true)},
			makeDir:     true,
		},
		{
			description: "File Doesn't Exist, Error",
			makeDir:     true,
			errExpected: true,
		},
		{
			description: "Dir Doesn't Exist, Error",
			errExpected: true,
		},
		{
			description: "Dir Doesn't Exist, No Error",
			opts:        []FileFetcherOption{WithAllowInitialFailure(true), WithCreateDirIfNotExist(true)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			tDir := t.TempDir()
			dirToWatch := tDir + "/test"
			if tt.makeDir {
				os.Mkdir(dirToWatch, 0755)
			}
			_, err := NewFileFetcher(context.Background(), dirToWatch+"/badfile", tt.opts...)
			if tt.errExpected {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestNewFileFetcher_EmptyPath(t *testing.T) {
	_, err := NewFileFetcher(context.Background(), "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "file path is required")
}
