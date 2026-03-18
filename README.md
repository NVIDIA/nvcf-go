# nvcf-go

[![CI](https://github.com/NVIDIA/nvcf-go/actions/workflows/ci.yml/badge.svg)](https://github.com/NVIDIA/nvcf-go/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/NVIDIA/nvcf-go.svg)](https://pkg.go.dev/github.com/NVIDIA/nvcf-go)

Go client library for [NVIDIA Cloud Functions (NVCF)](https://docs.nvidia.com/cloud-functions/).

## Overview

`nvcf-go` provides Go packages for building and integrating with NVIDIA Cloud
Functions. It is used internally across NVCF's Go-based services and is
published here as an open-source library.

## Installation

```bash
go get github.com/NVIDIA/nvcf-go
```

## Requirements

- Go 1.24 or later

## Usage

```go
import "github.com/NVIDIA/nvcf-go/pkg/<package>"
```

See the [pkg/](pkg/) directory for available packages and their documentation.

## Development

### Build

```bash
go build ./...
```

### Test

```bash
go test ./...
```

### Lint

```bash
go vet ./...
golangci-lint run ./...
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

All pull requests must be signed off with the Developer Certificate of Origin
(DCO). See [CONTRIBUTING.md](CONTRIBUTING.md) for instructions.

## License

Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.

Licensed under the [Apache License, Version 2.0](LICENSE).

## Security

Please report security vulnerabilities via [SECURITY.md](SECURITY.md).
