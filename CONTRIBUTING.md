# Contributing to nvcf-go

Thank you for your interest in contributing to this project! We welcome
contributions from the community. Please read these guidelines before
submitting a pull request.

## Developer Certificate of Origin (DCO)

All contributions must be signed off with the Developer Certificate of Origin.
By adding a `Signed-off-by` line to your commits you certify that you have the
right to submit the contribution under the project license.

```
git commit -s -m "feat: your commit message"
```

This adds a line like:

```
Signed-off-by: Your Name <your.email@example.com>
```

## How to Contribute

1. **Fork** the repository and create your branch from `main`.
2. **Make your changes** and add tests where applicable.
3. **Ensure tests pass**: `go test ./...`
4. **Sign your commits** with `git commit -s`.
5. **Submit a pull request** with a clear description of the change.

## Reporting Issues

Please use [GitHub Issues](https://github.com/NVIDIA/nvcf-go/issues) to
report bugs or request features. Include as much detail as possible.

## Code Style

This project follows standard Go formatting conventions:
- Run `go fmt ./...` before submitting
- Run `go vet ./...` to catch common issues
- Follow [Effective Go](https://go.dev/doc/effective_go) guidelines

## License

By contributing to this project, you agree that your contributions will be
licensed under the [Apache License 2.0](LICENSE).
