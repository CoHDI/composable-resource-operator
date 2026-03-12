# Contributing to composable-resource-operator

Thank you for your interest in contributing! This document covers everything you need to know
to get started.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Prerequisites](#prerequisites)
- [Local Development](#local-development)
- [DCO Sign-off](#dco-sign-off)
- [Opening a Pull Request](#opening-a-pull-request)
- [Reporting Bugs](#reporting-bugs)
- [Reporting Security Vulnerabilities](#reporting-security-vulnerabilities)

## Code of Conduct

Please be respectful and constructive in all interactions. We follow the
[Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

## Prerequisites

- Go 1.24+
- Docker 17.03+ (for building the operator image)
- kubectl v1.11.3+
- Access to a Kubernetes cluster for integration testing
- [controller-gen](https://book.kubebuilder.io/reference/controller-gen.html) (installed by `make`)

## Local Development

```sh
# Clone the repository
git clone https://github.com/CoHDI/composable-resource-operator.git
cd composable-resource-operator

# Install tooling and generate manifests
make generate manifests

# Run code formatting
make fmt

# Run static analysis
make vet

# Run the full test suite (uses envtest; no cluster required)
make test

# Build the operator binary
make build
```

The test suite lives under `internal/controller/` and `internal/webhook/`. New features
and bug fixes should include tests.

## DCO Sign-off

All commits must be signed off in accordance with the [Developer Certificate of Origin](DCO).
Add the `-s` flag to your commit command:

```sh
git commit -s -m "feat: add CXL memory support"
```

This appends a `Signed-off-by: Your Name <your@email.com>` trailer to the commit message,
certifying that you have the right to submit the contribution under the project's Apache 2.0
license. Pull requests with unsigned commits will not be merged.

## Opening a Pull Request

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, ensuring `make fmt vet test` passes locally.
3. Sign off every commit (see above).
4. Open a pull request against `main` with a clear description of the change and the
   motivation behind it.
5. At least one maintainer review and approval is required before merging.
6. The CI workflow will run `make fmt vet test` automatically; please address any failures.

## Reporting Bugs

Open a [GitHub Issue](https://github.com/CoHDI/composable-resource-operator/issues) and
include:

- A clear, descriptive title
- Steps to reproduce the problem
- Expected vs. actual behaviour
- Operator version / Git SHA
- Kubernetes version and cluster type
- Relevant log output (`kubectl logs -n <namespace> <operator-pod>`)

## Reporting Security Vulnerabilities

Please do **not** file a public issue for security vulnerabilities. Instead, use
[GitHub Security Advisories](https://github.com/CoHDI/composable-resource-operator/security/advisories/new)
or follow the process described in [SECURITY.md](SECURITY.md).