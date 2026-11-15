# The checks of this repository. The CI runs these same targets, so passing
# `make check` locally is passing the CI jobs of the same name.

# The CI's golangci-lint action pins its own copy of this version.
GOLANGCI_LINT_VERSION   := v2.14.0
GOVULNCHECK_VERSION     := v1.9.0
BUF_VERSION             := v1.57.0
PROTOC_GEN_GO_VERSION   := v1.36.10
PROTOC_GEN_GRPC_VERSION := v1.5.1

# The Python of .venv when `make venv` created it.
VENV   := $(CURDIR)/.venv
PYTHON ?= $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,python3)

.PHONY: check go lint vuln proto proto-tools venv python e2e

## check: every job of the CI except e2e
check: go lint vuln proto python

## go: formatting, vet with and without the dev and e2e tags, build and tests with -race
go:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "gofmt: the files above are not formatted"; exit 1; }
	go vet ./...
	go vet -tags dev ./...
	go vet -tags e2e ./...
	go build ./...
	go test -race ./...

## lint: golangci-lint with the configuration of .golangci.yml
lint:
	@golangci-lint version 2>/dev/null | grep -q "version $(GOLANGCI_LINT_VERSION:v%=%) " || \
		echo "warning: the CI runs golangci-lint $(GOLANGCI_LINT_VERSION)"
	golangci-lint run

## vuln: the vulnerabilities the code reaches
vuln:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) -tags dev ./...

## proto-tools: installs buf and the protoc plugins at the pinned versions
proto-tools:
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GRPC_VERSION)

## proto: buf lint, and regenerates the Go code and the Python stubs of the NRI in place
proto:
	buf lint
	buf generate
	PYTHON=$(PYTHON) sdk/python/generate.sh

## venv: a local .venv with the pinned requirements of the Python targets
venv:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --require-hashes -r sdk/python/requirements.txt
	$(VENV)/bin/pip install --require-hashes -r sdk/python/requirements-codegen.txt
	$(VENV)/bin/pip install --require-hashes -r bundles/forge/openai/requirements.txt

## python: the tests of the Python library and of the Forge bundle
python:
	cd sdk/python && PYTHONPATH=src $(PYTHON) -m unittest discover -s tests
	cd bundles/forge/openai && PYTHONPATH=../../../sdk/python/src:. $(PYTHON) -m unittest discover -s tests

## e2e: the end-to-end tests, against Docker
e2e:
	go test -tags e2e -count=1 -timeout 30m ./test/e2e/ ./internal/deploy/ ./internal/role/scanner/
