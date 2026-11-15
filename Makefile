# The checks of this repository. Each CI job runs the target of the same name,
# except lint, which runs the golangci-lint action at GOLANGCI_LINT_VERSION.

# The CI's golangci-lint action pins its own copy of this version.
GOLANGCI_LINT_VERSION   := v2.14.0
GOVULNCHECK_VERSION     := v1.9.0
BUF_VERSION             := v1.57.0
PROTOC_GEN_GO_VERSION   := v1.36.10
PROTOC_GEN_GRPC_VERSION := v1.5.1

# The Python of .venv when `make venv` created it. Only the command line
# overrides it (`make python PYTHON=python`), with a command name or an
# absolute path: the Python targets run it from subdirectories.
VENV   := $(CURDIR)/.venv
PYTHON := $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,python3)

# The files proto regenerates.
GENERATED := find proto sdk/python/src/zobik/nri -type f -not -path '*/__pycache__/*' \( -name '*.pb.go' -o -name '*_pb2*.py*' \) | sort | xargs sha256sum

# proto rewrites sources that go, lint and python read.
.NOTPARALLEL:

.PHONY: check go lint vuln proto proto-tools venv python e2e

## check: every job of the CI except e2e; runs them all and reports each failure
check:
	@$(MAKE) --no-print-directory -k go lint vuln proto python

## go: formatting, vet with and without the dev and e2e tags, build and tests with -race
go:
	@out="$$(gofmt -l .)"; test -z "$$out" || { echo "$$out"; echo "gofmt: the files above are not formatted"; exit 1; }
	go vet ./...
	go vet -tags dev ./...
	go vet -tags e2e ./...
	go build ./...
	go test -race ./...

## lint: golangci-lint with the configuration of .golangci.yml
lint:
	@golangci-lint version 2>/dev/null | grep -Eq "version v?$(subst .,\.,$(GOLANGCI_LINT_VERSION:v%=%)) " || \
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

## proto: buf lint, and fails if the Go code and the Python stubs of the NRI are not the ones the protos generate, leaving them regenerated
proto: proto-tools
	buf lint
	@before="$$($(GENERATED))" && \
		echo "buf generate" && buf generate && \
		echo "sdk/python/generate.sh" && ZOBIK_PYTHON=$(PYTHON) sdk/python/generate.sh && \
		test "$$before" = "$$($(GENERATED))" || \
		{ echo "proto: the generated code was not the one the protos generate; it is regenerated now: review and commit it"; exit 1; }

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
