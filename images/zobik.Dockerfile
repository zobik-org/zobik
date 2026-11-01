# The zobik image: every structural role, and the console's ephemeral acts on the Bus.
# Build from the repository root: docker build -f images/zobik.Dockerfile -t zobik:dev .
# TAGS=dev builds a development binary.
FROM golang:1.27.3@sha256:f24164268fc1e741bbdc0066dbadcf1c8426c5a8489f0729c02c3905a53032fd AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TAGS=
RUN CGO_ENABLED=0 go build -tags "$TAGS" -trimpath -o /zobik ./cmd/zobik

# Trivy, which only the scanner role invokes (implementation §1.2.19): the
# release's binary, pinned by the digest of its official image. Its version is
# scanner.TrivyVersion.
FROM ghcr.io/aquasecurity/trivy:0.76.1@sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c AS trivy

FROM scratch
# The system CAs the https adapter trusts (implementation §1.2.12).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /zobik /zobik
COPY --from=trivy /usr/local/bin/trivy /usr/local/bin/trivy
ENTRYPOINT ["/zobik"]
