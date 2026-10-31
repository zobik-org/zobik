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

FROM scratch
# The system CAs the https adapter trusts (implementation §1.2.12).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /zobik /zobik
ENTRYPOINT ["/zobik"]
