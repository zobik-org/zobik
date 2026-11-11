# The zobik-sidecar image: the Integration Sidecar of every node.
# Build from the repository root: docker build -f images/zobik-sidecar.Dockerfile -t zobik-sidecar:dev .
FROM golang:1.27.4@sha256:08bcb99a38f85a265f9a93d1d3323e2440eec333380f3ea26ee0dec9264bb41c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /zobik-sidecar ./cmd/zobik-sidecar

FROM scratch
# The system CAs the https adapter trusts (implementation §1.2.12).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /zobik-sidecar /zobik-sidecar
ENTRYPOINT ["/zobik-sidecar"]
