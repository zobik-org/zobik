# The zobik-sidecar image: the Integration Sidecar of every node.
# Build from the repository root: docker build -f images/zobik-sidecar.Dockerfile -t zobik-sidecar:dev .
FROM golang:1.27.3@sha256:f24164268fc1e741bbdc0066dbadcf1c8426c5a8489f0729c02c3905a53032fd AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /zobik-sidecar ./cmd/zobik-sidecar

FROM scratch
COPY --from=build /zobik-sidecar /zobik-sidecar
ENTRYPOINT ["/zobik-sidecar"]
