# The zobik-sidecar image: the Integration Sidecar of every node.
# Build from the repository root: docker build -f images/zobik-sidecar.Dockerfile -t zobik-sidecar:dev .
FROM golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /zobik-sidecar ./cmd/zobik-sidecar

FROM scratch
COPY --from=build /zobik-sidecar /zobik-sidecar
ENTRYPOINT ["/zobik-sidecar"]
