# The zobik-sidecar image: the Integration Sidecar of every node.
# Build from the repository root: docker build -f images/zobik-sidecar.Dockerfile -t zobik-sidecar:dev .
FROM golang:1.27.3@sha256:f20d83a83e18dbd2a4714b8ffd4b30b287b6243962dd75608e86abcc63ba4efc AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /zobik-sidecar ./cmd/zobik-sidecar

FROM scratch
COPY --from=build /zobik-sidecar /zobik-sidecar
ENTRYPOINT ["/zobik-sidecar"]
