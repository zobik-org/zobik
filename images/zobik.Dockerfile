# The zobik image: every structural role, and the console's ephemeral acts on the Bus.
# Build from the repository root: docker build -f images/zobik.Dockerfile -t zobik:dev .
# TAGS=dev builds a development binary.
FROM golang:1.27.3@sha256:f20d83a83e18dbd2a4714b8ffd4b30b287b6243962dd75608e86abcc63ba4efc AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TAGS=
RUN CGO_ENABLED=0 go build -tags "$TAGS" -trimpath -o /zobik ./cmd/zobik

FROM scratch
COPY --from=build /zobik /zobik
ENTRYPOINT ["/zobik"]
