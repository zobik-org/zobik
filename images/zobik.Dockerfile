# The zobik image: every structural role, and the console's ephemeral acts on the Bus.
# Build from the repository root: docker build -f images/zobik.Dockerfile -t zobik:dev .
# TAGS=dev builds a development binary.
FROM golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TAGS=
RUN CGO_ENABLED=0 go build -tags "$TAGS" -trimpath -o /zobik ./cmd/zobik

FROM scratch
COPY --from=build /zobik /zobik
ENTRYPOINT ["/zobik"]
