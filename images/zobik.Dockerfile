# The zobik image: every structural role, and the console's ephemeral acts on the Bus.
# Build from the repository root: docker build -f images/zobik.Dockerfile -t zobik:dev .
# TAGS=dev builds a development binary.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TAGS=
RUN CGO_ENABLED=0 go build -tags "$TAGS" -trimpath -o /zobik ./cmd/zobik

FROM scratch
COPY --from=build /zobik /zobik
ENTRYPOINT ["/zobik"]
