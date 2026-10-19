# The fake embeddings endpoint of the end-to-end tests.
# Build from the repository root: docker build -f test/e2e/embeddings/Dockerfile -t zobik-embeddings:e2e .
FROM golang:1.27.2@sha256:820153096fda4de6ac1b7194d12e416b7dd8f4e9a33d17e0345b111b945162d8 AS build
WORKDIR /src
# Only what the endpoint is built from, so an edit elsewhere keeps the cache.
COPY go.mod go.sum ./
COPY internal/embedding/embeddingtest internal/embedding/embeddingtest
COPY test/e2e/embeddings test/e2e/embeddings
RUN CGO_ENABLED=0 go build -trimpath -o /embeddings ./test/e2e/embeddings

FROM scratch
COPY --from=build /embeddings /embeddings
ENTRYPOINT ["/embeddings"]
