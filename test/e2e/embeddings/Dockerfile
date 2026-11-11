# The fake embeddings endpoint of the end-to-end tests.
# Build from the repository root: docker build -f test/e2e/embeddings/Dockerfile -t zobik-embeddings:e2e .
FROM golang:1.27.4@sha256:08bcb99a38f85a265f9a93d1d3323e2440eec333380f3ea26ee0dec9264bb41c AS build
WORKDIR /src
# Only what the endpoint is built from, so an edit elsewhere keeps the cache.
COPY go.mod go.sum ./
COPY internal/embedding/embeddingtest internal/embedding/embeddingtest
COPY test/e2e/embeddings test/e2e/embeddings
RUN CGO_ENABLED=0 go build -trimpath -o /embeddings ./test/e2e/embeddings

FROM scratch
COPY --from=build /embeddings /embeddings
ENTRYPOINT ["/embeddings"]
