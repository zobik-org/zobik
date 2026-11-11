# The fake OpenAI endpoint of the end-to-end tests.
# Build from the repository root: docker build -f test/e2e/openai/Dockerfile -t zobik-openai:e2e .
FROM golang:1.27.4@sha256:08bcb99a38f85a265f9a93d1d3323e2440eec333380f3ea26ee0dec9264bb41c AS build
WORKDIR /src
# Only what the endpoint is built from, so an edit elsewhere keeps the cache.
COPY go.mod go.sum ./
COPY test/e2e/openai test/e2e/openai
RUN CGO_ENABLED=0 go build -trimpath -o /openai ./test/e2e/openai

FROM scratch
COPY --from=build /openai /openai
ENTRYPOINT ["/openai"]
