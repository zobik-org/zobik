# The fake OpenAI endpoint of the end-to-end tests.
# Build from the repository root: docker build -f test/e2e/openai/Dockerfile -t zobik-openai:e2e .
FROM golang:1.27.3@sha256:f24164268fc1e741bbdc0066dbadcf1c8426c5a8489f0729c02c3905a53032fd AS build
WORKDIR /src
# Only what the endpoint is built from, so an edit elsewhere keeps the cache.
COPY go.mod go.sum ./
COPY test/e2e/openai test/e2e/openai
RUN CGO_ENABLED=0 go build -trimpath -o /openai ./test/e2e/openai

FROM scratch
COPY --from=build /openai /openai
ENTRYPOINT ["/openai"]
