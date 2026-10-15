// Command embeddings serves the fake embeddings endpoint of the test harness as
// a container on a network's Docker network, where the provisioning act, the
// Integration Sidecar and zobik publish reach it. The end-to-end test fixes the
// vectors its similarity floors are measured against.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"zobik.org/zobik/internal/embedding/embeddingtest"
)

func main() {
	addr := flag.String("addr", ":8080", "the address to listen on")
	vectors := flag.String("vectors", "{}", "the fixed vectors, as a JSON object from input, prefix included, to vector")
	flag.Parse()

	var fixed map[string][]float32
	if err := json.Unmarshal([]byte(*vectors), &fixed); err != nil {
		log.Fatalf("-vectors: %v", err)
	}
	h := embeddingtest.NewHandler()
	for input, v := range fixed {
		h.Set(input, v)
	}
	log.Fatal(http.ListenAndServe(*addr, h))
}
