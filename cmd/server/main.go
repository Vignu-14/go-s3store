package main

import (
	"encoding/json"
	"fmt"
	"go-s3store/internal/engine"
	"go-s3store/internal/metadata"
	"go-s3store/internal/storage"
	"log"
	"net/http"
)

func main() {

	// refMe = create a notebook
	metaStore := metadata.NewMemoryStore()

	// create the warehouse(folder) to store chunks
	diskBackend, err := storage.NewLocalDiskBackend("./data/chunks")
	if err != nil {
		log.Fatalf("could not create storage folder: %w", err)
	}

	// create the engine with 1MB chunks size
	s3Engine := engine.NewStorageEngine(metaStore, diskBackend, engine.DefaultChunkSize)

	// create the multiplexer
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /{bucket}/{key...}", func(w http.ResponseWriter, r *http.Request) {
		bucket := r.PathValue("bucket")
		key := r.PathValue("key")

		objMeta, err := s3Engine.PutObject(r.Context(), bucket, key, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return 
		}

		// returning s2-style etag header
		w.Header().Set("ETag", fmt.Sprintf("%q", objMeta.ETag))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(objMeta)
	})
	
}



