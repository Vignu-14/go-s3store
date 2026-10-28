package main

import (
	"encoding/json"
	"errors"
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
		log.Fatalf("could not create storage folder: %v", err)
	}

	// create the engine with 1MB chunks size
	s3Engine := engine.NewStorageEngine(metaStore, diskBackend, engine.DefaultChunkSize)

	// create the multiplexer
	mux := http.NewServeMux()

	// Upload a file
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

	// Download a file
	mux.HandleFunc("GET /{bucket}/{key}", func(w http.ResponseWriter, r *http.Request) {
		bucket := r.PathValue("bucket")
		key := r.PathValue("key")

		metaObj, err := metaStore.GetObject(r.Context(), bucket, key)
		if err != nil {
			if errors.Is(err, metadata.ErrObjectNotFound){
				http.Error(w, "NoSuchKey: file not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", metaObj.TotalSize))
		w.Header().Set("Etag", fmt.Sprintf("%s", fmt.Sprintf("%s", metaObj.ETag)))
		_, err = s3Engine.GetObject(r.Context(), bucket, key, w)
		if err != nil {
			log.Printf("error downloading %s/%s: %v", bucket, key, err)
		}
	})

	mux.HandleFunc("DELETE /{bucket}/{key}", func(w http.ResponseWriter, r *http.Request) {
		bucket := r.PathValue("bucket")
		key := r.PathValue("key")

		err := s3Engine.DeleteObject(r.Context(), bucket, key)
		if err != nil {
			if errors.Is(err, metadata.ErrObjectNotFound){
				http.Error(w, "NoSuchKey: file not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	log.Println("Stage 1 S3 store running on http://localhost:8080  (chunk size: 1MB)")
	log.Fatal(http.ListenAndServe(":8080", mux))

}




