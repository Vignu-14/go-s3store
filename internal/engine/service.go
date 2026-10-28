package engine

import (
	"go-s3store/internal/metadata"
	"go-s3store/internal/storage"
)

// Default size for a chunk
const DefaultChunkSize = 1 * 1024 * 1024

type StorageEngine struct {
	meta      metadata.Store  // myRef := notebook
	chunks    storage.ChunkBackend // myRef
	chunkSize int64
}

func NewStorageEngine(meta metadata.Store, chunks storage.ChunkBackend, chunkSize int64) *StorageEngine {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &StorageEngine{
		meta: meta,
		chunks: chunks,
		chunkSize: chunkSize,
	}
}

