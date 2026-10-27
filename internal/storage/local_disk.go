package storage

import (
	"context"
	"io"
)

// ChunkBackend interface is for saving, reading and deleting a single 1MB chunk
// LocalDiskBackend saves chunks in a local folder
// after it I will to minio
type ChunkBackend interface {
	PutChunk(ctx context.Context, chunkID string, r io.Reader) (int64, error)
	GetChunk(ctx context.Context, chunkID string) (io.ReadCloser, error)
	DeleteChunk(ctx context.Context, chunkID string) error
}
