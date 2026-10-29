package storage

import (
	"context"
	"io"
)

type ClusterBackend interface {
	PutChunk(ctx context.Context, chunkID string, r io.Reader) (int64, error)
	GetChunk(ctx context.Context, chunkID string) (io.ReadCloser, error)
	DeleteChunk(ctx context.Context, chunkID string) error
}
