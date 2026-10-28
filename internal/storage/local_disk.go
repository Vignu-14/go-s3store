package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ChunkBackend interface is for saving, reading and deleting a single 1MB chunk
// LocalDiskBackend saves chunks in a local folder
// after it I will to minio
type ChunkBackend interface {
	PutChunk(ctx context.Context, chunkID string, r io.Reader) (int64, error)
	GetChunk(ctx context.Context, chunkID string) (io.ReadCloser, error)
	DeleteChunk(ctx context.Context, chunkID string) error
}

// LocalDiskBackend stores chunk files in this folder
type LocalDiskBackend struct {
	baseDir string  //  ex: ./data/chunks
}

// NewLocalDiskBackend creates the "./data/chunks" folder if it doesn't exist yet
func NewLocalDiskBackend(baseDir string) (*LocalDiskBackend, error) {
	//  0755 permission to read and write
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return &LocalDiskBackend{}, fmt.Errorf("failed to create storage folder: %w", err)
	}
	return &LocalDiskBackend{baseDir: baseDir}, nil
}

func (l *LocalDiskBackend) chunkPath(chunkId string) string {
	return filepath.Join(l.baseDir, chunkId)
}

