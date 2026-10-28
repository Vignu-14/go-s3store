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

func (l *LocalDiskBackend) PutChunk(_ context.Context, chunkID string, r io.Reader) (int64, error) {
	fullPath := l.chunkPath(chunkID)  // returns the full address for specific chunk
	file, err := os.Create(fullPath)  // Creates a blank file
	if err != nil {
		return  0, fmt.Errorf("failed to create file: %w", err)
	}
	//  io.Copy function copies the file from src to dest, which returns how much bytes are written in dest and error
	bytesWritten, err := io.Copy(file, r)
	if err != nil {
		return 0, fmt.Errorf("failed to write inside the file: %w", err)
	}
	return bytesWritten, nil
}

