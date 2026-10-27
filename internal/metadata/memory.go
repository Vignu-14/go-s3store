package metadata

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrObjectNotFound = errors.New("object not found")

type ChunkMeta struct {
	Index      int    `json:"idnex"`
	ChunkID    string `json:"chunk_id"`
	Size       int64  `json:"size"`
	SHA256Hash string `json:"sha256_hash"`
}

type ObjectMeta struct {
	Bucket    string      `json:"bucket"`
	Key       string      `json:"key"`
	TotalSize int64       `json:"total_size"`
	ETag      string      `json:"etag"`
	Chunks    []ChunkMeta `json:"chunks"`
	CreatedAt time.Time   `json:"created_at"`
}

type Store interface {
	SaveObject(ctx context.Context, obj ObjectMeta) error
	GetObject(ctx context.Context, bucket, key string) (ObjectMeta, error)
	DeleteObject(ctx context.Context, bucket, key string) error
}

type MemoryStore struct {
	mu sync.RWMutex
	objects map[string]ObjectMeta
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		objects: make(map[string]ObjectMeta),
	}
}

func objectMapKey(bucket, key string) string{
	return bucket + "/" + key
}

