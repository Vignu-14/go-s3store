package metadata

import (
	"errors"
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
