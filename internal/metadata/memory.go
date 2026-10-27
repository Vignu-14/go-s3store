package metadata

import "errors"

var ErrObjectNotFound = errors.New("object not found")

type ChunkMeta struct {
	Index      int    `json:"idnex"`
	ChunkID    string `json:"chunk_id"`
	Size       int64  `json:"size"`
	SHA256Hash string `json:"sha256_hash"`
}
