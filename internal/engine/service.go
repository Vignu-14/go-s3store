package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go-s3store/internal/metadata"
	"go-s3store/internal/storage"
	"io"
	"time"
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

func (e *StorageEngine) PutObject(ctx context.Context, bucket, key string, r io.Reader) (metadata.ObjectMeta, error) {
	// hash the whole object
	objectHasher := sha256.New()

	// io.TeeReader split the reader in two reader, one goes into objectHasher and another one for our work
	teeReader := io.TeeReader(r, objectHasher)

	var (
		chunks []metadata.ChunkMeta
		totalSize int64
		chunkIdx int
	)

	// loop for making chunks
	for {
		//  hasher for chunks
		chunkHasher := sha256.New()

		// limits the reader to e.chunkSize bytes
		limitReader := io.LimitReader(teeReader, e.chunkSize)

		// io.TeeReader splits the stream into two parts one goes chunkHasher and another goes to our use
		chunkTeeReader := io.TeeReader(limitReader, chunkHasher)

		// Creating the file name
		chunkID := fmt.Sprintf("%s_%s_part_%05d_%s", bucket, key, chunkIdx, time.Now().UnixNano())

		bytesWritten, err := e.chunks.PutChunk(ctx, chunkID, chunkTeeReader)
		if err != nil {
			return metadata.ObjectMeta{}, fmt.Errorf("failed to write chunk: %w", err)
		}

		// deleting the files if 0 bytes are written
		if bytesWritten == 0 {
			_ = e.chunks.DeleteChunk(ctx, chunkID)
			break
		}

		// Adding info to chunks
		chunks = append(chunks, metadata.ChunkMeta{
			Index: chunkIdx,
			ChunkID: chunkID,
			Size: bytesWritten,
			SHA256Hash: hex.EncodeToString(chunkHasher.Sum(nil)),
		})
		// increase chunk index by 1 so in next iteration it can get unique value
		chunkIdx++

		// adding the bytes written into total size so end of iteration we can see total size of the file
		totalSize += bytesWritten

		// adding edge case
		// if bytes written are lesser than chunksize then we break, becuase there is no data left.
		if bytesWritten < e.chunkSize{
			break
		}	
	}
	objMeta := metadata.ObjectMeta{
		Bucket: bucket,
		Key: key,
		TotalSize: totalSize,
		ETag: hex.EncodeToString(objectHasher.Sum(nil)),
		Chunks: chunks,
		CreatedAt: time.Now().UTC(),
	}

	err := e.meta.SaveObject(ctx, objMeta)
	if err != nil {
		return metadata.ObjectMeta{}, err
	}
	return objMeta, nil
}