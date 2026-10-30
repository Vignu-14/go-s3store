package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go-s3store/internal/metadata"
	"go-s3store/internal/storage"
	"io"
	"strings"
	"time"
)

// Default size for a chunk
const DefaultChunkSize = 1 * 1024 * 1024

type StorageEngine struct {
	meta      metadata.Store       // myRef := notebook
	chunks    *storage.ClusterBackend // myRef
	chunkSize int64
}

func NewStorageEngine(meta metadata.Store, chunks *storage.ClusterBackend, chunkSize int64) *StorageEngine {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &StorageEngine{
		meta:      meta,
		chunks:    chunks,
		chunkSize: chunkSize,
	}
}

func (e *StorageEngine) PutObject(ctx context.Context, bucket, key string, r io.Reader) (metadata.ObjectMeta, error) {
	// hash the whole object
	objectHasher := sha256.New()

	// io.TeeReader split the reader in two reader, one goes into objectHasher and another one for our work
	teeReader := io.TeeReader(r, objectHasher)

	var (
		// arrary of chunks information
		chunks    []metadata.ChunkMeta
		// total size of the file
		totalSize int64
		// chunk index
		chunkIdx  int
	)

	// loop for making chunks
	for {
		//  hasher for chunks
		chunkHasher := sha256.New()

		// limits the reader to e.chunkSize bytes
		limitReader := io.LimitReader(teeReader, e.chunkSize)

		// io.TeeReader splits the stream into two parts one goes chunkHasher and another goes to our use
		chunkTeeReader := io.TeeReader(limitReader, chunkHasher)

		safeKey := strings.ReplaceAll(key, "/", "_")
		// Creating the file name
		chunkID := fmt.Sprintf("%s_%s_part_%05d_%v", bucket, safeKey, chunkIdx, time.Now().UnixNano())

		bytesWritten, nodeID, err := e.chunks.PutChunk(ctx, chunkID, chunkTeeReader)
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
			Index:      chunkIdx,
			ChunkID:    chunkID,
			NodeID: nodeID,
			Size:       bytesWritten,
			SHA256Hash: hex.EncodeToString(chunkHasher.Sum(nil)),
		})
		// increase chunk index by 1 so in next iteration it can get unique value
		chunkIdx++

		// adding the bytes written into total size so end of iteration we can see total size of the file
		totalSize += bytesWritten

		// adding edge case
		// if bytes written are lesser than chunksize then we break, becuase there is no data left.
		if bytesWritten < e.chunkSize {
			break
		}
	}
	objMeta := metadata.ObjectMeta{
		Bucket:    bucket,
		Key:       key,
		TotalSize: totalSize,
		ETag:      hex.EncodeToString(objectHasher.Sum(nil)),
		Chunks:    chunks,
		CreatedAt: time.Now().UTC(),
	}

	err := e.meta.SaveObject(ctx, objMeta)
	if err != nil {
		return metadata.ObjectMeta{}, err
	}
	return objMeta, nil
}

func (e *StorageEngine) GetObject(ctx context.Context, bucket, key string, dst io.Writer) (metadata.ObjectMeta, error) {
	// get metadata of object realted to bucket and key
	metaObj, err := e.meta.GetObject(ctx, bucket, key)
	if err != nil {
		return metadata.ObjectMeta{}, nil
	}

	for _, c := range metaObj.Chunks{
		r, err := e.chunks.GetChunk(ctx, c.ChunkID)
		if err != nil {
			return metadata.ObjectMeta{}, fmt.Errorf("missing chunk %d (%s), %w", c.Index, c.ChunkID, err)
		}

		// hash the chunk for knowing that is chunk isn't corrupted
		hasher := sha256.New()

		// io.TeeReader for spliting reader
		tee := io.TeeReader(r, hasher)

		// write the data into dst(io.Writer)
		_, err = io.Copy(dst, tee)
		
		// never defer inside the loop
		// close the file before returning
		r.Close() 
		if err != nil {
			return metadata.ObjectMeta{}, fmt.Errorf("failed to streaming chunk %d: %w", c.Index, err)
		}

		// Verify the hash by checking stored hash and currently calculated hash
		currentHash := hex.EncodeToString(hasher.Sum(nil))

		if currentHash != c.SHA256Hash {
			return metadata.ObjectMeta{}, fmt.Errorf("chunk %d is corrupted: expected %s got %s", c.Index, c.SHA256Hash, currentHash)
		}
	}
	return metaObj, nil
}

func (e *StorageEngine) DeleteObject (ctx context.Context, bucket, key string) error {
	metaObj, err := e.meta.GetObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	for _, c := range metaObj.Chunks{
		_ = e.chunks.DeleteChunk(ctx, c.ChunkID)
	}
	return e.meta.DeleteObject(ctx, bucket, key)
}