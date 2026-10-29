package metadata

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrObjectNotFound = errors.New("object not found")

type ChunkMeta struct {
	Index      int    `json:"idnex"`  // serial number
	ChunkID    string `json:"chunk_id"`  // file name of the chunk on the disk device
	NodeID 	   string `json:"node_id"`  //  name of the node
	Size       int64  `json:"size"`  // size of the bytes
	SHA256Hash string `json:"sha256_hash"`  // hash of the chunk 
}

type ObjectMeta struct {
	Bucket    string      `json:"bucket"`  // bucket name
	Key       string      `json:"key"`  // file name
	TotalSize int64       `json:"total_size"`  // total size of the file 
	ETag      string      `json:"etag"`  // fingerprint of the full file 
	Chunks    []ChunkMeta `json:"chunks"` // it will contains the slice of ChunkMeta struct, it will store the chunks of the file
	CreatedAt time.Time   `json:"created_at"`  // time of the uploaded file
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

// this function creates the key for MemoryStore.objects map
func objectMapKey(bucket, key string) string{
	return bucket + "/" + key
}

// here we only store the file obj info, which the store info of the files
func (m *MemoryStore) SaveObject (_ context.Context, obj ObjectMeta) error {
	m.mu.Lock()
	mapKey := objectMapKey(obj.Bucket, obj.Key)   // first create key for the map
	m.objects[mapKey] = obj
	m.mu.Unlock()
	return nil
}


func (m *MemoryStore) GetObject(ctx context.Context, bucket, key string) (ObjectMeta, error) {
	m.mu.RLock()  // lock the map before reading map because another goroutine can't write it 
	defer m.mu.RUnlock()
	mapKey := objectMapKey(bucket, key)  // first create key for the map
	obj, exists := m.objects[mapKey]
	if !exists{
		return ObjectMeta{}, ErrObjectNotFound
	}
	return obj, nil
}

// deleting the objects
func (m *MemoryStore) DeleteObject(ctx context.Context, bucket, key string) error {
	m.mu.Lock() // locking 
	mapKey := objectMapKey(bucket, key)  // first create key for the map
	defer m.mu.Unlock()  // unlocking
	if _, exists := m.objects[mapKey]; !exists {  // before deleting check if key exists or not
		return ErrObjectNotFound
	}
	delete(m.objects, mapKey)
	return nil
}