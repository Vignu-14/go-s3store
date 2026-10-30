package storage

import (
	"context"
	"go-s3store/internal/cluster"
	"io"
)

type NodeBackend interface {
	PutChunk(ctx context.Context, chunkID string, r io.Reader) (int64, error)
	GetChunk(ctx context.Context, chunkID string) (io.ReadCloser, error)
	DeleteChunk(ctx context.Context, chunkID string) error
}

type ClusterBackend struct {
	// getting ring from the cluster/ring.go
	ring *cluster.HashRing

	// nodes maps the nodeID to acutal disk storage backend
	nodes map[string]NodeBackend
}

func NewClusterBackend(ring *cluster.HashRing) *ClusterBackend {
	return &ClusterBackend{
		ring: ring,
		nodes: make(map[string]NodeBackend),
	}
}

// Register node adds a physical storage node to both the cluster map and the hash ring
func (c *ClusterBackend) RegisterNode(nodeID string, backend NodeBackend) {
	// store the backend worker in our map under its nodeID key
	c.nodes[nodeID] = backend
	// add the node in hash ring with virutal noeds
	c.ring.AddNode(nodeID)
}

