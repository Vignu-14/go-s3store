package storage

import (
	"context"
	"fmt"
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
		ring:  ring,
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

func (c *ClusterBackend) PutChunk(ctx context.Context, chunkID string, r io.Reader) (int64, string, error) {
	// get the node to store chunkID
	nodeID, err := c.ring.GetNode(chunkID)
	if err != nil {
		return 0, "", fmt.Errorf("hash ring error: %w", err)
	}

	// first verify the does node exist?
	backend, exists := c.nodes[nodeID]
	if !exists {
		return 0, "", fmt.Errorf("node %s is not registeres in cluster", nodeID)
	}

	// it writes the chunk into specified disk
	bytesWritten, err := backend.PutChunk(ctx, chunkID, r)
	if err != nil {
		return bytesWritten, "", fmt.Errorf("node %s write failed: %w", nodeID, err)
	}

	// return how much bytes are written and nodeID
	return bytesWritten, nodeID, nil
}


func (c *ClusterBackend) GetChunk(ctx context.Context, chunkID string) (io.ReadCloser, error) {
	// Get the node where chunkID is stored
	nodeId, err := c.ring.GetNode(chunkID)
	if err != nil {
		return nil, fmt.Errorf("hash ring error: %w", err)
	}

	// find the storage backend of this node
	backend, exists := c.nodes[nodeId]
	if !exists {
		return nil, fmt.Errorf("node %s is not found in cluster", nodeId)
	}
	
	// open and return the chunk file stream from the node
	return backend.GetChunk(ctx, chunkID)
}