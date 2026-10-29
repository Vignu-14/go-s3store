package cluster

import (
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"sync"
)

// new error for no nodes
var ErrNoNodes = errors.New("no storage nodes available in hash ring")

type HashRing struct {
	mu      sync.RWMutex      // locking and unlocking
	vnodes  int               // number of virtual tokens for per physical node
	ring    []uint32          // sorted list of token position on the cricle
	nodeMap map[uint32]string // maps token number on the ring to actual physical node
	nodes   map[string]bool   // set of nodes for stopping duplication
}

func NewHashRing(vnodes int) *HashRing {
	if vnodes <= 0 {
		vnodes = 50
	}
	return &HashRing{
		vnodes:  vnodes,
		nodeMap: make(map[uint32]string),
		nodes:   make(map[string]bool),
	}
}

//  hash function which return uint32 number
func hashToken(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (h *HashRing) AddNode(nodeID string) {
	// first lock the data struture so other go routine can't access it
	h.mu.Lock()
	defer h.mu.Unlock()  // unlock 

	// first check if node already exists in ring
	if h.nodes[nodeID]{
		return 
	}

	// add the node in the set
	h.nodes[nodeID] = true

	// Generate 50 virtual nodes scattered across the ring
	for i := 0; i < h.vnodes; i++ {
		// Generate the virtual node name
		vnodeLabel := fmt.Sprintf("%s#%d", nodeID, i)

		// hash the virtual node name, however the hash is uint32 number
		token := hashToken(vnodeLabel)

		// add the token(virtual node name hash) in the hash ring slice
		h.ring = append(h.ring, token)

		// assign token to nodeID in map
		h.nodeMap[token] = nodeID
	}

	// slice the sort
	slices.Sort(h.ring)
}

