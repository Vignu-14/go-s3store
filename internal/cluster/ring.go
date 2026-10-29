package cluster

import (
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"sort"
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

// hash function which return uint32 number
func hashToken(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (h *HashRing) AddNode(nodeID string) {
	// first lock the data struture so other go routine can't access it
	h.mu.Lock()
	defer h.mu.Unlock() // unlock

	// first check if node already exists in ring
	if h.nodes[nodeID] {
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

// Remove the node
func (h *HashRing) RemoveNode(nodeID string) {
	// lock and unlock the data structure
	h.mu.Lock()
	defer h.mu.Unlock()

	// first check the node before removing the node
	if !h.nodes[nodeID] {
		return
	}

	// remove the node from the set
	delete(h.nodes, nodeID)

	// make the new ring size
	newRing := make([]uint32, 0, len(h.ring)-(h.vnodes))
	for _, token := range h.ring {

		// check if the  token related to nodeID exist in the hashmap of nodemap if yes remove the token key
		if h.nodeMap[token] == nodeID {
			delete(h.nodeMap, token)
		} else {
			// if the token does not related to nodeID then add it to new ring
			newRing = append(newRing, token)
		}
	}
	h.ring = newRing
}

// GetNode function decides the in which node the chunk file should go into
func (h *HashRing) GetNode(chunkID string) (string, error) {
	// locking and unlocking
	h.mu.RLock()
	defer h.mu.RUnlock()

	// before getting node for chunk first check if nodes exist?
	if len(h.ring) == 0 {
		return "", ErrNoNodes
	}

	// get the hash of chunkID file for to point on circle
	token := hashToken(chunkID)

	// it return the index of nearest node address clockwise
	// when the it can't find the index it will return the len of h.ring(len(h.ring))
	// ex : uint32 size is 4billion so when last node is at 3 billion but our chunkID hash is 3.5 billion so our function can't find serverID >= chunkHash so it returns the len of h.ring
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= token
	})

	// so in above case we make the index to 0
	if idx == len(h.ring) {
		idx = 0
	}

	// now we take the  node in ring for our chunkID
	chosenToken := h.ring[idx]

	// it will return the actual node address by virtual node address using hashmap search
	return h.nodeMap[chosenToken], nil
}


