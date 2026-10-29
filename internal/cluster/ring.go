package cluster

import (
	"errors"
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
