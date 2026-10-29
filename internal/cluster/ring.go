package cluster

import "errors"

var ErrNoNodes = errors.New("no storage nodes available in hash ring")

