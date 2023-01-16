package chunkscache

import (
	"context"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/util/pool"
)

type Range struct {
	BlockID   ulid.ULID
	Start     chunks.ChunkRef
	NumChunks int // We are certain about the number of chunks, not about their total size
}

type ChunksCache interface {
	FetchMultiChunks(ctx context.Context, userID string, bytesPool *pool.SafeSlabPool[byte], ranges []Range) (hits map[Range][]byte, misses []Range)
	StoreChunks(ctx context.Context, userID string, r Range, v []byte)
}

type inMemory struct {
	cached map[string]map[Range][]byte
}

func NewInmemoryChunksCache() ChunksCache {
	return &inMemory{
		cached: map[string]map[Range][]byte{},
	}
}

func (c *inMemory) FetchMultiChunks(ctx context.Context, userID string, bytesPool *pool.SafeSlabPool[byte], ranges []Range) (hits map[Range][]byte, misses []Range) {
	hits = make(map[Range][]byte, len(ranges))
	for i, r := range ranges {
		if cached, ok := c.cached[userID][r]; ok {
			pooled := bytesPool.Get(len(cached))
			copy(pooled, cached)
			hits[r] = pooled
		} else {
			misses = append(misses, ranges[i])
		}
	}
	return
}

func (c *inMemory) StoreChunks(ctx context.Context, userID string, r Range, v []byte) {
	if c.cached[userID] == nil {
		c.cached[userID] = make(map[Range][]byte)
	}
	c.cached[userID][r] = v
}
