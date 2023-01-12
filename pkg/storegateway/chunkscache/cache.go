package chunkscache

import (
	"context"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type Range struct {
	BlockID   ulid.ULID
	Start     chunks.ChunkRef
	NumChunks int // We are certain about the number of chunks, not about their total size
}

// TODO dimitarvdimitrov add inmemory and memcached implementation
type ChunksCache interface {
	FetchMultiChunks(ctx context.Context, userID string, ranges ...Range) (hits map[Range][]byte, misses []Range)
	StoreChunks(ctx context.Context, userID string, r Range, v []byte)
}
