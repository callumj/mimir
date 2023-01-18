// SPDX-License-Identifier: AGPL-3.0-only

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
