// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/memcached_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package chunkscache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/pool"
)

func TestMemcachedIndexCache_FetchMultiPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	range1 := Range{BlockID: block1, Start: chunks.ChunkRef(100), NumChunks: 10}
	range2 := Range{BlockID: block1, Start: chunks.ChunkRef(200), NumChunks: 20}
	range3 := Range{BlockID: block2, Start: chunks.ChunkRef(100), NumChunks: 10}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedChunks
		mockedErr      error
		fetchUserID    string
		fetchRanges    []Range
		expectedHits   map[Range][]byte
		expectedMisses []Range
	}{
		"should return no hits on empty cache": {
			setup:          []mockedChunks{},
			fetchUserID:    user1,
			fetchRanges:    []Range{range1, range2},
			expectedHits:   nil,
			expectedMisses: []Range{range1, range2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedChunks{
				{userID: user1, r: range1, value: value1},
				{userID: user2, r: range2, value: value2},
				{userID: user1, r: range3, value: value3},
			},
			fetchUserID: user1,
			fetchRanges: []Range{range1, range3},
			expectedHits: map[Range][]byte{
				range1: value1,
				range3: value3,
			},
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setup: []mockedChunks{
				{userID: user1, r: range1, value: value1},
				{userID: user1, r: range2, value: value2},
			},
			fetchUserID:    user1,
			fetchRanges:    []Range{range1, range3},
			expectedHits:   map[Range][]byte{range1: value1},
			expectedMisses: []Range{range3},
		},
		"should return no hits on memcached error": {
			setup: []mockedChunks{
				{userID: user1, r: range1, value: value1},
				{userID: user1, r: range1, value: value2},
				{userID: user1, r: range1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchUserID:    user1,
			fetchRanges:    []Range{range1, range2},
			expectedHits:   nil,
			expectedMisses: []Range{range1, range2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewMemcachedChunksCache(log.NewNopLogger(), memcached, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreChunks(ctx, p.userID, p.r, p.value)
			}

			// Fetch postings from cached and assert on it.
			p := pool.NewSafeSlabPool[byte](&pool.TrackedPool{Parent: &sync.Pool{}}, 10000)
			hits, misses := c.FetchMultiChunks(ctx, testData.fetchUserID, testData.fetchRanges, p)
			assert.Equal(t, testData.expectedHits, hits)
			assert.Equal(t, testData.expectedMisses, misses)

			// Assert on metrics.
			assert.Equal(t, float64(len(testData.fetchRanges)), prom_testutil.ToFloat64(c.requests))
			assert.Equal(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits))

		})
	}
}

func BenchmarkStringCacheKeys(b *testing.B) {
	userID := "tenant"
	rng := Range{BlockID: ulid.MustNew(1, nil), Start: chunks.ChunkRef(200), NumChunks: 20}

	b.Run("chunks", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			chunksKey(userID, rng)
		}
	})

}

type mockedChunks struct {
	userID string
	r      Range
	value  []byte
}

type mockedMemcachedClient struct {
	cache             map[string][]byte
	mockedGetMultiErr error
}

func newMockedMemcachedClient(mockedGetMultiErr error) *mockedMemcachedClient {
	return &mockedMemcachedClient{
		cache:             map[string][]byte{},
		mockedGetMultiErr: mockedGetMultiErr,
	}
}

func (c *mockedMemcachedClient) GetMulti(_ context.Context, keys []string, _ ...cache.Option) map[string][]byte {
	if c.mockedGetMultiErr != nil {
		return nil
	}

	hits := map[string][]byte{}

	for _, key := range keys {
		if value, ok := c.cache[key]; ok {
			hits[key] = value
		}
	}

	return hits
}

func (c *mockedMemcachedClient) SetAsync(_ context.Context, key string, value []byte, _ time.Duration) error {
	c.cache[key] = value

	return nil
}

func (c *mockedMemcachedClient) Delete(_ context.Context, key string) error {
	delete(c.cache, key)

	return nil
}

func (c *mockedMemcachedClient) Stop() {
	// Nothing to do.
}
