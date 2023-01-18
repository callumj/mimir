// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storegateway/chunkscache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/test"
)

func init() {
	// Track the balance of gets/puts in pools in all tests.
	seriesEntrySlicePool = &pool.TrackedPool{Parent: seriesEntrySlicePool}
	seriesChunksSlicePool = &pool.TrackedPool{Parent: seriesChunksSlicePool}
	chunkBytesSlicePool = &pool.TrackedPool{Parent: chunkBytesSlicePool}
}

func TestSeriesChunksSet(t *testing.T) {
	t.Run("newSeriesChunksSet() should guarantee requested capacity", func(t *testing.T) {
		const (
			numRuns     = 1000
			minCapacity = 10
			maxCapacity = 1000
		)

		for r := 0; r < numRuns; r++ {
			capacity := minCapacity + rand.Intn(maxCapacity-minCapacity)
			set := newSeriesChunksSet(capacity, true)
			require.GreaterOrEqual(t, cap(set.series), capacity)

			set.release()
		}
	})

	t.Run("release() should reset the series and chunk entries before putting them back to the pool", func(t *testing.T) {
		const (
			numRuns            = 1000
			numSeries          = 10
			numChunksPerSeries = 10
		)

		// Reset the memory pool tracker.
		seriesEntrySlicePool.(*pool.TrackedPool).Reset()

		for r := 0; r < numRuns; r++ {
			set := newSeriesChunksSet(numSeries, true)

			// Ensure the series slice is made of all zero values. Then write something inside before releasing it again.
			// The slice is expected to be picked from the pool, at least in some runs (there's an assertion on it at
			// the end of the test).
			set.series = set.series[:numSeries]
			for i := 0; i < numSeries; i++ {
				require.Zero(t, set.series[i])

				set.series[i].lset = labels.FromStrings(labels.MetricName, "metric")
				set.series[i].refs = []chunks.ChunkRef{1, 2, 3}
				set.series[i].chks = set.newSeriesAggrChunkSlice(numChunksPerSeries)
			}

			// Do the same with the groups.
			for i := 0; i < numSeries; i++ {
				set.series[i].chks = set.newSeriesAggrChunkSlice(numChunksPerSeries)
				for j := 0; j < numChunksPerSeries; j++ {
					require.Equal(t, storepb.AggrChunk{}, set.series[i].chks[j])

					set.series[i].chks[j].MinTime = 10
					set.series[i].chks[j].MaxTime = 10
					set.series[i].chks[j].Raw = &storepb.Chunk{Data: []byte{1, 2, 3}}
				}
			}

			set.release()
		}

		// Ensure at least 1 series slice has been pulled from the pool.
		assert.Greater(t, seriesEntrySlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))
		assert.Zero(t, seriesEntrySlicePool.(*pool.TrackedPool).Balance.Load())
	})

	t.Run("newSeriesAggrChunkSlice() should allocate slices from the pool and release() should put it back if the set is releasable", func(t *testing.T) {
		seriesChunksSlicePool.(*pool.TrackedPool).Reset()

		set := newSeriesChunksSet(1, true)

		slice := set.newSeriesAggrChunkSlice(seriesChunksSlabSize - 1)
		assert.Equal(t, seriesChunksSlabSize-1, len(slice))
		assert.Equal(t, seriesChunksSlabSize-1, cap(slice))
		assert.Equal(t, 1, int(seriesChunksSlicePool.(*pool.TrackedPool).Gets.Load()))

		slice = set.newSeriesAggrChunkSlice(seriesChunksSlabSize)
		assert.Equal(t, seriesChunksSlabSize, len(slice))
		assert.Equal(t, seriesChunksSlabSize, cap(slice))
		assert.Equal(t, 2, int(seriesChunksSlicePool.(*pool.TrackedPool).Gets.Load()))

		set.release()
		assert.Equal(t, 0, int(seriesChunksSlicePool.(*pool.TrackedPool).Balance.Load()))
	})

	t.Run("newSeriesAggrChunkSlice() should directly allocate a new slice and release() should not put back to the pool if the set is not releasable", func(t *testing.T) {
		seriesChunksSlicePool.(*pool.TrackedPool).Reset()

		set := newSeriesChunksSet(1, false)

		slice := set.newSeriesAggrChunkSlice(seriesChunksSlabSize)
		assert.Equal(t, seriesChunksSlabSize, len(slice))
		assert.Equal(t, seriesChunksSlabSize, cap(slice))
		assert.Equal(t, 0, int(seriesChunksSlicePool.(*pool.TrackedPool).Gets.Load()))
		assert.Equal(t, 0, int(seriesChunksSlicePool.(*pool.TrackedPool).Balance.Load()))

		set.release()

		assert.Equal(t, 0, int(seriesChunksSlicePool.(*pool.TrackedPool).Gets.Load()))
		assert.Equal(t, 0, int(seriesChunksSlicePool.(*pool.TrackedPool).Balance.Load()))
	})
}

func TestSeriesChunksSeriesSet(t *testing.T) {
	c := generateAggrChunk(6)

	series1 := labels.FromStrings(labels.MetricName, "metric_1")
	series2 := labels.FromStrings(labels.MetricName, "metric_2")
	series3 := labels.FromStrings(labels.MetricName, "metric_3")
	series4 := labels.FromStrings(labels.MetricName, "metric_4")
	series5 := labels.FromStrings(labels.MetricName, "metric_4")

	// Utility function to create sets, so that each test starts from a clean setup (e.g. releaser is not released).
	createSets := func() (sets []seriesChunksSet, releasers []*releaserMock) {
		for i := 0; i < 3; i++ {
			releasers = append(releasers, newReleaserMock())
		}

		set1 := newSeriesChunksSet(2, true)
		set1.chunksReleaser = releasers[0]
		set1.series = append(set1.series,
			seriesEntry{lset: series1, chks: []storepb.AggrChunk{c[1]}},
			seriesEntry{lset: series2, chks: []storepb.AggrChunk{c[2]}},
		)

		set2 := newSeriesChunksSet(2, true)
		set2.chunksReleaser = releasers[1]
		set2.series = append(set2.series,
			seriesEntry{lset: series3, chks: []storepb.AggrChunk{c[3]}},
			seriesEntry{lset: series4, chks: []storepb.AggrChunk{c[4]}},
		)

		set3 := newSeriesChunksSet(1, true)
		set3.chunksReleaser = releasers[2]
		set3.series = append(set3.series,
			seriesEntry{lset: series5, chks: []storepb.AggrChunk{c[5]}},
		)

		sets = append(sets, set1, set2, set3)
		return
	}

	t.Run("should iterate over a single set and release it once done", func(t *testing.T) {
		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIterator(sets[0])
		it := newSeriesChunksSeriesSet(source)

		lbls, chks := it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series1, lbls)
		require.Equal(t, []storepb.AggrChunk{c[1]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series2, lbls)
		require.Equal(t, []storepb.AggrChunk{c[2]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())

		require.False(t, it.Next())
		lbls, chks = it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
	})

	t.Run("should iterate over a multiple sets and release each set once we begin to iterate the next one", func(t *testing.T) {
		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIterator(sets[0], sets[1])
		it := newSeriesChunksSeriesSet(source)

		lbls, chks := it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())

		// Set 1.
		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series1, lbls)
		require.Equal(t, []storepb.AggrChunk{c[1]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())
		require.False(t, releasers[1].isReleased())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series2, lbls)
		require.Equal(t, []storepb.AggrChunk{c[2]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())
		require.False(t, releasers[1].isReleased())

		// Set 2.
		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series3, lbls)
		require.Equal(t, []storepb.AggrChunk{c[3]}, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
		require.False(t, releasers[1].isReleased())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series4, lbls)
		require.Equal(t, []storepb.AggrChunk{c[4]}, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
		require.False(t, releasers[1].isReleased())

		require.False(t, it.Next())
		lbls, chks = it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
	})

	t.Run("should release the current set on error", func(t *testing.T) {
		expectedErr := errors.New("mocked error")

		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIteratorWithError(expectedErr, 1, sets[0], sets[1], sets[2])
		it := newSeriesChunksSeriesSet(source)

		lbls, chks := it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series1, lbls)
		require.Equal(t, []storepb.AggrChunk{c[1]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series2, lbls)
		require.Equal(t, []storepb.AggrChunk{c[2]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())

		require.False(t, it.Next())
		lbls, chks = it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.Equal(t, expectedErr, it.Err())

		// The current set is released.
		require.True(t, releasers[0].isReleased())

		// Can't release the next ones because can't move forward with the iteration (due to the error).
		require.False(t, releasers[1].isReleased())
		require.False(t, releasers[2].isReleased())
	})
}

func TestPreloadingSetIterator(t *testing.T) {
	test.VerifyNoLeak(t)

	const delay = 10 * time.Millisecond

	// Create some sets, each set containing 1 series.
	sets := make([]seriesChunksSet, 0, 10)
	for i := 0; i < 10; i++ {
		// The set is not releseable because will be reused by multiple tests.
		set := newSeriesChunksSet(1, false)
		set.series = append(set.series, seriesEntry{
			lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
			refs: []chunks.ChunkRef{chunks.ChunkRef(i)},
		})

		sets = append(sets, set)
	}

	t.Run("should iterate all sets if no error occurs", func(t *testing.T) {
		for preloadSize := 1; preloadSize <= len(sets)+1; preloadSize++ {
			preloadSize := preloadSize

			t.Run(fmt.Sprintf("preload size: %d", preloadSize), func(t *testing.T) {
				t.Parallel()

				source := newSliceSeriesChunksSetIterator(sets...)
				source = newDelayedSeriesChunksSetIterator(delay, source)

				preloading := newPreloadingSetIterator[seriesChunksSet](context.Background(), preloadSize, source)

				// Ensure expected sets are returned in order.
				expectedIdx := 0
				for preloading.Next() {
					require.NoError(t, preloading.Err())
					require.Equal(t, sets[expectedIdx], preloading.At())
					expectedIdx++
				}

				// Ensure all sets have been returned.
				require.NoError(t, preloading.Err())
				require.Equal(t, len(sets), expectedIdx)
			})
		}
	})

	t.Run("should stop iterating once an error is found", func(t *testing.T) {
		for preloadSize := 1; preloadSize <= len(sets)+1; preloadSize++ {
			preloadSize := preloadSize

			t.Run(fmt.Sprintf("preload size: %d", preloadSize), func(t *testing.T) {
				t.Parallel()

				source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(sets), sets...)
				source = newDelayedSeriesChunksSetIterator(delay, source)

				preloading := newPreloadingSetIterator[seriesChunksSet](context.Background(), preloadSize, source)

				// Ensure expected sets are returned in order.
				expectedIdx := 0
				for preloading.Next() {
					require.NoError(t, preloading.Err())
					require.Equal(t, sets[expectedIdx], preloading.At())
					expectedIdx++
				}

				// Ensure an error is returned at the end.
				require.Error(t, preloading.Err())
				require.Equal(t, len(sets), expectedIdx)
			})
		}
	})

	t.Run("should not leak preloading goroutine if caller doesn't iterated until the end of sets but context is canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancelCtx := context.WithCancel(context.Background())

		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(sets), sets...)
		source = newDelayedSeriesChunksSetIterator(delay, source)

		preloading := newPreloadingSetIterator[seriesChunksSet](ctx, 1, source)

		// Just call Next() once.
		require.True(t, preloading.Next())
		require.NoError(t, preloading.Err())
		require.Equal(t, sets[0], preloading.At())

		// Cancel the context.
		cancelCtx()

		// Give a short time to the preloader goroutine to react to the context cancellation.
		// This is required to avoid a flaky test.
		time.Sleep(100 * time.Millisecond)

		// At this point we expect Next() to return false.
		require.False(t, preloading.Next())
		require.NoError(t, preloading.Err())
	})

	t.Run("should not leak preloading goroutine if caller doesn't call Next() until false but context is canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancelCtx := context.WithCancel(context.Background())

		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(sets), sets...)
		source = newDelayedSeriesChunksSetIterator(delay, source)

		preloading := newPreloadingSetIterator[seriesChunksSet](ctx, 1, source)

		// Just call Next() once.
		require.True(t, preloading.Next())
		require.NoError(t, preloading.Err())
		require.Equal(t, sets[0], preloading.At())

		// Cancel the context. Do NOT call Next() after canceling the context.
		cancelCtx()
	})
}

func TestPreloadingSetIterator_Concurrency(t *testing.T) {
	const (
		numRuns     = 100
		numBatches  = 100
		preloadSize = 10
	)

	// Create some sets.
	sets := make([]seriesChunksSet, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		// This set is unreleseable because reused by multiple test runs.
		set := newSeriesChunksSet(1, false)
		set.series = append(set.series, seriesEntry{
			lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
		})

		sets = append(sets, set)
	}

	// Run many times to increase the likelihood to find a race (if any).
	for i := 0; i < numRuns; i++ {
		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(sets), sets...)
		preloading := newPreloadingSetIterator[seriesChunksSet](context.Background(), preloadSize, source)

		for preloading.Next() {
			require.NoError(t, preloading.Err())
			require.NotZero(t, preloading.At())
		}
		require.Error(t, preloading.Err())
	}

}

type testBlock struct {
	ulid   ulid.ULID
	series []seriesEntry
}

func (b testBlock) toSeriesChunkRefsWithGroups(seriesIndex, numGroups int) seriesChunkRefs {
	series := b.series[seriesIndex]

	groups := make([]seriesChunkRefsGroup, numGroups)
	chunksPerGroup := len(series.refs) / numGroups
	for i := range groups {
		groups[i].blockID = b.ulid
		for j := 0; j < chunksPerGroup; j++ {
			groups[i].chunks = append(groups[i].chunks, seriesChunkRef{
				ref:     series.refs[i*chunksPerGroup+j],
				minTime: series.chks[i*chunksPerGroup+j].MinTime,
				maxTime: series.chks[i*chunksPerGroup+j].MaxTime,
			})
		}
	}

	// Account for integer division (e.g. 10 chunks in 3 groups)
	for i := chunksPerGroup * len(groups); i < len(series.refs); i++ {
		groups[len(groups)-1].chunks = append(groups[len(groups)-1].chunks, seriesChunkRef{
			ref:     series.refs[i],
			minTime: series.chks[i].MinTime,
			maxTime: series.chks[i].MaxTime,
		})
	}
	return seriesChunkRefs{
		lset:   series.lset,
		groups: groups,
	}
}

func (b testBlock) toSeriesChunkRefs(seriesIndex int) seriesChunkRefs {
	return b.toSeriesChunkRefsWithGroups(seriesIndex, 1)
}

func TestLoadingSeriesChunksSetIterator(t *testing.T) {
	block1 := testBlock{
		ulid:   ulid.MustNew(1, nil),
		series: generateSeriesEntries(t, 10),
	}

	block2 := testBlock{
		ulid:   ulid.MustNew(2, nil),
		series: generateSeriesEntries(t, 10),
	}

	filterAndCopyChunks := func(s seriesEntry, minT, maxT int64) seriesEntry {
		filtered := make([]storepb.AggrChunk, len(s.chks))
		copy(filtered, s.chks)
		s.chks = removeNonRequestedChunks(filtered, minT, maxT)
		return s
	}

	type loadRequest struct {
		existingBlocks      []testBlock
		setsToLoad          []seriesChunkRefsSet
		expectedSets        []seriesChunksSet
		minT, maxT          int64 // optional; if empty, select a wide time range
		addLoadErr, loadErr error
		expectedErr         string
	}

	testCases := map[string][]loadRequest{
		"loads single set from single block": {
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[1]}},
				},
			},
		},
		"loads single set from single block with multiple groups": {
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 10), block1.toSeriesChunkRefsWithGroups(1, 10)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[1]}},
				},
			},
		},
		"loads single set from single block with multiple groups with mint/maxt": {
			{
				existingBlocks: []testBlock{block1},
				minT:           0,
				maxT:           50,
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 10), block1.toSeriesChunkRefsWithGroups(1, 10)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{filterAndCopyChunks(block1.series[0], 0, 50), filterAndCopyChunks(block1.series[1], 0, 50)}},
				},
			},
		},
		"loads multiple sets from single block": {
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(2), block1.toSeriesChunkRefs(3)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[1]}},
					{series: []seriesEntry{block1.series[2], block1.series[3]}},
				},
			},
		},
		"loads single set from multiple blocks": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block2.series[1]}},
				},
			},
		},
		"loads multiple sets from multiple blocks": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[1]}},
					{series: []seriesEntry{block2.series[0], block2.series[1]}},
				},
			},
		},
		"loads multiple sets from multiple blocks with multiple groups": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 4), block1.toSeriesChunkRefsWithGroups(1, 4)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefsWithGroups(0, 4), block2.toSeriesChunkRefsWithGroups(1, 4)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[1]}},
					{series: []seriesEntry{block2.series[0], block2.series[1]}},
				},
			},
		},
		"loads sets from multiple blocks mixed": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(0)}},
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(1), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block2.series[0]}},
					{series: []seriesEntry{block1.series[1], block2.series[1]}},
				},
			},
		},
		"loads series with groups from different blocks": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: func() []seriesChunkRefs {
						series := block1.toSeriesChunkRefs(0)
						series.groups = append(series.groups, block2.toSeriesChunkRefs(0).groups...)
						return []seriesChunkRefs{series}
					}()},
				},
				expectedSets: []seriesChunksSet{
					{series: func() []seriesEntry {
						entry := block1.series[0]
						entry.chks = append(entry.chks, block2.series[0].chks...)
						return []seriesEntry{entry}
					}()},
				},
			},
		},
		"loads series with groups from different blocks and multiple groups": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: func() []seriesChunkRefs {
						series := block1.toSeriesChunkRefsWithGroups(0, 3)
						series.groups = append(series.groups, block2.toSeriesChunkRefsWithGroups(0, 4).groups...)
						return []seriesChunkRefs{series}
					}()},
				},
				expectedSets: []seriesChunksSet{
					{series: func() []seriesEntry {
						entry := block1.series[0]
						entry.chks = append(entry.chks, block2.series[0].chks...)
						return []seriesEntry{entry}
					}()},
				},
			},
		},
		"loads series with groups from different blocks and multiple groups with minT/maxT": {
			{
				existingBlocks: []testBlock{block1, block2},
				minT:           0,
				maxT:           10,
				setsToLoad: []seriesChunkRefsSet{
					{series: func() []seriesChunkRefs {
						series := block1.toSeriesChunkRefsWithGroups(0, 3)
						series.groups = append(series.groups, block2.toSeriesChunkRefsWithGroups(0, 4).groups...)
						return []seriesChunkRefs{series}
					}()},
				},
				expectedSets: []seriesChunksSet{
					{series: func() []seriesEntry {
						entry := block1.series[0]
						entry.chks = append(entry.chks, block2.series[0].chks...)
						return []seriesEntry{filterAndCopyChunks(entry, 0, 10)}
					}()},
				},
			},
		},
		"handles error in addLoad": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{},
				addLoadErr:   errors.New("test err"),
				expectedErr:  "test err",
			},
		},
		"handles error in load": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{},
				loadErr:      errors.New("test err"),
				expectedErr:  "test err",
			},
		},
		"only cache misses": {
			// Load series 0 and 2
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(2)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(2)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[2]}},
					{series: []seriesEntry{block2.series[0], block2.series[2]}},
				},
			},
			// Next load a different set of series (1 and 3)
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(1), block1.toSeriesChunkRefs(3)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefs(1), block2.toSeriesChunkRefs(3)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[1], block1.series[3]}},
					{series: []seriesEntry{block2.series[1], block2.series[3]}},
				},
			},
		},
		"skips cache chunks when there is a different number of chunks in the group": {
			// Issue a request where the first series has its chunks only in two groups
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 2), block1.toSeriesChunkRefs(2)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[2]}},
				},
			},
			// Issue a request where the first series has its chunks in 12 groups; it shouldn't interfere with the cache item from last request
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 12), block1.toSeriesChunkRefs(2)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[2]}},
				},
			},
		},
		"cache hits": {
			// Issue a request
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 1), block1.toSeriesChunkRefs(2)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[2]}},
				},
			},
			// Issue the same request; this time with an empty storage
			{
				existingBlocks: []testBlock{},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 1), block1.toSeriesChunkRefs(2)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[2]}},
				},
			},
		},
		"some cache hits, some misses": {
			// First query only from block1
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 1), block1.toSeriesChunkRefs(2)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[2]}},
				},
			},
			// Next query from block1 and block2 with only block2 available in the storage; chunks for block1 should be already cached
			{
				existingBlocks: []testBlock{block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithGroups(0, 1), block1.toSeriesChunkRefs(2)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefsWithGroups(0, 1), block2.toSeriesChunkRefs(2)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesEntry{block1.series[0], block1.series[2]}},
					{series: []seriesEntry{block2.series[0], block2.series[2]}},
				},
			},
		},
	}

	for testName, testScenario := range testCases {
		t.Run(testName, func(t *testing.T) {
			// Reuse the cache between requests, so we can test caching too
			cache := newInmemoryChunksCache()

			for _, testCase := range testScenario {
				// Reset the memory pool tracker.
				seriesEntrySlicePool.(*pool.TrackedPool).Reset()
				seriesChunksSlicePool.(*pool.TrackedPool).Reset()
				chunkBytesSlicePool.(*pool.TrackedPool).Reset()

				// Setup
				readersMap := make(map[ulid.ULID]chunkGroupReader, len(testCase.existingBlocks))
				for _, block := range testCase.existingBlocks {
					readersMap[block.ulid] = newChunkReaderMockWithSeries(block.series, testCase.setsToLoad, testCase.addLoadErr, testCase.loadErr)
				}
				readers := newChunkGroupReaders(readersMap)
				metrics := NewBucketStoreMetrics(nil)

				minT, maxT := testCase.minT, testCase.maxT
				if minT == 0 && maxT == 0 {
					minT, maxT = 0, 100000 // select everything by default
				}

				// Run test
				set := newLoadingSeriesChunksSetIterator(context.Background(), "tenant", *readers, newSliceSeriesChunkRefsSetIterator(nil, testCase.setsToLoad...), 100, newSafeQueryStats(), cache, minT, maxT, metrics.chunksRefetches)
				loadedSets := readAllSeriesChunksSets(set)

				// Assertions
				if testCase.expectedErr != "" {
					assert.ErrorContains(t, set.Err(), testCase.expectedErr)
					assert.Equal(t, chunkBytesSlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))
				} else {
					assert.NoError(t, set.Err())
					assert.Greater(t, chunkBytesSlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))
				}
				// Check that groups bytes are what we expect
				require.Len(t, loadedSets, len(testCase.expectedSets))
				for i, loadedSet := range loadedSets {
					require.Len(t, loadedSet.series, len(testCase.expectedSets[i].series))
					for j, loadedSeries := range loadedSet.series {
						assert.ElementsMatch(t, testCase.expectedSets[i].series[j].chks, loadedSeries.chks)
						assert.Truef(t, labels.Equal(testCase.expectedSets[i].series[j].lset, loadedSeries.lset),
							"%d, %d: labels don't match, expected %s, got %s", i, j, testCase.expectedSets[i].series[j].lset, loadedSeries.lset,
						)
					}
				}

				// Release the sets and expect that they also return their chunk bytes to the pool
				for _, s := range loadedSets {
					s.release()
				}

				if testCase.expectedErr != "" {
					assert.Zero(t, chunkBytesSlicePool.(*pool.TrackedPool).Gets.Load())
				} else {
					assert.Greater(t, chunkBytesSlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))
				}
				assert.Zero(t, chunkBytesSlicePool.(*pool.TrackedPool).Balance.Load())

				assert.Zero(t, seriesEntrySlicePool.(*pool.TrackedPool).Balance.Load())
				assert.Greater(t, seriesEntrySlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))

				assert.Zero(t, seriesChunksSlicePool.(*pool.TrackedPool).Balance.Load())
				assert.Greater(t, seriesChunksSlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))
			}
		})
	}
}

func BenchmarkLoadingSeriesChunksSetIterator(b *testing.B) {
	for batchSize := 10; batchSize <= 10000; batchSize *= 10 {
		b.Run(fmt.Sprintf("batch size: %d", batchSize), func(b *testing.B) {
			var (
				numSets            = 100
				numSeriesPerSet    = batchSize
				numChunksPerSeries = 10
			)

			// For simplicity all series have the same groups. This doesn't affect the benchmark anyway.
			blockID := ulid.MustNew(1, nil)
			seriesEntries := make([]seriesEntry, 0, numChunksPerSeries)
			for chunkIdx := 0; chunkIdx < numChunksPerSeries; chunkIdx++ {
				seriesEntries = append(seriesEntries, seriesEntry{
					refs: []chunks.ChunkRef{chunks.ChunkRef(chunkIdx)},
					chks: []storepb.AggrChunk{{MinTime: int64(chunkIdx), MaxTime: int64(chunkIdx), Raw: &storepb.Chunk{}}},
				})
			}

			// Creates the series sets, guaranteeing series sorted by labels.
			sets := make([]seriesChunkRefsSet, 0, numSets)
			for setIdx := 0; setIdx < numSets; setIdx++ {
				minSeriesID := (numSets * numSeriesPerSet) + (setIdx * numSeriesPerSet)
				maxSeriesID := minSeriesID + numSeriesPerSet - 1

				// This set cannot be released because reused between multiple benchmark runs.
				set := createSeriesChunkRefsSet(minSeriesID, maxSeriesID, false)

				for seriesIdx := 0; seriesIdx < set.len(); seriesIdx++ {
					set.series[seriesIdx].groups = generateSeriesChunkRef(blockID, numChunksPerSeries)
				}

				sets = append(sets, set)
			}

			// Mock the chunk reader.
			readersMap := map[ulid.ULID]chunkGroupReader{
				blockID: newChunkReaderMockWithSeries(seriesEntries, sets, nil, nil),
			}

			chunkReaders := newChunkGroupReaders(readersMap)
			stats := newSafeQueryStats()
			metrics := NewBucketStoreMetrics(nil)

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				batchSize := numSeriesPerSet
				it := newLoadingSeriesChunksSetIterator(context.Background(), "tenant", *chunkReaders, newSliceSeriesChunkRefsSetIterator(nil, sets...), batchSize, stats, newInmemoryChunksCache(), 0, 10000, metrics.chunksRefetches)

				actualSeries := 0
				actualChunks := 0

				for it.Next() {
					set := it.At()

					for _, series := range set.series {
						actualSeries++
						actualChunks += len(series.chks)
					}

					set.release()
				}

				if err := it.Err(); err != nil {
					b.Fatal(it.Err())
				}

				// Ensure each benchmark run go through the same data set.
				if expectedSeries := numSets * numSeriesPerSet; actualSeries != expectedSeries {
					b.Fatalf("benchmark iterated through an unexpected number of series (expected: %d got: %d)", expectedSeries, actualSeries)
				}
				if expectedChunks := numSets * numSeriesPerSet * numChunksPerSeries; actualChunks != expectedChunks {
					b.Fatalf("benchmark iterated through an unexpected number of groups (expected: %d got: %d)", expectedChunks, actualChunks)
				}
			}
		})
	}
}

func TestRemoveNonRequestedChunks(t *testing.T) {
	testCases := map[string]struct {
		input      []storepb.AggrChunk
		minT, maxT int64

		expected []storepb.AggrChunk
	}{
		"returns all chunks when they all fit in minT/maxT": {
			input: []storepb.AggrChunk{
				{MinTime: 0, MaxTime: 10},
				{MinTime: 11, MaxTime: 20},
				{MinTime: 21, MaxTime: 30},
			},
			minT: 0, maxT: 100,
			expected: []storepb.AggrChunk{
				{MinTime: 0, MaxTime: 10},
				{MinTime: 11, MaxTime: 20},
				{MinTime: 21, MaxTime: 30},
			},
		},
		"returns no chunks when none fit": {
			input: []storepb.AggrChunk{
				{MinTime: 0, MaxTime: 10},
				{MinTime: 11, MaxTime: 20},
				{MinTime: 21, MaxTime: 30},
			},
			minT: 1000, maxT: 100000,
			expected: []storepb.AggrChunk{},
		},
		"returns some chunks when some fit on the edge": {
			input: []storepb.AggrChunk{
				{MinTime: 0, MaxTime: 10},
				{MinTime: 11, MaxTime: 20},
				{MinTime: 21, MaxTime: 30},
			},
			minT: 11, maxT: 20,
			expected: []storepb.AggrChunk{
				{MinTime: 11, MaxTime: 20},
			},
		},
		"returns some chunks when some fit in the middle": {
			input: []storepb.AggrChunk{
				{MinTime: 0, MaxTime: 10},
				{MinTime: 11, MaxTime: 20},
				{MinTime: 21, MaxTime: 30},
			},
			minT: 15, maxT: 20,
			expected: []storepb.AggrChunk{
				{MinTime: 11, MaxTime: 20},
			},
		},
		"returns some chunks when some partially overlap at the end": {
			input: []storepb.AggrChunk{
				{MinTime: 0, MaxTime: 10},
				{MinTime: 11, MaxTime: 20},
				{MinTime: 21, MaxTime: 30},
			},
			minT: 27, maxT: 40,
			expected: []storepb.AggrChunk{
				{MinTime: 21, MaxTime: 30},
			},
		},
		"returns some chunks when some partially overlap in the beginning": {
			input: []storepb.AggrChunk{
				{MinTime: 11, MaxTime: 20},
				{MinTime: 21, MaxTime: 30},
			},
			minT: 0, maxT: 12,
			expected: []storepb.AggrChunk{
				{MinTime: 11, MaxTime: 20},
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			actual := removeNonRequestedChunks(testCase.input, testCase.minT, testCase.maxT)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

type chunkReaderMock struct {
	chunks              map[chunks.ChunkRef][]storepb.AggrChunk
	addLoadErr, loadErr error

	toLoad map[chunks.ChunkRef]groupLoadIdx
}

// newChunkReaderMockWithSeries returns a chunksLoader that uses the groups in sets to
func newChunkReaderMockWithSeries(existingChunks []seriesEntry, seriesSetWithGroups []seriesChunkRefsSet, addLoadErr, loadErr error) *chunkReaderMock {
	storage := map[chunks.ChunkRef][]storepb.AggrChunk{}

	// That many for loops will be slow if we have a lot of series/chunks,
	// but it should keep test cases easier to write because they don't have to do the grouping there
	for _, set := range seriesSetWithGroups {
		for _, seriesWithGroups := range set.series {
			for _, chksGroup := range seriesWithGroups.groups {
				for _, chunkFromGroup := range chksGroup.chunks {
					for _, series := range existingChunks {
						for i, ungroupedRef := range series.refs {
							if ungroupedRef == chunkFromGroup.ref {
								storage[chksGroup.firstRef()] = append(storage[chksGroup.firstRef()], series.chks[i])
							}
						}
					}
				}
			}
		}
	}

	return &chunkReaderMock{
		chunks:     storage,
		addLoadErr: addLoadErr,
		loadErr:    loadErr,
		toLoad:     make(map[chunks.ChunkRef]groupLoadIdx),
	}
}

func (f *chunkReaderMock) addLoadGroup(g seriesChunkRefsGroup, seriesEntry int, groupEntry int) error {
	if f.addLoadErr != nil {
		return f.addLoadErr
	}
	f.toLoad[g.firstRef()] = groupLoadIdx{
		seriesEntry: seriesEntry,
		groupEntry:  groupEntry,
	}
	return nil
}

func (f *chunkReaderMock) loadGroups(partialSeries []partialSeriesChunksSet, chunksPool *pool.SafeSlabPool[byte], _ *safeQueryStats) error {
	if f.loadErr != nil {
		return f.loadErr
	}
	for firstRef, idx := range f.toLoad {
		encodedGroup := encodeGroup(f.chunks[firstRef])
		// Take bytes from the pool, so we can assert on number of allocations and that frees are happening
		copiedChunkData := chunksPool.Get(len(encodedGroup))
		copy(copiedChunkData, encodedGroup)
		partialSeries[idx.seriesEntry].rawGroups[idx.groupEntry] = copiedChunkData
	}
	return nil
}

// encodeGroup encodes each chunk in this format [len, encoding, data, crc32] and appends them to the out slice
// The crc32 is not valid.
func encodeGroup(aggrChunks []storepb.AggrChunk) (out []byte) {
	for _, chk := range aggrChunks {
		const uvarintMaxLen = 10

		out = append(out, make([]byte, uvarintMaxLen)...)
		lenUvarintBytes := binary.PutUvarint(out[len(out)-uvarintMaxLen:], uint64(len(chk.Raw.Data)))
		out = out[:len(out)-uvarintMaxLen+lenUvarintBytes]
		out = append(out, byte(chunkenc.EncXOR))
		out = append(out, chk.Raw.Data...)
		out = append(out, make([]byte, crc32.Size)...) // append a fake CRC32, because we don't use it
	}
	return
}

func (f *chunkReaderMock) Close() error {
	return nil
}

func (f *chunkReaderMock) reset() {
	f.toLoad = make(map[chunks.ChunkRef]groupLoadIdx)
}

// generateSeriesEntriesWithChunks generates seriesEntries with groups. Each chunk is a random byte slice.
func generateSeriesEntriesWithChunks(t *testing.T, numSeries, numChunksPerSeries int) []seriesEntry {
	out := make([]seriesEntry, 0, numSeries)
	labels := generateSeries([]int{numSeries})

	for i := 0; i < numSeries; i++ {
		entry := seriesEntry{
			lset: labels[i],
			refs: make([]chunks.ChunkRef, 0, numChunksPerSeries),
			chks: make([]storepb.AggrChunk, 0, numChunksPerSeries),
		}

		for j := 0; j < numChunksPerSeries; j++ {
			chunkBytes := make([]byte, 10)
			readBytes, err := rand.Read(chunkBytes)
			require.NoError(t, err, "couldn't generate test data")
			require.Equal(t, 10, readBytes, "couldn't generate test data")

			entry.refs = append(entry.refs, chunks.ChunkRef(i*numChunksPerSeries+j))
			entry.chks = append(entry.chks, storepb.AggrChunk{
				MinTime: int64(10 * j),
				MaxTime: int64(10 * (j + 1)),
				Raw:     &storepb.Chunk{Data: chunkBytes},
			})
		}
		out = append(out, entry)
	}
	return out
}

// generateSeriesEntries generates seriesEntries with groups. Each chunk is a random byte slice.
func generateSeriesEntries(t *testing.T, numSeries int) []seriesEntry {
	return generateSeriesEntriesWithChunks(t, numSeries, 50)
}

// sliceSeriesChunksSetIterator implements seriesChunksSetIterator and
// returns the provided err when the sets are exhausted
type sliceSeriesChunksSetIterator struct {
	current int
	sets    []seriesChunksSet

	err   error
	errAt int
}

func newSliceSeriesChunksSetIterator(sets ...seriesChunksSet) seriesChunksSetIterator {
	return &sliceSeriesChunksSetIterator{
		current: -1,
		sets:    sets,
	}
}

func newSliceSeriesChunksSetIteratorWithError(err error, errAt int, sets ...seriesChunksSet) seriesChunksSetIterator {
	return &sliceSeriesChunksSetIterator{
		current: -1,
		sets:    sets,
		err:     err,
		errAt:   errAt,
	}
}

func (s *sliceSeriesChunksSetIterator) Next() bool {
	s.current++

	// If the next item should fail, we return false. The Err() function will return the error.
	if s.err != nil && s.current >= s.errAt {
		return false
	}

	return s.current < len(s.sets)
}

func (s *sliceSeriesChunksSetIterator) At() seriesChunksSet {
	return s.sets[s.current]
}

func (s *sliceSeriesChunksSetIterator) Err() error {
	if s.err != nil && s.current >= s.errAt {
		return s.err
	}
	return nil
}

// delayedSeriesChunksSetIterator implements seriesChunksSetIterator and
// introduces an artificial delay before returning from Next() and At().
type delayedSeriesChunksSetIterator struct {
	wrapped seriesChunksSetIterator
	delay   time.Duration
}

func newDelayedSeriesChunksSetIterator(delay time.Duration, wrapped seriesChunksSetIterator) seriesChunksSetIterator {
	return &delayedSeriesChunksSetIterator{
		wrapped: wrapped,
		delay:   delay,
	}
}

func (s *delayedSeriesChunksSetIterator) Next() bool {
	time.Sleep(s.delay)
	return s.wrapped.Next()
}

func (s *delayedSeriesChunksSetIterator) At() seriesChunksSet {
	time.Sleep(s.delay)
	return s.wrapped.At()
}

func (s *delayedSeriesChunksSetIterator) Err() error {
	return s.wrapped.Err()
}

func generateAggrChunk(num int) []storepb.AggrChunk {
	out := make([]storepb.AggrChunk, 0, num)

	for i := 0; i < num; i++ {
		out = append(out, storepb.AggrChunk{
			MinTime: int64(i),
			MaxTime: int64(i),
		})
	}

	return out
}

type releaserMock struct {
	released *atomic.Bool
}

func newReleaserMock() *releaserMock {
	return &releaserMock{
		released: atomic.NewBool(false),
	}
}

func (r *releaserMock) Release() {
	r.released.Store(true)
}

func (r *releaserMock) isReleased() bool {
	return r.released.Load()
}

func readAllSeriesChunksSets(it seriesChunksSetIterator) []seriesChunksSet {
	var out []seriesChunksSet
	for it.Next() {
		out = append(out, it.At())
	}
	return out
}

func readAllSeriesLabels(it storepb.SeriesSet) []labels.Labels {
	var out []labels.Labels
	for it.Next() {
		lbls, _ := it.At()
		out = append(out, lbls)
	}
	return out
}

type inMemory struct {
	cached map[string]map[chunkscache.Range][]byte
}

func newInmemoryChunksCache() chunkscache.ChunksCache {
	return &inMemory{
		cached: map[string]map[chunkscache.Range][]byte{},
	}
}

func (c *inMemory) FetchMultiChunks(ctx context.Context, userID string, ranges []chunkscache.Range, bytesPool *pool.SafeSlabPool[byte]) (hits map[chunkscache.Range][]byte, misses []chunkscache.Range) {
	hits = make(map[chunkscache.Range][]byte, len(ranges))
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

func (c *inMemory) StoreChunks(_ context.Context, userID string, r chunkscache.Range, v []byte) {
	if c.cached[userID] == nil {
		c.cached[userID] = make(map[chunkscache.Range][]byte)
	}
	c.cached[userID][r] = v
}
