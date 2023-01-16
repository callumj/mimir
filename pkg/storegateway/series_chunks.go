// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storegateway/chunkscache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	// Mimir compacts blocks up to 24h. Assuming a 5s scrape interval as worst case scenario,
	// and 120 samples per chunk, there could be 86400 * (1 / 5) * (1 / 120) = 144 chunks for
	// a series in the biggest block. Using a slab size of 1000 looks a good trade-off to support
	// high frequency scraping without wasting too much memory in case of queries hitting a low
	// number of chunks (across series).
	seriesChunksSlabSize = 1000

	// Selected so that an individual chunk's data typically fits within the slab size (16 KiB)
	chunkBytesSlabSize = 16_384
)

var (
	seriesEntrySlicePool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})

	seriesChunksSlicePool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})

	chunkBytesSlicePool = pool.Interface(&sync.Pool{
		// Intentionally return nil if the pool is empty, so that the caller can preallocate
		// the slice with the right size.
		New: nil,
	})
)

// seriesChunksSetIterator is the interface implemented by an iterator returning a sequence of seriesChunksSet.
type seriesChunksSetIterator interface {
	Next() bool

	// At returns the current seriesChunksSet. The caller should (but NOT must) invoke seriesChunksSet.release()
	// on the returned set once it's guaranteed it will not be used anymore.
	At() seriesChunksSet

	Err() error
}

// seriesChunksSet holds a set of series, each with its own chunks.
type seriesChunksSet struct {
	series           []seriesEntry
	seriesReleasable bool

	// It gets lazy initialized (only if required).
	seriesChunksPool *pool.SlabPool[storepb.AggrChunk]

	// chunksReleaser releases the memory used to allocate series chunks.
	chunksReleaser chunksReleaser
}

// newSeriesChunksSet creates a new seriesChunksSet. The series slice is pre-allocated with
// the provided seriesCapacity at least. This means this function GUARANTEES the series slice
// will have a capacity of at least seriesCapacity.
//
// If seriesReleasable is true, then a subsequent call release() will put the internal
// series slices to a memory pool for reusing.
func newSeriesChunksSet(seriesCapacity int, seriesReleasable bool) seriesChunksSet {
	var prealloc []seriesEntry

	// If it's releasable then we try to reuse a slice from the pool.
	if seriesReleasable {
		if reused := seriesEntrySlicePool.Get(); reused != nil {
			prealloc = *(reused.(*[]seriesEntry))

			// The capacity MUST be guaranteed. If it's smaller, then we forget it and will be
			// reallocated.
			if cap(prealloc) < seriesCapacity {
				prealloc = nil
			}
		}
	}

	if prealloc == nil {
		prealloc = make([]seriesEntry, 0, seriesCapacity)
	}

	return seriesChunksSet{
		series:           prealloc,
		seriesReleasable: seriesReleasable,
	}
}

type chunksReleaser interface {
	// Release the memory used to allocate series chunks.
	Release()
}

// release the internal series and chunks slices to a memory pool, and call the chunksReleaser.Release().
// The series and chunks slices won't be released to a memory pool if seriesChunksSet was created to be not releasable.
//
// This function is not idempotent. Calling it twice would introduce subtle bugs.
func (b *seriesChunksSet) release() {
	if b.chunksReleaser != nil {
		b.chunksReleaser.Release()
	}

	if b.seriesReleasable {
		// Reset series and chunk entries, before putting back to the pool.
		for i := range b.series {
			for c := range b.series[i].chks {
				b.series[i].chks[c].Reset()
			}

			b.series[i] = seriesEntry{}
		}

		if b.seriesChunksPool != nil {
			b.seriesChunksPool.Release()
		}

		reuse := b.series[:0]
		seriesEntrySlicePool.Put(&reuse)
	}
}

// newSeriesAggrChunkSlice returns a []storepb.AggrChunk guaranteed to have length and capacity
// equal to the provided size. The returned slice may be picked from a memory pool and then released
// back once release() gets invoked.
func (b *seriesChunksSet) newSeriesAggrChunkSlice(size int) []storepb.AggrChunk {
	if !b.seriesReleasable {
		return make([]storepb.AggrChunk, size)
	}

	// Lazy initialise the pool.
	if b.seriesChunksPool == nil {
		b.seriesChunksPool = pool.NewSlabPool[storepb.AggrChunk](seriesChunksSlicePool, seriesChunksSlabSize)
	}

	return b.seriesChunksPool.Get(size)
}

func (b *seriesChunksSet) len() int {
	return len(b.series)
}

type seriesChunksSeriesSet struct {
	from seriesChunksSetIterator

	currSet    seriesChunksSet
	currOffset int
}

func newSeriesChunksSeriesSet(from seriesChunksSetIterator) storepb.SeriesSet {
	return &seriesChunksSeriesSet{
		from: from,
	}
}

func newSeriesSetWithChunks(
	ctx context.Context,
	userID string,
	chunkReaders bucketChunkReaders,
	refsIterator seriesChunkRefsSetIterator,
	refsIteratorBatchSize int,
	stats *safeQueryStats,
	iteratorLoadDurations *prometheus.HistogramVec,
	cache chunkscache.ChunksCache,
	minT, maxT int64,
	m *BucketStoreMetrics,
) storepb.SeriesSet {
	var iterator seriesChunksSetIterator
	iterator = newLoadingSeriesChunksSetIterator(ctx, userID, chunkReaders, refsIterator, refsIteratorBatchSize, stats, cache, minT, maxT, m.chunksRefetches)
	iterator = newDurationMeasuringIterator[seriesChunksSet](iterator, iteratorLoadDurations.WithLabelValues("chunks_load"))
	iterator = newPreloadingSetIterator[seriesChunksSet](ctx, 1, iterator)
	// We are measuring the time we wait for a preloaded batch. In an ideal world this is 0 because there's always a preloaded batch waiting.
	// But realistically it will not be. Along with the duration of the chunks_load iterator,
	// we can determine where is the bottleneck in the streaming pipeline.
	iterator = newDurationMeasuringIterator[seriesChunksSet](iterator, iteratorLoadDurations.WithLabelValues("chunks_preloaded"))
	return newSeriesChunksSeriesSet(iterator)
}

// Next advances to the next item. Once the underlying seriesChunksSet has been fully consumed
// (which means the call to Next moves to the next set), the seriesChunksSet is released. This
// means that it's not safe to read from the values returned by At() after Next() is called again.
func (b *seriesChunksSeriesSet) Next() bool {
	b.currOffset++
	if b.currOffset >= b.currSet.len() {
		// The current set won't be accessed anymore because the iterator is moving to the next one,
		// so we can release it.
		b.currSet.release()

		if !b.from.Next() {
			b.currSet = seriesChunksSet{}
			return false
		}

		b.currSet = b.from.At()
		b.currOffset = 0
	}
	return true
}

// At returns the current series. The result from At() MUST not be retained after calling Next()
func (b *seriesChunksSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	if b.currOffset >= b.currSet.len() {
		return nil, nil
	}

	return b.currSet.series[b.currOffset].lset, b.currSet.series[b.currOffset].chks
}

func (b *seriesChunksSeriesSet) Err() error {
	return b.from.Err()
}

// preloadedSeriesChunksSet holds the result of preloading the next set. It can either contain
// the preloaded set or an error, but not both.
type preloadedSeriesChunksSet[T any] struct {
	set T
	err error
}

type genericIterator[V any] interface {
	Next() bool
	At() V
	Err() error
}

type preloadingSetIterator[Set any] struct {
	ctx  context.Context
	from genericIterator[Set]

	current Set

	preloaded chan preloadedSeriesChunksSet[Set]
	err       error
}

func newPreloadingSetIterator[Set any](ctx context.Context, preloadedSetsCount int, from genericIterator[Set]) *preloadingSetIterator[Set] {
	preloadedSet := &preloadingSetIterator[Set]{
		ctx:       ctx,
		from:      from,
		preloaded: make(chan preloadedSeriesChunksSet[Set], preloadedSetsCount-1), // one will be kept outside the channel when the channel blocks
	}
	go preloadedSet.preload()
	return preloadedSet
}

func (p *preloadingSetIterator[Set]) preload() {
	defer close(p.preloaded)

	for p.from.Next() {
		select {
		case <-p.ctx.Done():
			// If the context is done, we should just stop the preloading goroutine.
			return
		case p.preloaded <- preloadedSeriesChunksSet[Set]{set: p.from.At()}:
		}
	}

	if p.from.Err() != nil {
		p.preloaded <- preloadedSeriesChunksSet[Set]{err: p.from.Err()}
	}
}

func (p *preloadingSetIterator[Set]) Next() bool {
	preloaded, ok := <-p.preloaded
	if !ok {
		// Iteration reached the end or context has been canceled.
		return false
	}

	p.current = preloaded.set
	p.err = preloaded.err

	return p.err == nil
}

func (p *preloadingSetIterator[Set]) At() Set {
	return p.current
}

func (p *preloadingSetIterator[Set]) Err() error {
	return p.err
}

type loadingSeriesChunksSetIterator struct {
	ctx    context.Context
	userID string

	chunkReaders  bucketChunkReaders
	from          seriesChunkRefsSetIterator
	fromBatchSize int
	stats         *safeQueryStats

	cache chunkscache.ChunksCache

	current seriesChunksSet
	err     error
	minTime int64
	maxTime int64

	refetches prometheus.Counter
}

// TODO dimitarvdimitrov add/update tests with the chunks cache
func newLoadingSeriesChunksSetIterator(
	ctx context.Context,
	userID string,
	chunkReaders bucketChunkReaders,
	from seriesChunkRefsSetIterator,
	fromBatchSize int,
	stats *safeQueryStats,
	cache chunkscache.ChunksCache,
	minT int64,
	maxT int64,
	refetches prometheus.Counter,
) *loadingSeriesChunksSetIterator {
	return &loadingSeriesChunksSetIterator{
		ctx:           ctx,
		userID:        userID,
		chunkReaders:  chunkReaders,
		from:          from,
		fromBatchSize: fromBatchSize,
		stats:         stats,
		cache:         cache,
		minTime:       minT,
		maxTime:       maxT,
		refetches:     refetches,
	}
}

func (c *loadingSeriesChunksSetIterator) Next() (retHasNext bool) {
	if c.err != nil {
		return false
	}

	if !c.from.Next() {
		c.err = c.from.Err()
		return false
	}

	nextUnloaded := c.from.At()

	// This data structure doesn't retain the seriesChunkRefsSet so it can be released once done.
	defer nextUnloaded.release()

	c.chunkReaders.reset()
	groupCount := 0
	for _, s := range nextUnloaded.series {
		groupCount += len(s.groups)
	}
	loadedGroups := make([][]byte, groupCount)

	// Create a batched memory pool that can be released all at once. We keep all chunks bytes there.
	chunksPool := pool.NewSafeSlabPool[byte](chunkBytesSlicePool, chunkBytesSlabSize)
	var cachedRanges map[chunkscache.Range][]byte
	if c.cache != nil {
		cachedRanges, _ = c.cache.FetchMultiChunks(c.ctx, c.userID, chunksPool, toCacheRanges(nextUnloaded.series, groupCount))
	}

	// Collect the cached groups bytes or prepare to fetch cache misses from the bucket.
	currentGroup := 0
	for _, s := range nextUnloaded.series {
		for _, group := range s.groups {
			cacheRange := toChunkRange(group)
			if cachedBytes, ok := cachedRanges[cacheRange]; ok {
				loadedGroups[currentGroup] = cachedBytes
				currentGroup++
				continue
			}
			err := c.chunkReaders.addLoadGroup(group.blockID, group, currentGroup)
			if err != nil {
				c.err = errors.Wrap(err, "preloading chunks")
				return false
			}
			currentGroup++
		}
	}

	err := c.chunkReaders.loadGroups(loadedGroups, chunksPool, c.stats)
	if err != nil {
		c.err = errors.Wrap(err, "loading chunks")
		return false
	}

	c.chunkReaders.reset()
	// Pre-allocate the series slice using the expected batchSize even if nextUnloaded has less elements,
	// so that there's a higher chance the slice will be reused once released.
	nextSet := newSeriesChunksSet(util_math.Max(c.fromBatchSize, nextUnloaded.len()), true)
	nextSet.chunksReleaser = chunksPool

	// Release the set if an error occurred.
	defer func() {
		if !retHasNext && c.err != nil {
			nextSet.release()
		}
	}()

	// The series slice is guaranteed to have at least the requested capacity,
	// so can safely expand it.
	nextSet.series = nextSet.series[:nextUnloaded.len()]

	// Parse the bytes we have from the cache or the bucket. This returns the groups for which we didn't have
	// enough fetched bytes. This may happen when the group length was underestimated.
	underfetchedGroups, err := c.parseGroups(nextUnloaded, &nextSet, loadedGroups)
	if err != nil {
		c.err = err
		return false
	}
	if len(underfetchedGroups) > 0 {
		err = c.refetchGroups(underfetchedGroups, nextUnloaded, loadedGroups, chunksPool, nextSet)
		if err != nil {
			c.err = err
			return false
		}
	}

	c.storeChunkGroups(nextUnloaded, cachedRanges, loadedGroups)

	// Since groups may contain more chunks that we need for the request,
	// go through all chunks and reslice to remove any chunks that are outside the request's MinT/MaxT
	for i, s := range nextSet.series {
		firstOverlappingIdx, lastOverlappingIdx, someOverlap := overlappingChunksIndices(s.chks, c.minTime, c.maxTime)
		if someOverlap {
			nextSet.series[i].chks = s.chks[firstOverlappingIdx : lastOverlappingIdx+1]
		}
	}

	c.current = nextSet
	return true
}

// TODO dimitarvdimitrov add metric with the number of times we call this
func (c *loadingSeriesChunksSetIterator) refetchGroups(underfetchedGroups []underfetchedGroupIdx, nextUnloaded seriesChunkRefsSet, loadedGroups [][]byte, chunksPool *pool.SafeSlabPool[byte], nextSet seriesChunksSet) error {
	for _, g := range underfetchedGroups {
		err := c.chunkReaders.addLoadGroup(g.blockID, nextUnloaded.series[g.seriesIdx].groups[g.groupIdx], g.groupIdx)
		if err != nil {
			return fmt.Errorf("add load underfetched block %s first ref %d: %w", g.blockID, nextUnloaded.series[g.seriesIdx].groups[g.groupIdx].firstRef(), err)
		}
	}

	// Go back to the bucket to fetch anything we undefetched.
	err := c.chunkReaders.loadGroups(loadedGroups, chunksPool, c.stats)
	if err != nil {
		return errors.Wrap(err, "refetch groups")
	}
	for _, indices := range underfetchedGroups {
		populatedChks := nextSet.series[indices.seriesIdx].chks
		groupChunksCount := len(nextUnloaded.series[indices.seriesIdx].groups[indices.groupIdx].chunks)
		ok, lastChkLen, err := parseGroup(loadedGroups[indices.loadedGroupIdx], populatedChks[indices.firstChkIdx:indices.firstChkIdx+groupChunksCount])
		if err != nil {
			return errors.Wrap(err, "parsing underfetched group")
		}
		if !ok {
			return fmt.Errorf("chunk length doesn't match after refetching (lastChkLen %d, lset %s, group index %d)", lastChkLen, nextSet.series[indices.seriesIdx].lset, indices.groupIdx)
		}
	}
	return nil
}

func overlappingChunksIndices(chks []storepb.AggrChunk, minT, maxT int64) (int, int, bool) {
	firstOverlappingIdx, lastOverlappingIdx := len(chks), 0
	for j, chk := range chks {
		if chk.MaxTime >= minT || chk.MinTime <= maxT {
			if firstOverlappingIdx > j {
				firstOverlappingIdx = j
			}
			if lastOverlappingIdx < j {
				lastOverlappingIdx = j
			}
		}
	}
	return firstOverlappingIdx, lastOverlappingIdx, firstOverlappingIdx <= lastOverlappingIdx
}

func (c *loadingSeriesChunksSetIterator) storeChunkGroups(set seriesChunkRefsSet, cachedRanges map[chunkscache.Range][]byte, loadedGroups [][]byte) {
	if c.cache == nil {
		return
	}
	currentGroup := 0
	for _, s := range set.series {
		for _, g := range s.groups {
			if _, ok := cachedRanges[toChunkRange(g)]; ok {
				currentGroup++
				continue
			}
			// This was parsed ok and we didn't get it from the cache, so we should cache it.
			// TODO figure out how to release pooled bytes after they've been cached
			// Doing a copy shouldn't be the end of the world provided there is some decent cache hit ratio
			toCache := make([]byte, len(loadedGroups[currentGroup]))
			// Memcached caching is async, so we can't use the pooled bytes to send to memcached
			copy(toCache, loadedGroups[currentGroup])
			c.cache.StoreChunks(c.ctx, c.userID, toChunkRange(g), toCache)
			currentGroup++
		}
	}
}

type underfetchedGroupIdx struct {
	blockID        ulid.ULID
	seriesIdx      int
	groupIdx       int
	firstChkIdx    int
	loadedGroupIdx int
}

// parseGroups parses the passed bytes into nextSet. In case a group was underfetched, parseGroups will return an underfetchedGroupIdx
// with the indices of the group; parseGroups will also set the correct length of the last chunk in the group
// because it was the last chunk which was estimated.
func (c *loadingSeriesChunksSetIterator) parseGroups(nextUnloaded seriesChunkRefsSet, nextSet *seriesChunksSet, loadedGroups [][]byte) ([]underfetchedGroupIdx, error) {
	var underfetchedGroups []underfetchedGroupIdx
	currentGroupIdx := 0
	for i, unloadedS := range nextUnloaded.series {
		nextSet.series[i].lset = unloadedS.lset
		chunksCount := 0
		for _, g := range unloadedS.groups {
			chunksCount += len(g.chunks)
		}
		populatedChks := nextSet.newSeriesAggrChunkSlice(chunksCount)
		nextSet.series[i].chks = populatedChks
		populatedChksCount := 0
		for j, g := range unloadedS.groups {
			groupChunks := populatedChks[populatedChksCount : populatedChksCount+len(g.chunks)]

			for k, c := range g.chunks {
				groupChunks[k].MinTime = c.minTime
				groupChunks[k].MaxTime = c.maxTime
				if groupChunks[k].Raw == nil {
					// This may come as initialized from the pool. Do an allocation only if it already isn't.
					groupChunks[k].Raw = &storepb.Chunk{}
				}
			}

			ok, lastChkLen, err := parseGroup(loadedGroups[currentGroupIdx], groupChunks)
			if err != nil {
				return nil, fmt.Errorf("parsing chunk group (block %s, first ref %d, num chunks %d): %w", g.blockID, g.firstRef(), len(g.chunks), err)
			}
			if !ok {
				// We estimate the length of the last chunk of a series.
				// Unfortunately, we got it wrong. We need to refetch the whole group.
				// We set the length correctly because we now know it.
				unloadedS.groups[j].chunks[len(unloadedS.groups[j].chunks)-1].length = lastChkLen
				underfetchedGroups = append(underfetchedGroups, underfetchedGroupIdx{
					seriesIdx:      i,
					groupIdx:       j,
					firstChkIdx:    populatedChksCount,
					loadedGroupIdx: currentGroupIdx,
					blockID:        g.blockID,
				})
			}
			populatedChksCount += len(g.chunks)
			currentGroupIdx++
		}
	}
	return underfetchedGroups, nil
}

// parseGroup parses the byte slice as concatenated encoded chunks. lastChunkLen is non-zero when allChunksComplete==false.
// An error is returned when gBytes are malformed or when not only the last chunk is incomplete.
func parseGroup(gBytes []byte, chunks []storepb.AggrChunk) (allChunksComplete bool, lastChunkLen int64, _ error) {
	for i := range chunks {
		chunkDataLen, n := binary.Uvarint(gBytes)
		if n == 0 {
			return false, 0, fmt.Errorf("not enough bytes to infer length of chunk %d/%d", i, len(chunks))
		}
		if n < 0 {
			return false, 0, fmt.Errorf("chunk length doesn't fit into uint64 %d/%d", i, len(chunks))
		}
		// ┌───────────────┬───────────────────┬──────────────┬────────────────┐
		// │ len <uvarint> │ encoding <1 byte> │ data <bytes> │ CRC32 <4 byte> │
		// └───────────────┴───────────────────┴──────────────┴────────────────┘
		totalChunkLen := n + 1 + int(chunkDataLen) + crc32.Size
		if totalChunkLen > len(gBytes) {
			if i != len(chunks)-1 {
				return false, 0, fmt.Errorf("underfetched before the last chunk, don't know what to do (chunk idx %d/%d, fetched %d/%d bytes)", i, len(chunks), len(gBytes), totalChunkLen)
			}
			return false, int64(totalChunkLen), nil
		}
		c := rawChunk(gBytes[n : n+1+int(chunkDataLen)])
		if cEnc := c.Encoding(); cEnc != chunkenc.EncXOR {
			return false, 0, fmt.Errorf("encoding (%d, %s) isn't XOR, don't know what to do ", cEnc, cEnc.String())
		}
		chunks[i].Raw.Type = storepb.Chunk_XOR
		chunks[i].Raw.Data = c.Bytes()
		// We ignore the crc32 because we assume that the chunk didn't get corrupted.
		// TODO maybe check the crc for every 1 in 100 chunks? 1 in 1000?
		gBytes = gBytes[totalChunkLen:]
	}
	return true, 0, nil
}

func toCacheRanges(series []seriesChunkRefs, totalRanges int) []chunkscache.Range {
	ranges := make([]chunkscache.Range, 0, totalRanges)
	for _, s := range series {
		for _, g := range s.groups {
			ranges = append(ranges, toChunkRange(g))
		}
	}
	return ranges
}

func toChunkRange(g chunksGroup) chunkscache.Range {
	return chunkscache.Range{
		BlockID:   g.blockID,
		Start:     g.firstRef(),
		NumChunks: len(g.chunks),
	}
}

func (c *loadingSeriesChunksSetIterator) At() seriesChunksSet {
	return c.current
}

func (c *loadingSeriesChunksSetIterator) Err() error {
	return c.err
}

type durationMeasuringIterator[Set any] struct {
	from             genericIterator[Set]
	durationObserver prometheus.Observer
}

func newDurationMeasuringIterator[Set any](from genericIterator[Set], durationObserver prometheus.Observer) genericIterator[Set] {
	return &durationMeasuringIterator[Set]{
		from:             from,
		durationObserver: durationObserver,
	}
}

func (m *durationMeasuringIterator[Set]) Next() bool {
	start := time.Now()
	next := m.from.Next()
	m.durationObserver.Observe(time.Since(start).Seconds())
	return next
}

func (m *durationMeasuringIterator[Set]) At() Set {
	return m.from.At()
}

func (m *durationMeasuringIterator[Set]) Err() error {
	return m.from.Err()
}
