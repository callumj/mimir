// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/grafana/dskit/cache"

	"github.com/grafana/mimir/pkg/querier/stats"
)

const (
	// TODO think about a reasonable value for cardinalityEstimateTTL
	cardinalityEstimateTTL = 7 * 24 * time.Hour
)

type cardinalityEstimation struct {
	cache cache.Cache
	next  Handler
}

func newCardinalityEstimationMiddleware() Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return &cardinalityEstimation{
			// TODO init cache
			next: next,
		}
	})
}

func (c *cardinalityEstimation) Do(ctx context.Context, request Request) (Response, error) {
	k := key(request)

	request = c.addEstimatedCardinalityForKey(ctx, request, k)

	res, err := c.next.Do(ctx, request)
	if err != nil {
		return nil, err
	}

	s := stats.FromContext(ctx)
	if s == nil {
		return res, err
	}
	c.storeCardinalityForKey(ctx, k, s.LoadFetchedSeries())
	return res, err
}

func (c *cardinalityEstimation) addEstimatedCardinalityForKey(ctx context.Context, request Request, k string) Request {
	if cardinality, ok := c.lookupCardinalityForKey(ctx, k); ok {
		if hints := request.GetHints(); hints != nil {
			hints.EstimatedCardinality = cardinality
		} else {
			request = request.WithHints(&Hints{EstimatedCardinality: cardinality})
		}
	}
	return request
}

func (c *cardinalityEstimation) storeCardinalityForKey(ctx context.Context, key string, count uint64) {
	c.cache.Store(ctx, map[string][]byte{cacheHashKey(key): marshalBinary(count)}, cardinalityEstimateTTL)
}

func key(r Request) string {
	return fmt.Sprintf("%s%d%d", r.GetQuery(), r.GetEnd()-r.GetStart(), r.GetStart()/((24 * time.Hour).Milliseconds()))
}

func (c *cardinalityEstimation) lookupCardinalityForKey(ctx context.Context, key string) (uint64, bool) {
	cacheKey := cacheHashKey(key)
	res := c.cache.Fetch(ctx, []string{cacheKey})
	if val, ok := res[cacheKey]; ok {
		return unmarshalBinary(val), ok
	}
	return 0, false
}

func marshalBinary(in uint64) []byte {
	marshaled := make([]byte, 8)
	binary.LittleEndian.PutUint64(marshaled, in)
	return marshaled
}

func unmarshalBinary(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}
