// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	// ShardLabel is a reserved label referencing a shard on read path.
	ShardLabel = "__query_shard__"
)

var (
	seps = []byte{'\xff'}
)

// ShardSelector holds information about the configured query shard.
type ShardSelector struct {
	ShardIndex uint64
	ShardCount uint64
}

// LabelValue returns the label value to use to select this shard.
func (shard ShardSelector) LabelValue() string {
	return FormatShardIDLabelValue(shard.ShardIndex, shard.ShardCount)
}

// Label generates the ShardSelector as a label.
func (shard ShardSelector) Label() labels.Label {
	return labels.Label{
		Name:  ShardLabel,
		Value: shard.LabelValue(),
	}
}

// Matcher converts ShardSelector to Matcher.
func (shard ShardSelector) Matcher() *labels.Matcher {
	return labels.MustNewMatcher(labels.MatchEqual, ShardLabel, shard.LabelValue())
}

// ShardFromMatchers extracts a ShardSelector and the index it was pulled from the matcher list.
func ShardFromMatchers(matchers []*labels.Matcher) (shard *ShardSelector, idx int, err error) {
	for i, matcher := range matchers {
		if matcher.Name == ShardLabel && matcher.Type == labels.MatchEqual {
			index, count, err := ParseShardIDLabelValue(matcher.Value)
			if err != nil {
				return nil, i, err
			}
			return &ShardSelector{
				ShardIndex: index,
				ShardCount: count,
			}, i, nil
		}
	}
	return nil, 0, nil
}

// RemoveShardFromMatchers returns the input matchers without the label matcher on the query shard (if any).
func RemoveShardFromMatchers(matchers []*labels.Matcher) (shard *ShardSelector, filtered []*labels.Matcher, err error) {
	shard, idx, err := ShardFromMatchers(matchers)
	if err != nil || shard == nil {
		return nil, matchers, err
	}

	// Create a new slice with the shard matcher removed.
	filtered = make([]*labels.Matcher, 0, len(matchers)-1)
	filtered = append(filtered, matchers[:idx]...)
	filtered = append(filtered, matchers[idx+1:]...)

	return shard, filtered, nil
}

// FormatShardIDLabelValue expects 0-based shardID, but uses 1-based shard in the output string.
func FormatShardIDLabelValue(shardID, shardCount uint64) string {
	return fmt.Sprintf("%d_of_%d", shardID+1, shardCount)
}

// ParseShardIDLabelValue returns original (0-based) shard index and shard count parsed from formatted value.
func ParseShardIDLabelValue(val string) (index, shardCount uint64, _ error) {
	// If we fail to parse shardID, we better not consider this block fully included in successors.
	matches := strings.Split(val, "_")
	if len(matches) != 3 || matches[1] != "of" {
		return 0, 0, errors.Errorf("invalid shard ID: %q", val)
	}

	index, err := strconv.ParseUint(matches[0], 10, 64)
	if err != nil {
		return 0, 0, errors.Errorf("invalid shard ID: %q: %v", val, err)
	}
	count, err := strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return 0, 0, errors.Errorf("invalid shard ID: %q: %v", val, err)
	}

	if index == 0 || count == 0 || index > count {
		return 0, 0, errors.Errorf("invalid shard ID: %q", val)
	}

	return index - 1, count, nil
}

// ShardFunc is a simpler API to the above, when you don't need to avoid allocations.
func ShardFunc(l labels.Labels) uint64 {
	hash, _ := labelsXXHash(make([]byte, 0, 1024), l)
	return hash
}

// labelsXXHash replicates historical hash computation from Prometheus circa 2022.
// The buffer b can be recycled to avoid allocations.
func labelsXXHash(b []byte, ls labels.Labels) (uint64, []byte) {
	b = b[:0]

	for i, v := range ls {
		if len(b)+len(v.Name)+len(v.Value)+2 >= cap(b) {
			// If labels entry is 1KB+ do not allocate whole entry.
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, v := range ls[i:] {
				_, _ = h.WriteString(v.Name)
				_, _ = h.Write(seps)
				_, _ = h.WriteString(v.Value)
				_, _ = h.Write(seps)
			}
			return h.Sum64(), b
		}

		b = append(b, v.Name...)
		b = append(b, seps[0])
		b = append(b, v.Value...)
		b = append(b, seps[0])
	}
	return xxhash.Sum64(b), b
}
