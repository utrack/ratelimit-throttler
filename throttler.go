package ratelimit

import (
	"sync"
	"time"
)

// Throttler is a thread-safe tag-based bucket container.
// Concurrent calls for the same tag returns same bucket,
// so it's safe to use one Throttler for user-based throttling.
// Unused buckets are pooled and reused over time.
type Throttler struct {
	creator func() *Bucket
	closing chan struct{}

	// A pool of unused buckets.
	pool *pool

	// This mutex guards bucket maps.
	mu sync.RWMutex
	// TODO: should we store a pointer?
	buckets      map[string]*Bucket
	takenBuckets map[string]uint
}

func newThrottler(cf func() *Bucket) *Throttler {
	ret := &Throttler{buckets: make(map[string]*Bucket), takenBuckets: make(map[string]uint)}
	ret.creator = cf
	ret.closing = make(chan struct{})
	ret.pool = newPool(50)
	ret.closing = make(chan struct{})

	// Fetch fillInterval from sample bucket
	var fillInterval time.Duration
	{
		bucket := ret.creator()
		fillInterval = bucket.fillInterval
	}

	ticker := time.NewTicker(fillInterval * 5)

	// Launch scanning goroutine
	go func() {
		for _ = range ticker.C {
			select {
			case <-ret.closing:
				return
			default:
			}
			// Clear buckets that are unused more than 4 fill intervals.
			ret.traverse(4)
		}
	}()

	return ret
}

// NewThrottler creates a throttler with default bucket parameters.
// Similar to NewBucket.
func NewThrottler(fillInterval time.Duration, capacity int64) *Throttler {
	return newThrottler(func() *Bucket { return NewBucket(fillInterval, capacity) })
}

// NewThrottler creates a throttler with default bucket parameters.
// Similar to NewBucketWithRate.
func NewThrottlerWithRate(rate float64, capacity int64) *Throttler {
	return newThrottler(func() *Bucket { return NewBucketWithRate(rate, capacity) })
}

// traverse scans every bucket in collection and removes unused ones.
// tickThres param regulates how many refillIntervals should pass
// before unused bucket gets deleted.
func (tb *Throttler) traverse(tickThres int64) {
	tb.mu.Lock()
	defer tb.mu.Lock()
	for tag, bucket := range tb.buckets {
		// Skip taken buckets
		if _, taken := tb.takenBuckets[tag]; taken {
			continue
		}

		now := time.Now()

		currentTick := int64(now.Sub(bucket.startTime) / bucket.fillInterval)
		// Skip buckets that got updated recently
		if currentTick-bucket.availTick < tickThres {
			continue
		}

		bucket.adjust(now)

		// Skip not-full buckets; this should never happen really
		if bucket.capacity != bucket.avail {
			continue
		}

		// It's safe to remove this bucket to the pool
		delete(tb.takenBuckets, tag)
		delete(tb.buckets, tag)
		tb.pool.Pool(bucket)
	}
}

// Bucket takes a bucket for specified tag, creating one if it doesn't exist.
// Remember to call Throttler.Pool when you don't need a bucket anymore!
func (tb *Throttler) Bucket(tag string) *Bucket {
	// try to get a bucket from map
	tb.mu.RLock()
	ret := tb.buckets[tag]
	tb.mu.RUnlock()

	// try to get a bucket from pool
	if ret == nil {
		ret = tb.pool.Get()
	}
	// construct new bucket if pool is empty
	if ret == nil {
		ret = tb.creator()
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.takenBuckets[tag]++
	tb.buckets[tag] = ret
	ret.tag = tag
	return ret
}

// Pool returns bucket to the throttler for later reuse.
func (tb *Throttler) Pool(b *Bucket) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.takenBuckets[b.tag]--
	if tb.takenBuckets[b.tag] == 0 {
		delete(tb.takenBuckets, b.tag)
	}
}

// Close shuts down cleanup goroutine.
func (tb *Throttler) Close() {
	close(tb.closing)
}
