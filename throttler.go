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
	closing chan struct{}

	// A pool of unused buckets.
	pool *sync.Pool

	// This mutex guards bucket maps.
	mu sync.RWMutex
	// TODO: should we store a pointer?
	buckets      map[string]*Bucket
	takenBuckets map[string]uint
}

func newThrottler(cf func() interface{}) *Throttler {
	ret := &Throttler{buckets: make(map[string]*Bucket), takenBuckets: make(map[string]uint)}
	ret.closing = make(chan struct{})
	ret.pool = &sync.Pool{}
	ret.closing = make(chan struct{})

	ret.pool.New = cf

	// Fetch fillInterval from sample bucket
	var fillInterval time.Duration
	var capacity int64
	{
		bucket := cf().(*Bucket)
		capacity = bucket.capacity
		fillInterval = bucket.fillInterval
	}

	ticker := time.NewTicker(fillInterval * time.Duration(capacity))

	// Launch scanning goroutine
	go func() {
		for _ = range ticker.C {
			select {
			case <-ret.closing:
				return
			default:
			}
			// Clear buckets that are unused more than 4 fill intervals.
			// Prefetch bucket tags
			ret.mu.RLock()
			keys := make([]string, len(ret.buckets))
			i := 0
			for k := range ret.buckets {
				keys[i] = k
				i++
			}
			ret.mu.RUnlock()

			for _, tag := range keys {
				ret.checkBucket(4, tag)
			}
		}
	}()

	return ret
}

// NewThrottler creates a throttler with default bucket parameters.
// Similar to NewBucket.
func NewThrottler(fillInterval time.Duration, capacity int64) *Throttler {
	return newThrottler(func() interface{} { return NewBucket(fillInterval, capacity) })
}

// NewThrottler creates a throttler with default bucket parameters.
// Similar to NewBucketWithRate.
func NewThrottlerWithRate(rate float64, capacity int64) *Throttler {
	return newThrottler(func() interface{} { return NewBucketWithRate(rate, capacity) })
}

// checkBucket scans bucket from collection and removes it if it's unused.
// tickThres param regulates how many refillIntervals should pass
// before unused bucket gets deleted.
func (tb *Throttler) checkBucket(tickThres int64, tag string) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Skip taken buckets
	if _, taken := tb.takenBuckets[tag]; taken {
		return
	}

	now := time.Now()

	// Check if it was taken again
	if _, taken := tb.takenBuckets[tag]; taken {
		return
	}

	bucket := tb.buckets[tag]
	currentTick := int64(now.Sub(bucket.startTime) / bucket.fillInterval)
	// Skip buckets that got updated recently
	if currentTick-bucket.availTick < tickThres {
		return
	}

	bucket.adjust(now)

	// Skip not-full buckets; this should never happen really
	if bucket.capacity != bucket.avail {
		return
	}

	// It's safe to remove this bucket to the pool
	delete(tb.takenBuckets, tag)
	delete(tb.buckets, tag)
	tb.pool.Put(bucket)
}

// Bucket takes a bucket for specified tag, creating one if it doesn't exist.
// Remember to call Throttler.Pool when you don't need a bucket anymore!
func (tb *Throttler) Bucket(tag string) *Bucket {
	// try to get a bucket from map
	tb.mu.RLock()
	ret, ok := tb.buckets[tag]
	tb.mu.RUnlock()

	// try to get a bucket from pool
	if !ok {
		ret = tb.pool.Get().(*Bucket)
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
