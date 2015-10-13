package ratelimit

import (
	"time"

	gc "launchpad.net/gocheck"
)

func (rateLimitSuite) TestThrCreate(c *gc.C) {
	tag := "tag1"
	thr := NewThrottler(time.Second, 10)
	b := thr.Bucket(tag)
	c.Assert(b.tag, gc.Equals, tag)
	thr.mu.Lock()
	_, taken := thr.takenBuckets[tag]
	thr.mu.Unlock()
	c.Assert(taken, gc.Equals, true)

	b.Take(int64(1))
	thr.Pool(b)
	thr.mu.Lock()
	_, taken = thr.takenBuckets[tag]
	thr.mu.Unlock()
	c.Assert(taken, gc.Equals, false)
}

func (rateLimitSuite) TestThrGetSame(c *gc.C) {
	tag := "tag1"
	thr := NewThrottler(time.Second, 10)
	b := thr.Bucket(tag)
	b.Take(10)
	thr.Pool(b)

	b = thr.Bucket(tag)
	taken := b.TakeAvailable(10)
	c.Assert(taken, gc.Not(gc.Equals), int64(10))

}

func (rateLimitSuite) TestThrClose(c *gc.C) {
	thr := NewThrottler(time.Second, 10)
	thr.Close()
}

func (rateLimitSuite) TestThrConcGet(c *gc.C) {
	tag := "tag1"
	thr := NewThrottler(time.Second, int64(10))
	b := thr.Bucket(tag)
	b2 := thr.Bucket(tag)

	thr.Pool(b)

	thr.mu.RLock()
	c.Assert(len(thr.takenBuckets), gc.Equals, 1)
	thr.mu.RUnlock()

	thr.Pool(b2)

	thr.mu.RLock()
	c.Assert(len(thr.takenBuckets), gc.Equals, 0)
	thr.mu.RUnlock()

}

func (rateLimitSuite) TestThrPool(c *gc.C) {
	tag := "tag1"
	thr := NewThrottler(time.Second, int64(1))
	b := thr.Bucket(tag)

	thr.Pool(b)

	time.Sleep(time.Second * 6)

	thr.mu.Lock()
	c.Assert(len(thr.buckets), gc.Equals, 0)
	thr.mu.Unlock()

}
