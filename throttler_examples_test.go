package ratelimit

import "time"

// This example shows basic Throttler operations.
func ExampleThrottler() {
	// Create new throttler with refill for each second
	// and capacity of 20 tokens per bucket.
	thr := NewThrottler(time.Second, 20)
	// Don't forget to close the throttler when you're done!
	defer thr.Close()

	// Take a bucket for Leroy Jenkins.
	bucket := thr.Bucket("leroy_jenkins")

	// Remember to return the bucket to throttler.
	defer thr.Pool(bucket)

	bucket.Take(13)

	go func() {
		// It's safe to retrieve one bucket many times.
		bucket := thr.Bucket("leroy_jenkins")
		defer thr.Pool(bucket)
		bucket.TakeAvailable(37)
	}()

}
