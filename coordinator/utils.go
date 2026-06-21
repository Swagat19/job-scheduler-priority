package main

import (
	"math"
	"math/rand"
	"time"
)

// PriorityScoreMultiplier separates priority bands in the Redis sorted set
// score. priority * 1e13 + unix_millis stays well within float64 integer
// precision (2^53 ≈ 9e15) for ~317 years. Must match the value used by the
// submitter so retries and fresh submissions live in the same band.
const PriorityScoreMultiplier = 1e13

// priorityScore returns the sorted-set score for a job at the given priority.
// Lower scores pop first via BZPOPMIN: smaller priority value = higher
// dispatch priority, with FIFO ordering within each priority band.
//
// On retries the current timestamp is used so requeued jobs go to the back
// of their priority band rather than starving fresh submissions in the
// same band.
func priorityScore(priority int) float64 {
	return float64(priority)*PriorityScoreMultiplier + float64(time.Now().UnixMilli())
}

func calculateBackoffDelay(retryCount int) time.Duration {
	// Exponentail Backoff: 2 ^ retryCount seconds with jitter
	baseDelay := time.Duration(math.Pow(2, float64(retryCount))) * time.Second

	// Add jitter to prevent thundering herd
	// Having fixed delay may cause multiple failure jobs access same resources at exactly same time
	// This causes a sudden spike and it can overwhelm the system.
	jitter := time.Duration(rand.Intn(1000)) * time.Millisecond

	// Cap at maximum delay
	maxDelay := 60 * time.Second
	if baseDelay > maxDelay {
		baseDelay = maxDelay
	}

	return baseDelay + jitter
}
