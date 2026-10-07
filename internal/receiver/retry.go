package receiver

import "time"

// RetryInput carries the signals the retry policy evaluates for a failed attempt.
type RetryInput struct {
	ErrorCode       string
	AttemptNumber   int64
	RemainingWindow time.Duration
	LinkHealthy     bool
}

// RetryDecision tells the pass service whether to start another attempt or terminate.
type RetryDecision struct {
	Retry  bool
	Reason string
}

// RetryPolicy decides whether a failed acquisition attempt is retried or terminates
// the pass. The zero value terminates every failure, which keeps services without
// a configured policy on the previous fail-fast behavior.
type RetryPolicy struct {
	RetryableErrors        map[string]bool
	MaxAttempts            int64
	MinRemainingWindow     time.Duration
	MaxConsecutiveFailures int
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		RetryableErrors: map[string]bool{
			"NO-LOCK":      true,
			"CARRIER-LOSS": true,
			"SYNC-TIMEOUT": true,
			"WEAK-SIGNAL":  true,
		},
		MaxAttempts:            3,
		MinRemainingWindow:     2 * time.Minute,
		MaxConsecutiveFailures: 3,
	}
}

// ChainHealthy reports whether a receiver chain with the given trailing failure
// count is still usable for another attempt.
func (p RetryPolicy) ChainHealthy(consecutiveFailures int) bool {
	if p.MaxConsecutiveFailures <= 0 {
		return true
	}
	return consecutiveFailures < p.MaxConsecutiveFailures
}

// Decide evaluates a failed attempt: an unhealthy link, a terminal error code, an
// exhausted attempt budget, or a window too short for another acquisition all
// terminate the pass instead of retrying.
func (p RetryPolicy) Decide(input RetryInput) RetryDecision {
	if !input.LinkHealthy {
		return RetryDecision{Reason: "link-unhealthy"}
	}
	if !p.RetryableErrors[input.ErrorCode] {
		return RetryDecision{Reason: "error-terminal"}
	}
	if input.AttemptNumber >= p.MaxAttempts {
		return RetryDecision{Reason: "max-attempts"}
	}
	if input.RemainingWindow < p.MinRemainingWindow {
		return RetryDecision{Reason: "window-exhausted"}
	}
	return RetryDecision{Retry: true, Reason: "retryable"}
}
