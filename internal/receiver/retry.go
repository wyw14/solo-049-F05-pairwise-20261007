package receiver

import (
	"fmt"
	"time"
)

// Acquisition failure error codes reported by receiver hardware.
const (
	ErrNoCarrier    = "NO-CARRIER"
	ErrSyncTimeout  = "SYNC-TIMEOUT"
	ErrSyncOffset   = "SYNC-OFFSET"
	ErrDopplerDrift = "DOPPLER-DRIFT"
	ErrChainFault   = "CHAIN-FAULT"
	ErrInhibited    = "INHIBITED"
)

// RetrySignals carries the external facts the retry policy evaluates for one delivery.
type RetrySignals struct {
	RemainingWindow time.Duration
	LinkHealthy     bool
}

// RetryPolicy decides whether a failed acquisition attempt is retried or terminated.
type RetryPolicy struct {
	MaxAttempts int64
	MinWindow   time.Duration
	Retryable   map[string]bool
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 8,
		MinWindow:   90 * time.Second,
		Retryable: map[string]bool{
			ErrNoCarrier:    true,
			ErrSyncTimeout:  true,
			ErrSyncOffset:   true,
			ErrDopplerDrift: true,
		},
	}
}

// Decide returns whether the failed attempt may be retried, and the rationale.
func (p RetryPolicy) Decide(attempt Attempt, result AcquisitionResult, signals RetrySignals) (bool, string) {
	if !p.Retryable[result.ErrorCode] {
		return false, fmt.Sprintf("error %s is terminal", result.ErrorCode)
	}
	if !signals.LinkHealthy {
		return false, "link is unhealthy"
	}
	if p.MaxAttempts > 0 && attempt.Number >= p.MaxAttempts {
		return false, fmt.Sprintf("attempt budget %d is exhausted", p.MaxAttempts)
	}
	if signals.RemainingWindow < p.MinWindow {
		return false, "remaining window is below the retry minimum"
	}
	return true, "failure is retryable"
}

type DeliveryKind string

const (
	DeliveryApplied   DeliveryKind = "applied"
	DeliveryStale     DeliveryKind = "stale"
	DeliveryRetry     DeliveryKind = "retry"
	DeliveryTerminate DeliveryKind = "terminate"
)

// DeliveryOutcome describes what a delivered acquisition result changed.
type DeliveryOutcome struct {
	Kind       DeliveryKind `json:"kind"`
	Reason     string       `json:"reason,omitempty"`
	NewAttempt *Attempt     `json:"new_attempt,omitempty"`
}
