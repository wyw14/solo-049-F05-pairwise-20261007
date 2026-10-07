package receiver

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

type AttemptDispatcher struct {
	mu       sync.RWMutex
	path     string
	attempts map[string]Attempt
	current  map[string]string
	policy   RetryPolicy
	sink     ResultSink
	audit    *audit.Trail
}

func NewAttemptDispatcher(paths store.Paths, trail *audit.Trail) (*AttemptDispatcher, error) {
	dispatcher := &AttemptDispatcher{
		path: paths.Snapshot("receiver-attempts"), attempts: make(map[string]Attempt),
		current: make(map[string]string), policy: DefaultRetryPolicy(), audit: trail,
	}
	var attempts []Attempt
	found, err := store.ReadJSON(dispatcher.path, &attempts)
	if err != nil {
		return nil, err
	}
	if found {
		for _, attempt := range attempts {
			dispatcher.attempts[attempt.ID] = attempt
			if current, ok := dispatcher.attempts[dispatcher.current[attempt.PassID]]; !ok || attempt.Number > current.Number {
				dispatcher.current[attempt.PassID] = attempt.ID
			}
		}
	}
	return dispatcher, nil
}

func (d *AttemptDispatcher) SetSink(sink ResultSink) {
	d.mu.Lock()
	d.sink = sink
	d.mu.Unlock()
}

func (d *AttemptDispatcher) SetPolicy(policy RetryPolicy) {
	d.mu.Lock()
	d.policy = policy
	d.mu.Unlock()
}

func (d *AttemptDispatcher) Begin(passID, chainID string) (Attempt, error) {
	if passID == "" || chainID == "" {
		return Attempt{}, fmt.Errorf("pass and receiver chain are required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	number := int64(1)
	if previousID := d.current[passID]; previousID != "" {
		number = d.attempts[previousID].Number + 1
	}
	attempt := Attempt{ID: uuid.NewString(), PassID: passID, ChainID: chainID, Number: number, Status: AttemptPending, StartedAt: time.Now().UTC()}
	d.attempts[attempt.ID] = attempt
	d.current[passID] = attempt.ID
	if err := d.flushLocked(); err != nil {
		return Attempt{}, err
	}
	_, _ = d.audit.Record(audit.Event{Component: "receiver", Action: "attempt.started", Subject: attempt.ID, Fields: map[string]any{"pass_id": passID, "number": number}})
	return attempt, nil
}

func (d *AttemptDispatcher) Deliver(result AcquisitionResult, signals RetrySignals) (DeliveryOutcome, error) {
	d.mu.Lock()
	attempt, found := d.attempts[result.AttemptID]
	if !found || attempt.PassID != result.PassID {
		d.mu.Unlock()
		return DeliveryOutcome{}, fmt.Errorf("receiver attempt does not match result")
	}
	current := d.current[result.PassID]
	if current != result.AttemptID {
		_, _ = d.audit.Record(audit.Event{Component: "receiver", Action: "result.stale", Subject: result.AttemptID, Severity: audit.Warning, Fields: map[string]any{"current_attempt": current, "error_code": result.ErrorCode}})
		d.mu.Unlock()
		return DeliveryOutcome{Kind: DeliveryStale}, nil
	}
	locked := result.Locked && result.Synced
	if locked {
		attempt.Status = AttemptLocked
		attempt.Epoch = result.Epoch
	} else {
		attempt.Status = AttemptFailed
		attempt.ErrorCode = result.ErrorCode
	}
	attempt.FinishedAt = time.Now().UTC()
	d.attempts[attempt.ID] = attempt
	outcome := DeliveryOutcome{Kind: DeliveryApplied}
	if !locked {
		retry, reason := d.policy.Decide(attempt, result, signals)
		if retry {
			next := Attempt{
				ID: uuid.NewString(), PassID: attempt.PassID, ChainID: attempt.ChainID,
				Number: attempt.Number + 1, Status: AttemptPending, RetryOf: attempt.ID,
				StartedAt: time.Now().UTC(),
			}
			d.attempts[next.ID] = next
			d.current[next.PassID] = next.ID
			outcome = DeliveryOutcome{Kind: DeliveryRetry, Reason: reason, NewAttempt: &next}
			_, _ = d.audit.Record(audit.Event{Component: "receiver", Action: "attempt.retry", Subject: next.ID, Fields: map[string]any{"pass_id": next.PassID, "previous_attempt": attempt.ID, "number": next.Number, "error_code": result.ErrorCode, "reason": reason, "remaining_window_ms": signals.RemainingWindow.Milliseconds()}})
		} else {
			outcome = DeliveryOutcome{Kind: DeliveryTerminate, Reason: reason}
			_, _ = d.audit.Record(audit.Event{Component: "receiver", Action: "attempt.terminated", Subject: attempt.ID, Severity: audit.Warning, Fields: map[string]any{"pass_id": attempt.PassID, "error_code": result.ErrorCode, "reason": reason}})
		}
	}
	sink := d.sink
	err := d.flushLocked()
	d.mu.Unlock()
	if err != nil {
		return DeliveryOutcome{}, err
	}
	if outcome.Kind == DeliveryApplied || outcome.Kind == DeliveryTerminate {
		if sink == nil {
			return DeliveryOutcome{}, fmt.Errorf("receiver result sink is not configured")
		}
		return outcome, sink.ApplyAcquisitionResult(result)
	}
	return outcome, nil
}

func (d *AttemptDispatcher) Current(passID string) (Attempt, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	id := d.current[passID]
	attempt, found := d.attempts[id]
	return attempt, found
}

func (d *AttemptDispatcher) List() []Attempt {
	d.mu.RLock()
	defer d.mu.RUnlock()
	values := make([]Attempt, 0, len(d.attempts))
	for _, attempt := range d.attempts {
		values = append(values, attempt)
	}
	return values
}

func (d *AttemptDispatcher) flushLocked() error {
	values := make([]Attempt, 0, len(d.attempts))
	for _, attempt := range d.attempts {
		values = append(values, attempt)
	}
	return store.WriteJSON(d.path, values)
}
