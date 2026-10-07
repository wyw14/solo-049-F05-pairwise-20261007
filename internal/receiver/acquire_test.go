package receiver

import (
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

type recordingSink struct {
	results []AcquisitionResult
}

func (s *recordingSink) ApplyAcquisitionResult(result AcquisitionResult) error {
	s.results = append(s.results, result)
	return nil
}

func newTestDispatcher(t *testing.T) (*AttemptDispatcher, *recordingSink) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	dispatcher, err := NewAttemptDispatcher(paths, audit.NewTrail(store.NewAppendLog(paths)))
	if err != nil {
		t.Fatal(err)
	}
	sink := &recordingSink{}
	dispatcher.SetSink(sink)
	return dispatcher, sink
}

func healthySignals() RetrySignals {
	return RetrySignals{RemainingWindow: 5 * time.Minute, LinkHealthy: true}
}

func findAttempt(t *testing.T, dispatcher *AttemptDispatcher, id string) Attempt {
	t.Helper()
	for _, attempt := range dispatcher.List() {
		if attempt.ID == id {
			return attempt
		}
	}
	t.Fatalf("attempt %s not found", id)
	return Attempt{}
}

func TestRetryableFailureSpawnsAttemptAndPreservesPriorResult(t *testing.T) {
	dispatcher, sink := newTestDispatcher(t)
	first, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: first.ID, ChainID: "RF-01", ErrorCode: ErrNoCarrier}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != DeliveryRetry || outcome.NewAttempt == nil {
		t.Fatalf("expected retry with a new attempt, got %#v", outcome)
	}
	next := *outcome.NewAttempt
	if next.Number != first.Number+1 || next.RetryOf != first.ID || next.Status != AttemptPending {
		t.Fatalf("retry attempt does not extend the chain: %#v", next)
	}
	stored := findAttempt(t, dispatcher, first.ID)
	if stored.Status != AttemptFailed || stored.ErrorCode != ErrNoCarrier || stored.FinishedAt.IsZero() {
		t.Fatalf("prior attempt result was not preserved: %#v", stored)
	}
	current, found := dispatcher.Current("pass-1")
	if !found || current.ID != next.ID {
		t.Fatalf("current pointer did not advance to %s: %#v", next.ID, current)
	}
	if len(sink.results) != 0 {
		t.Fatalf("retried failure must not reach the pass sink: %#v", sink.results)
	}
}

func TestRetryChainAdvancesAcrossFailures(t *testing.T) {
	dispatcher, _ := newTestDispatcher(t)
	first, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	second, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: first.ID, ChainID: "RF-01", ErrorCode: ErrSyncTimeout}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	third, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: second.NewAttempt.ID, ChainID: "RF-01", ErrorCode: ErrDopplerDrift}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	if third.Kind != DeliveryRetry || third.NewAttempt.Number != 3 || third.NewAttempt.RetryOf != second.NewAttempt.ID {
		t.Fatalf("retry chain did not advance: %#v", third)
	}
	if len(dispatcher.List()) != 3 {
		t.Fatalf("expected 3 preserved attempts, got %d", len(dispatcher.List()))
	}
}

func TestTerminalErrorCodeTerminatesAttempt(t *testing.T) {
	dispatcher, sink := newTestDispatcher(t)
	attempt, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: attempt.ID, ChainID: "RF-01", ErrorCode: ErrChainFault}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != DeliveryTerminate || outcome.NewAttempt != nil {
		t.Fatalf("expected terminate, got %#v", outcome)
	}
	if len(sink.results) != 1 || sink.results[0].ErrorCode != ErrChainFault {
		t.Fatalf("terminal failure did not reach the pass sink: %#v", sink.results)
	}
	if len(dispatcher.List()) != 1 {
		t.Fatal("terminate must not spawn a retry attempt")
	}
}

func TestUnhealthyLinkTerminatesAttempt(t *testing.T) {
	dispatcher, _ := newTestDispatcher(t)
	attempt, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	signals := healthySignals()
	signals.LinkHealthy = false
	outcome, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: attempt.ID, ChainID: "RF-01", ErrorCode: ErrNoCarrier}, signals)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != DeliveryTerminate {
		t.Fatalf("unhealthy link must terminate, got %#v", outcome)
	}
}

func TestShortRemainingWindowTerminatesAttempt(t *testing.T) {
	dispatcher, _ := newTestDispatcher(t)
	attempt, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	signals := healthySignals()
	signals.RemainingWindow = 10 * time.Second
	outcome, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: attempt.ID, ChainID: "RF-01", ErrorCode: ErrNoCarrier}, signals)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != DeliveryTerminate {
		t.Fatalf("short window must terminate, got %#v", outcome)
	}
}

func TestAttemptBudgetTerminatesAttempt(t *testing.T) {
	dispatcher, _ := newTestDispatcher(t)
	dispatcher.SetPolicy(RetryPolicy{MaxAttempts: 1, MinWindow: time.Second, Retryable: map[string]bool{ErrNoCarrier: true}})
	attempt, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: attempt.ID, ChainID: "RF-01", ErrorCode: ErrNoCarrier}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != DeliveryTerminate {
		t.Fatalf("exhausted budget must terminate, got %#v", outcome)
	}
}

func TestLateResultCannotChangeCurrentAttempt(t *testing.T) {
	dispatcher, sink := newTestDispatcher(t)
	first, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: first.ID, ChainID: "RF-01", ErrorCode: ErrNoCarrier}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	next := outcome.NewAttempt
	late, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: first.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 9}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	if late.Kind != DeliveryStale {
		t.Fatalf("late lock on a superseded attempt must be stale, got %#v", late)
	}
	current, _ := dispatcher.Current("pass-1")
	if current.ID != next.ID || current.Status != AttemptPending {
		t.Fatalf("late result changed the current attempt: %#v", current)
	}
	lateFailure, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: first.ID, ChainID: "RF-01", ErrorCode: ErrChainFault}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	if lateFailure.Kind != DeliveryStale || len(dispatcher.List()) != 2 {
		t.Fatalf("late failure spawned an extra retry: %#v", lateFailure)
	}
	if len(sink.results) != 0 {
		t.Fatalf("stale results must not reach the pass sink: %#v", sink.results)
	}
}

func TestSuccessfulDeliveryApplies(t *testing.T) {
	dispatcher, sink := newTestDispatcher(t)
	attempt, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7}, healthySignals())
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != DeliveryApplied {
		t.Fatalf("expected applied, got %#v", outcome)
	}
	stored := findAttempt(t, dispatcher, attempt.ID)
	if stored.Status != AttemptLocked || stored.Epoch != 7 {
		t.Fatalf("lock was not recorded: %#v", stored)
	}
	if len(sink.results) != 1 {
		t.Fatalf("success did not reach the pass sink: %#v", sink.results)
	}
}
