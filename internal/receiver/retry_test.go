package receiver

import (
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

func TestRetryPolicyDecide(t *testing.T) {
	policy := DefaultRetryPolicy()
	base := RetryInput{ErrorCode: "NO-LOCK", AttemptNumber: 1, RemainingWindow: 5 * time.Minute, LinkHealthy: true}
	cases := []struct {
		name       string
		policy     RetryPolicy
		mutate     func(*RetryInput)
		wantRetry  bool
		wantReason string
	}{
		{"transient error retries", policy, nil, true, "retryable"},
		{"terminal error code", policy, func(i *RetryInput) { i.ErrorCode = "CONFIG-ERROR" }, false, "error-terminal"},
		{"empty error code", policy, func(i *RetryInput) { i.ErrorCode = "" }, false, "error-terminal"},
		{"unhealthy link", policy, func(i *RetryInput) { i.LinkHealthy = false }, false, "link-unhealthy"},
		{"attempt budget exhausted", policy, func(i *RetryInput) { i.AttemptNumber = policy.MaxAttempts }, false, "max-attempts"},
		{"window too short", policy, func(i *RetryInput) { i.RemainingWindow = policy.MinRemainingWindow - time.Second }, false, "window-exhausted"},
		{"window past LOS", policy, func(i *RetryInput) { i.RemainingWindow = -time.Minute }, false, "window-exhausted"},
		{"zero policy terminates", RetryPolicy{}, nil, false, "error-terminal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := base
			if tc.mutate != nil {
				tc.mutate(&input)
			}
			decision := tc.policy.Decide(input)
			if decision.Retry != tc.wantRetry || decision.Reason != tc.wantReason {
				t.Fatalf("decide = %+v, want retry=%v reason=%q", decision, tc.wantRetry, tc.wantReason)
			}
		})
	}
}

func TestRetryPolicyChainHealth(t *testing.T) {
	policy := DefaultRetryPolicy()
	if !policy.ChainHealthy(policy.MaxConsecutiveFailures - 1) {
		t.Fatal("chain below the failure limit should be healthy")
	}
	if policy.ChainHealthy(policy.MaxConsecutiveFailures) {
		t.Fatal("chain at the failure limit should be unhealthy")
	}
	if !(RetryPolicy{}).ChainHealthy(100) {
		t.Fatal("unconfigured policy should not limit chain health")
	}
}

type fakeSink struct{ results []AcquisitionResult }

func (f *fakeSink) ApplyAcquisitionResult(result AcquisitionResult) error {
	f.results = append(f.results, result)
	return nil
}

func newTestDispatcher(t *testing.T) (*AttemptDispatcher, *fakeSink) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	dispatcher, err := NewAttemptDispatcher(paths, audit.NewTrail(store.NewAppendLog(paths)))
	if err != nil {
		t.Fatal(err)
	}
	sink := &fakeSink{}
	dispatcher.SetSink(sink)
	return dispatcher, sink
}

func TestConsecutiveFailuresTracksChainOutcomes(t *testing.T) {
	dispatcher, _ := newTestDispatcher(t)
	first, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: first.ID, ChainID: "RF-01", ErrorCode: "NO-LOCK"}); err != nil {
		t.Fatal(err)
	}
	second, err := dispatcher.Begin("pass-2", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if got := dispatcher.ConsecutiveFailures("RF-01"); got != 1 {
		t.Fatalf("one trailing failure expected, got %d", got)
	}
	if err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-2", AttemptID: second.ID, ChainID: "RF-01", ErrorCode: "CARRIER-LOSS"}); err != nil {
		t.Fatal(err)
	}
	if got := dispatcher.ConsecutiveFailures("RF-01"); got != 2 {
		t.Fatalf("two trailing failures expected, got %d", got)
	}
	third, err := dispatcher.Begin("pass-3", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if got := dispatcher.ConsecutiveFailures("RF-01"); got != 2 {
		t.Fatalf("pending attempt must not change the streak, got %d", got)
	}
	if err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-3", AttemptID: third.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 4}); err != nil {
		t.Fatal(err)
	}
	if got := dispatcher.ConsecutiveFailures("RF-01"); got != 0 {
		t.Fatalf("lock should reset the streak, got %d", got)
	}
	if got := dispatcher.ConsecutiveFailures("RF-02"); got != 0 {
		t.Fatalf("other chains must be unaffected, got %d", got)
	}
}

func TestRetryPreservesFailedAttemptAndDropsLateResults(t *testing.T) {
	dispatcher, sink := newTestDispatcher(t)
	first, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Deliver(AcquisitionResult{PassID: "pass-1", AttemptID: first.ID, ChainID: "RF-01", ErrorCode: "NO-LOCK"}); err != nil {
		t.Fatal(err)
	}
	second, err := dispatcher.Begin("pass-1", "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if second.Number != first.Number+1 {
		t.Fatalf("retry should increment the attempt number: %d -> %d", first.Number, second.Number)
	}
	preserved := findAttempt(t, dispatcher, first.ID)
	if preserved.Status != AttemptFailed || preserved.ErrorCode != "NO-LOCK" || preserved.FinishedAt.IsZero() {
		t.Fatalf("previous attempt result not preserved: %+v", preserved)
	}
	deliveries := len(sink.results)
	late := AcquisitionResult{PassID: "pass-1", AttemptID: first.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 9}
	if err := dispatcher.Deliver(late); err != nil {
		t.Fatal(err)
	}
	if len(sink.results) != deliveries {
		t.Fatal("late result from a superseded attempt reached the sink")
	}
	current, found := dispatcher.Current("pass-1")
	if !found || current.ID != second.ID || current.Status != AttemptPending {
		t.Fatalf("late result changed the current attempt: %+v", current)
	}
	preserved = findAttempt(t, dispatcher, first.ID)
	if preserved.Status != AttemptFailed || preserved.ErrorCode != "NO-LOCK" {
		t.Fatalf("late result overwrote the previous attempt: %+v", preserved)
	}
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
