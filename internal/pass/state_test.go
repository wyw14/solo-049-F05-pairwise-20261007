package pass

import (
	"fmt"
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/store"
)

type fakeCoordinator struct {
	current  receiver.Attempt
	found    bool
	failures int
	began    []string
}

func (f *fakeCoordinator) Current(string) (receiver.Attempt, bool) { return f.current, f.found }

func (f *fakeCoordinator) Begin(passID, chainID string) (receiver.Attempt, error) {
	f.began = append(f.began, chainID)
	f.current = receiver.Attempt{
		ID: fmt.Sprintf("retry-%d", len(f.began)), PassID: passID, ChainID: chainID,
		Number: f.current.Number + 1, Status: receiver.AttemptPending, StartedAt: time.Now().UTC(),
	}
	f.found = true
	return f.current, nil
}

func (f *fakeCoordinator) ConsecutiveFailures(string) int { return f.failures }

func newTestService(t *testing.T) *Service {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	manager, err := recorder.NewManager(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(paths, manager, trail)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func newAcquiringPass(t *testing.T, service *Service, los time.Time) Pass {
	t.Helper()
	value, err := service.Create(CreateRequest{
		SatelliteID: "SAT-1", AntennaID: "ANT-1", RFChains: []string{"RF-01"},
		RequiredReceiverChains: []string{"RF-01"}, AOS: los.Add(-10 * time.Minute), LOS: los,
		Revision: 1, OwnerStation: "north",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.BeginAcquisition(value.ID, "attempt-1"); err != nil {
		t.Fatal(err)
	}
	value, found := service.Get(value.ID)
	if !found {
		t.Fatal("pass disappeared")
	}
	return value
}

func TestAcquisitionFailureRetriesWithFreshAttempt(t *testing.T) {
	service := newTestService(t)
	coordinator := &fakeCoordinator{found: true}
	service.ConfigureRetry(coordinator, receiver.DefaultRetryPolicy())
	value := newAcquiringPass(t, service, time.Now().UTC().Add(8*time.Minute))
	coordinator.current = receiver.Attempt{ID: "attempt-1", PassID: value.ID, ChainID: "RF-01", Number: 1, Status: receiver.AttemptFailed}

	result := receiver.AcquisitionResult{PassID: value.ID, AttemptID: "attempt-1", ChainID: "RF-01", ErrorCode: "NO-LOCK"}
	if err := service.ApplyAcquisitionResult(result); err != nil {
		t.Fatal(err)
	}
	updated, _ := service.Get(value.ID)
	if updated.Phase != Acquiring {
		t.Fatalf("retry should keep the pass acquiring, got %s", updated.Phase)
	}
	if updated.CurrentAttemptID == "attempt-1" || updated.CurrentAttemptID == "" {
		t.Fatalf("retry did not move the current pointer: %q", updated.CurrentAttemptID)
	}
	if len(coordinator.began) != 1 || coordinator.began[0] != "RF-01" {
		t.Fatalf("retry should begin one new attempt on the same chain, got %v", coordinator.began)
	}
	if updated.FailureReason != "" {
		t.Fatalf("retry should clear the failure reason, got %q", updated.FailureReason)
	}
}

func TestAcquisitionFailureTerminates(t *testing.T) {
	future := time.Now().UTC().Add(8 * time.Minute)
	cases := []struct {
		name      string
		policy    receiver.RetryPolicy
		errorCode string
		los       time.Time
		failures  int
	}{
		{"terminal error code", receiver.DefaultRetryPolicy(), "CONFIG-ERROR", future, 0},
		{"window exhausted", receiver.DefaultRetryPolicy(), "NO-LOCK", time.Now().UTC().Add(30 * time.Second), 0},
		{"link unhealthy", receiver.DefaultRetryPolicy(), "NO-LOCK", future, 3},
		{"zero policy terminates", receiver.RetryPolicy{}, "NO-LOCK", future, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service := newTestService(t)
			coordinator := &fakeCoordinator{found: true, failures: tc.failures}
			service.ConfigureRetry(coordinator, tc.policy)
			value := newAcquiringPass(t, service, tc.los)
			coordinator.current = receiver.Attempt{ID: "attempt-1", PassID: value.ID, ChainID: "RF-01", Number: 1}

			result := receiver.AcquisitionResult{PassID: value.ID, AttemptID: "attempt-1", ChainID: "RF-01", ErrorCode: tc.errorCode}
			if err := service.ApplyAcquisitionResult(result); err != nil {
				t.Fatal(err)
			}
			updated, _ := service.Get(value.ID)
			if updated.Phase != Failed {
				t.Fatalf("expected termination, got phase %s", updated.Phase)
			}
			if updated.FailureReason != tc.errorCode {
				t.Fatalf("failure reason = %q, want %q", updated.FailureReason, tc.errorCode)
			}
			if len(coordinator.began) != 0 {
				t.Fatalf("termination must not begin new attempts: %v", coordinator.began)
			}
		})
	}
}

func TestAcquisitionFailureTerminatesWhenAttemptBudgetSpent(t *testing.T) {
	service := newTestService(t)
	coordinator := &fakeCoordinator{found: true}
	service.ConfigureRetry(coordinator, receiver.DefaultRetryPolicy())
	value := newAcquiringPass(t, service, time.Now().UTC().Add(8*time.Minute))
	coordinator.current = receiver.Attempt{ID: "attempt-1", PassID: value.ID, ChainID: "RF-01", Number: receiver.DefaultRetryPolicy().MaxAttempts}

	result := receiver.AcquisitionResult{PassID: value.ID, AttemptID: "attempt-1", ChainID: "RF-01", ErrorCode: "NO-LOCK"}
	if err := service.ApplyAcquisitionResult(result); err != nil {
		t.Fatal(err)
	}
	updated, _ := service.Get(value.ID)
	if updated.Phase != Failed || len(coordinator.began) != 0 {
		t.Fatalf("exhausted attempt budget should terminate: phase=%s began=%v", updated.Phase, coordinator.began)
	}
}

func TestLateResultCannotMovePassAfterRetry(t *testing.T) {
	service := newTestService(t)
	coordinator := &fakeCoordinator{found: true}
	service.ConfigureRetry(coordinator, receiver.DefaultRetryPolicy())
	value := newAcquiringPass(t, service, time.Now().UTC().Add(8*time.Minute))
	coordinator.current = receiver.Attempt{ID: "attempt-1", PassID: value.ID, ChainID: "RF-01", Number: 1}

	failure := receiver.AcquisitionResult{PassID: value.ID, AttemptID: "attempt-1", ChainID: "RF-01", ErrorCode: "NO-LOCK"}
	if err := service.ApplyAcquisitionResult(failure); err != nil {
		t.Fatal(err)
	}
	retried, _ := service.Get(value.ID)
	if retried.Phase != Acquiring || retried.CurrentAttemptID == "attempt-1" {
		t.Fatalf("expected a retried acquiring pass, got %+v", retried)
	}
	lateLock := receiver.AcquisitionResult{PassID: value.ID, AttemptID: "attempt-1", ChainID: "RF-01", Locked: true, Synced: true, Epoch: 11}
	if err := service.ApplyAcquisitionResult(lateLock); err != nil {
		t.Fatal(err)
	}
	after, _ := service.Get(value.ID)
	if after.Phase != retried.Phase || after.CurrentAttemptID != retried.CurrentAttemptID || after.DecoderEpoch != 0 {
		t.Fatalf("late lock mutated the pass: before=%+v after=%+v", retried, after)
	}
	lateFailure := receiver.AcquisitionResult{PassID: value.ID, AttemptID: "attempt-1", ChainID: "RF-01", ErrorCode: "CARRIER-LOSS"}
	if err := service.ApplyAcquisitionResult(lateFailure); err != nil {
		t.Fatal(err)
	}
	after, _ = service.Get(value.ID)
	if after.Phase != retried.Phase || after.CurrentAttemptID != retried.CurrentAttemptID || after.FailureReason != "" {
		t.Fatalf("late failure mutated the pass: before=%+v after=%+v", retried, after)
	}
	if len(coordinator.began) != 1 {
		t.Fatalf("late results must not trigger more retries: %v", coordinator.began)
	}
}

func TestResultWithUnknownAttemptLineageIsIgnored(t *testing.T) {
	service := newTestService(t)
	coordinator := &fakeCoordinator{found: false}
	service.ConfigureRetry(coordinator, receiver.DefaultRetryPolicy())
	value := newAcquiringPass(t, service, time.Now().UTC().Add(8*time.Minute))

	result := receiver.AcquisitionResult{PassID: value.ID, AttemptID: "attempt-1", ChainID: "RF-01", ErrorCode: "NO-LOCK"}
	if err := service.ApplyAcquisitionResult(result); err != nil {
		t.Fatal(err)
	}
	updated, _ := service.Get(value.ID)
	if updated.Phase != Acquiring || updated.CurrentAttemptID != "attempt-1" {
		t.Fatalf("result without attempt lineage should be ignored: %+v", updated)
	}
	if len(coordinator.began) != 0 {
		t.Fatalf("no retry should start for unknown lineage: %v", coordinator.began)
	}
}
