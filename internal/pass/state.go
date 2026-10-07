package pass

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/store"
)

// AttemptCoordinator supplies attempt lineage and starts replacement attempts
// when the retry policy asks for one. *receiver.AttemptDispatcher implements it.
type AttemptCoordinator interface {
	Current(passID string) (receiver.Attempt, bool)
	Begin(passID, chainID string) (receiver.Attempt, error)
	ConsecutiveFailures(chainID string) int
}

type Service struct {
	mu          sync.RWMutex
	path        string
	passes      map[string]Pass
	recorder    *recorder.Manager
	audit       *audit.Trail
	attempts    AttemptCoordinator
	retryPolicy receiver.RetryPolicy
}

func NewService(paths store.Paths, manager *recorder.Manager, trail *audit.Trail) (*Service, error) {
	service := &Service{path: paths.Snapshot("passes"), passes: make(map[string]Pass), recorder: manager, audit: trail}
	var passes []Pass
	found, err := store.ReadJSON(service.path, &passes)
	if err != nil {
		return nil, err
	}
	if found {
		for _, value := range passes {
			service.passes[value.ID] = value
		}
	}
	return service, nil
}

func (s *Service) Create(request CreateRequest) (Pass, error) {
	value := Pass{
		ID: uuid.NewString(), SatelliteID: request.SatelliteID, AntennaID: request.AntennaID,
		RFChains:               append([]string(nil), request.RFChains...),
		RequiredReceiverChains: append([]string(nil), request.RequiredReceiverChains...),
		Window:                 Window{AOS: request.AOS, LOS: request.LOS, Revision: request.Revision},
		Phase:                  Planned, OwnerStation: request.OwnerStation, OwnerGeneration: 1,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := value.Validate(); err != nil {
		return Pass{}, err
	}
	s.mu.Lock()
	s.passes[value.ID] = value
	if err := s.flushLocked(); err != nil {
		delete(s.passes, value.ID)
		s.mu.Unlock()
		return Pass{}, err
	}
	s.mu.Unlock()
	_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "created", Subject: value.ID, Fields: map[string]any{"satellite_id": value.SatelliteID, "revision": value.Window.Revision}})
	return value, nil
}

func (s *Service) Get(id string) (Pass, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, found := s.passes[id]
	return value, found
}

// ConfigureRetry installs the retry policy and the attempt coordinator used to
// start replacement attempts. Without a coordinator, failed acquisitions keep
// terminating the pass immediately.
func (s *Service) ConfigureRetry(coordinator AttemptCoordinator, policy receiver.RetryPolicy) {
	s.mu.Lock()
	s.attempts = coordinator
	s.retryPolicy = policy
	s.mu.Unlock()
}

func (s *Service) List() []Pass {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]Pass, 0, len(s.passes))
	for _, value := range s.passes {
		values = append(values, value)
	}
	return values
}

func (s *Service) SetReservation(passID, reservationID string, generation int64) error {
	return s.update(passID, func(value *Pass) error {
		value.ReservationID = reservationID
		value.ReservationGeneration = generation
		return nil
	})
}

func (s *Service) BeginAcquisition(passID, attemptID string) error {
	return s.update(passID, func(value *Pass) error {
		if value.Phase != Planned && value.Phase != Acquiring {
			return fmt.Errorf("pass cannot acquire from phase %s", value.Phase)
		}
		value.Phase = Acquiring
		value.CurrentAttemptID = attemptID
		value.FailureReason = ""
		return nil
	})
}

func (s *Service) ApplyAcquisitionResult(result receiver.AcquisitionResult) error {
	s.mu.Lock()
	value, found := s.passes[result.PassID]
	if !found {
		s.mu.Unlock()
		return fmt.Errorf("pass %s not found", result.PassID)
	}
	if value.CurrentAttemptID != result.AttemptID {
		s.mu.Unlock()
		_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "stale-acquisition.ignored", Subject: result.PassID, Severity: audit.Warning, Fields: map[string]any{"attempt_id": result.AttemptID, "current_attempt": value.CurrentAttemptID}})
		return nil
	}
	if result.Locked && result.Synced {
		value.Phase = Tracking
		value.DecoderEpoch = result.Epoch
		value.UpdatedAt = time.Now().UTC()
		s.passes[value.ID] = value
		err := s.flushLocked()
		s.mu.Unlock()
		return err
	}
	reason := "retry-disabled"
	if s.attempts != nil {
		reason = "not-acquiring"
		if value.Phase == Acquiring {
			current, ok := s.attempts.Current(result.PassID)
			if !ok || current.ID != result.AttemptID {
				// The dispatcher already moved to a newer attempt; this result
				// arrived too late and must not change the pass.
				s.mu.Unlock()
				_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "stale-acquisition.ignored", Subject: result.PassID, Severity: audit.Warning, Fields: map[string]any{"attempt_id": result.AttemptID, "current_attempt": current.ID}})
				return nil
			}
			decision := s.retryPolicy.Decide(receiver.RetryInput{
				ErrorCode:       result.ErrorCode,
				AttemptNumber:   current.Number,
				RemainingWindow: time.Until(value.Window.LOS),
				LinkHealthy:     s.retryPolicy.ChainHealthy(s.attempts.ConsecutiveFailures(result.ChainID)),
			})
			if decision.Retry {
				attempt, err := s.retryLocked(&value, result)
				s.mu.Unlock()
				if err != nil {
					return err
				}
				_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "acquisition.retry", Subject: value.ID, Fields: map[string]any{"previous_attempt": result.AttemptID, "new_attempt": attempt.ID, "attempt_number": attempt.Number, "chain_id": result.ChainID, "error_code": result.ErrorCode}})
				return nil
			}
			reason = decision.Reason
		}
	}
	value.Phase = Failed
	value.FailureReason = result.ErrorCode
	value.UpdatedAt = time.Now().UTC()
	s.passes[value.ID] = value
	err := s.flushLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	_, _ = s.audit.Record(audit.Event{Component: "pass", Action: "acquisition.terminated", Subject: value.ID, Severity: audit.Warning, Fields: map[string]any{"attempt_id": result.AttemptID, "error_code": result.ErrorCode, "reason": reason}})
	return s.recorder.StopForFailure(value.ID, result.AttemptID, result.ErrorCode)
}

// retryLocked starts a replacement attempt and re-arms the pass for acquisition.
// The failed attempt keeps its delivered result in the dispatcher; the pass only
// moves its current pointer, so a late result for the old attempt stays stale.
func (s *Service) retryLocked(value *Pass, result receiver.AcquisitionResult) (receiver.Attempt, error) {
	attempt, err := s.attempts.Begin(result.PassID, result.ChainID)
	if err != nil {
		return receiver.Attempt{}, err
	}
	value.Phase = Acquiring
	value.CurrentAttemptID = attempt.ID
	value.FailureReason = ""
	value.UpdatedAt = time.Now().UTC()
	s.passes[value.ID] = *value
	if err := s.flushLocked(); err != nil {
		return receiver.Attempt{}, err
	}
	return attempt, nil
}

func (s *Service) AcceptDiversity(passID string, results []receiver.AcquisitionResult) (receiver.DiversityDecision, error) {
	s.mu.RLock()
	value, found := s.passes[passID]
	s.mu.RUnlock()
	if !found {
		return receiver.DiversityDecision{}, fmt.Errorf("pass %s not found", passID)
	}
	decision, err := (receiver.DiversityPolicy{RequiredChains: value.RequiredReceiverChains}).Evaluate(results)
	if err != nil {
		return decision, err
	}
	if !decision.Ready {
		return decision, nil
	}
	if err := s.update(passID, func(current *Pass) error {
		if current.Phase != Acquiring {
			return fmt.Errorf("pass cannot enter tracking from %s", current.Phase)
		}
		current.Phase = Tracking
		current.DecoderEpoch = decision.Epoch
		return nil
	}); err != nil {
		return decision, err
	}
	segment, err := s.recorder.Start(passID, value.OwnerStation, value.OwnerGeneration)
	if err != nil {
		return decision, err
	}
	if err := s.update(passID, func(current *Pass) error {
		current.RecordingSegmentID = segment.ID
		return nil
	}); err != nil {
		return decision, err
	}
	return decision, nil
}

func (s *Service) Fail(passID, reason string) error {
	return s.update(passID, func(value *Pass) error {
		value.Phase = Failed
		value.FailureReason = reason
		return nil
	})
}

func (s *Service) Cancel(passID, reason string) error {
	return s.update(passID, func(value *Pass) error {
		if value.Phase == Completed || value.Phase == Sealed {
			return fmt.Errorf("completed pass cannot be canceled")
		}
		value.Phase = Canceled
		value.FailureReason = reason
		return nil
	})
}

func (s *Service) update(passID string, apply func(*Pass) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, found := s.passes[passID]
	if !found {
		return fmt.Errorf("pass %s not found", passID)
	}
	if err := apply(&value); err != nil {
		return err
	}
	value.UpdatedAt = time.Now().UTC()
	s.passes[passID] = value
	return s.flushLocked()
}

func (s *Service) flushLocked() error {
	values := make([]Pass, 0, len(s.passes))
	for _, value := range s.passes {
		values = append(values, value)
	}
	return store.WriteJSON(s.path, values)
}
