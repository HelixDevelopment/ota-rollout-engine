package rollout

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Configuration / validation errors.
var (
	// ErrNoPhases is returned when a rollout is created with an empty phase list.
	ErrNoPhases = errors.New("rollout: at least one phase is required")
	// ErrEmptyDeploymentID is returned when a rollout has no deployment id.
	ErrEmptyDeploymentID = errors.New("rollout: deployment id must not be empty")
	// ErrPercentageRange is returned when a phase percentage is outside (0,100].
	ErrPercentageRange = errors.New("rollout: phase percentage must be in (0,100]")
	// ErrPercentageNotMonotonic is returned when phase percentages are not
	// strictly increasing (cohorts must only grow across ordered phases).
	ErrPercentageNotMonotonic = errors.New("rollout: phase percentages must strictly increase")
	// ErrThresholdRange is returned when a success/error threshold is not a
	// fraction in [0,1].
	ErrThresholdRange = errors.New("rollout: thresholds must be fractions in [0,1]")
	// ErrDurationNegative is returned when a phase duration is negative.
	ErrDurationNegative = errors.New("rollout: phase duration must not be negative")
	// ErrFinalPercentageNot100 is returned when the last phase does not reach
	// 100% — a rollout must be able to converge on the whole fleet.
	ErrFinalPercentageNot100 = errors.New("rollout: final phase percentage must be 100")
)

// Phase is one ordered step of a staged rollout (spec 1.0.1 §2;
// telemetry_processing §5).
//
// Percentage is the CUMULATIVE fraction of the fleet (1..100) covered once this
// phase is reached — phases are strictly increasing, so phase N's cohort is a
// superset of phase N-1's. SuccessThreshold and ErrorThreshold are fractions in
// [0,1] compared against the cohort's success_rate / error_rate. Duration is the
// evaluation window; AutoProgress controls whether meeting the success bar
// advances automatically (true) or holds for an operator (false).
type Phase struct {
	// Percentage is the cumulative cohort percentage for this phase, in (0,100].
	Percentage int
	// SuccessThreshold is the success_rate (fraction, [0,1]) required to advance.
	SuccessThreshold float64
	// ErrorThreshold is the error_rate (fraction, [0,1]) that triggers a halt.
	ErrorThreshold float64
	// Duration is the evaluation window for the phase. Zero means "no time
	// bound" — the phase is judged purely on thresholds at each evaluation.
	Duration time.Duration
	// AutoProgress, when true, advances to the next phase automatically once the
	// success bar is met; when false the engine holds for an operator decision.
	AutoProgress bool
}

// Status is the lifecycle state of a rollout.
type Status string

const (
	// StatusPending is the initial state before the first phase starts.
	StatusPending Status = "pending"
	// StatusActive means a phase is running and being evaluated.
	StatusActive Status = "active"
	// StatusHalted means an error threshold breach stopped the rollout
	// (safety-critical; never auto-resumes).
	StatusHalted Status = "halted"
	// StatusHeld means a phase finished its window without meeting the success
	// bar (or AutoProgress is off) and awaits an operator decision.
	StatusHeld Status = "held"
	// StatusCompleted means the final (100%) phase met its success bar.
	StatusCompleted Status = "completed"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusActive, StatusHalted, StatusHeld, StatusCompleted:
		return true
	default:
		return false
	}
}

// State is the persisted, serializable rollout state. It is the unit that flows
// through [StoragePort]. It carries the immutable plan (Phases) alongside the
// mutable cursor (CurrentPhase, Status, timestamps) so that a single Load yields
// everything the engine needs to evaluate without further I/O.
type State struct {
	// DeploymentID identifies the rollout; also the StoragePort key.
	DeploymentID string
	// Phases is the ordered, validated plan. It does not change after creation.
	Phases []Phase
	// CurrentPhase is the index into Phases of the phase under evaluation.
	CurrentPhase int
	// Status is the current lifecycle state.
	Status Status
	// PhaseStartedAt is when the CurrentPhase began (set by the engine via the
	// Clock). The zero value means the current phase has not started yet.
	PhaseStartedAt time.Time
	// UpdatedAt is the last time the engine wrote this state.
	UpdatedAt time.Time
}

// Clone returns a deep copy of the state so callers and storage fakes cannot
// alias the Phases slice. The engine treats State as a value and never mutates a
// caller's slice in place.
func (s State) Clone() State {
	cp := s
	if s.Phases != nil {
		cp.Phases = make([]Phase, len(s.Phases))
		copy(cp.Phases, s.Phases)
	}
	return cp
}

// Phase returns the phase at the current cursor, and ok=false if the cursor is
// out of range (e.g. an empty or finished plan).
func (s State) Phase() (Phase, bool) {
	if s.CurrentPhase < 0 || s.CurrentPhase >= len(s.Phases) {
		return Phase{}, false
	}
	return s.Phases[s.CurrentPhase], true
}

// isFinalPhase reports whether the cursor is on the last phase of the plan.
func (s State) isFinalPhase() bool {
	return s.CurrentPhase == len(s.Phases)-1
}

// validatePhases checks the ordered plan against the spec constraints: at least
// one phase, percentages strictly increasing within (0,100] and ending at 100,
// thresholds as fractions in [0,1], and non-negative durations.
func validatePhases(phases []Phase) error {
	if len(phases) == 0 {
		return ErrNoPhases
	}
	prev := 0
	for i, p := range phases {
		if p.Percentage <= 0 || p.Percentage > 100 {
			return fmt.Errorf("%w: phase %d = %d", ErrPercentageRange, i, p.Percentage)
		}
		if p.Percentage <= prev {
			return fmt.Errorf("%w: phase %d = %d after %d", ErrPercentageNotMonotonic, i, p.Percentage, prev)
		}
		prev = p.Percentage
		// NaN must be rejected explicitly: every ordered comparison against NaN
		// is false, so `NaN < 0 || NaN > 1` would be false and a NaN threshold
		// would slip through — then `rate >= NaN` in decide() is also always
		// false, silently disabling the halt (error) / advance (success) gate.
		// (±Inf is already caught by the >1 / <0 bounds below.)
		if math.IsNaN(p.SuccessThreshold) || math.IsNaN(p.ErrorThreshold) ||
			p.SuccessThreshold < 0 || p.SuccessThreshold > 1 ||
			p.ErrorThreshold < 0 || p.ErrorThreshold > 1 {
			return fmt.Errorf("%w: phase %d", ErrThresholdRange, i)
		}
		if p.Duration < 0 {
			return fmt.Errorf("%w: phase %d", ErrDurationNegative, i)
		}
	}
	if phases[len(phases)-1].Percentage != 100 {
		return ErrFinalPercentageNot100
	}
	return nil
}
