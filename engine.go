package rollout

import (
	"context"
	"errors"
	"fmt"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// ErrNilPort is returned when the engine is constructed without a storage port
// or clock.
var ErrNilPort = errors.New("rollout: storage port and clock must not be nil")

// Engine is the staged-rollout engine. It is stateless beyond its ports: all
// rollout state lives behind [StoragePort], and the only clock it reads is the
// injected [Clock]. An Engine is safe to share; concurrency control over a given
// deployment id (if needed) is the storage layer's responsibility.
type Engine struct {
	store StoragePort
	clock Clock
}

// New constructs an Engine over the given ports. Both must be non-nil.
func New(store StoragePort, clock Clock) (*Engine, error) {
	if store == nil || clock == nil {
		return nil, ErrNilPort
	}
	return &Engine{store: store, clock: clock}, nil
}

// Create validates the plan and persists the initial pending state for a new
// rollout. It is idempotent in the sense that re-creating an identical plan for
// the same deployment overwrites with the same pending state; callers that must
// not clobber an in-flight rollout should check existence first.
func (e *Engine) Create(ctx context.Context, deploymentID string, phases []Phase) (State, error) {
	if deploymentID == "" {
		return State{}, ErrEmptyDeploymentID
	}
	if err := validatePhases(phases); err != nil {
		return State{}, err
	}
	st := State{
		DeploymentID: deploymentID,
		Phases:       append([]Phase(nil), phases...),
		CurrentPhase: 0,
		Status:       StatusPending,
		UpdatedAt:    e.clock.Now(),
	}
	if err := e.store.Save(ctx, st.Clone()); err != nil {
		return State{}, fmt.Errorf("rollout: create save: %w", err)
	}
	return st, nil
}

// Start activates a pending rollout's first phase, stamping the phase start time
// from the clock. Start is idempotent: calling it on an already-active rollout
// returns the current state unchanged (it does not reset the phase clock), and
// it is a no-op error to start a terminal rollout.
func (e *Engine) Start(ctx context.Context, deploymentID string) (State, error) {
	st, err := e.load(ctx, deploymentID)
	if err != nil {
		return State{}, err
	}
	switch st.Status {
	case StatusActive:
		// Already started — idempotent no-op.
		return st, nil
	case StatusPending:
		st.Status = StatusActive
		st.CurrentPhase = 0
		now := e.clock.Now()
		st.PhaseStartedAt = now
		st.UpdatedAt = now
		if err := e.store.Save(ctx, st.Clone()); err != nil {
			return State{}, fmt.Errorf("rollout: start save: %w", err)
		}
		return st, nil
	default:
		return st, fmt.Errorf("rollout: cannot start from status %q", st.Status)
	}
}

// Evaluate applies one health verdict to the current phase and returns the
// resulting [Decision], persisting any state transition. It is the heart of the
// engine and enforces the safety invariant: HALT wins over ADVANCE.
//
// Decision precedence (telemetry_processing §5):
//
//  1. post-boot health failure       -> HALT (abort)
//  2. error_rate >= error_threshold  -> HALT  ── SAFETY INVARIANT: checked
//     before the success path, so a simultaneous error+success breach halts.
//  3. success_rate >= success_threshold:
//     - final phase  -> COMPLETE
//     - AutoProgress  -> ADVANCE to next phase (reset phase clock)
//     - otherwise     -> HOLD (operator decision)
//  4. window not yet elapsed         -> HOLD (window open)
//  5. window elapsed, bar not met    -> HOLD (held for operator)
//
// Evaluate is idempotent at a terminal status: evaluating a halted/completed
// rollout returns a no-op decision and writes nothing.
func (e *Engine) Evaluate(ctx context.Context, deploymentID string, v HealthVerdict) (Decision, error) {
	if err := v.validate(); err != nil {
		return Decision{}, err
	}
	st, err := e.load(ctx, deploymentID)
	if err != nil {
		return Decision{}, err
	}

	// Idempotent terminal handling: nothing to do, no write.
	switch st.Status {
	case StatusHalted:
		return Decision{Action: ActionHalt, Reason: ReasonErrorThreshold, Status: StatusHalted,
			DeviceStatus: otaprotocol.DeviceDeployFailed}, nil
	case StatusCompleted:
		return Decision{Action: ActionComplete, Reason: ReasonSuccessThreshold, Status: StatusCompleted,
			DeviceStatus: otaprotocol.DeviceDeploySuccess}, nil
	}

	phase, ok := st.Phase()
	if !ok || st.Status == StatusPending {
		// No active phase to evaluate against.
		return Decision{Action: ActionHold, Reason: ReasonNoActivePhase, Status: st.Status,
			DeviceStatus: otaprotocol.DeviceDeployPending}, nil
	}

	dec := decide(phase, v, st.PhaseStartedAt, e.clock.Now(), st.isFinalPhase())

	// Apply the transition to state.
	switch dec.Action {
	case ActionHalt:
		st.Status = StatusHalted
	case ActionComplete:
		st.Status = StatusCompleted
	case ActionAdvance:
		st.CurrentPhase++
		st.Status = StatusActive
		st.PhaseStartedAt = e.clock.Now()
	case ActionHold:
		if dec.Reason == ReasonWindowExpired || dec.Reason == ReasonAutoProgressOff {
			st.Status = StatusHeld
		}
		// ReasonWindowOpen keeps StatusActive — no status churn.
	}
	st.UpdatedAt = e.clock.Now()

	if err := e.store.Save(ctx, st.Clone()); err != nil {
		return Decision{}, fmt.Errorf("rollout: evaluate save: %w", err)
	}
	dec.Status = st.Status
	return dec, nil
}

// load fetches and validates state from the storage port.
func (e *Engine) load(ctx context.Context, deploymentID string) (State, error) {
	if deploymentID == "" {
		return State{}, ErrEmptyDeploymentID
	}
	st, err := e.store.Load(ctx, deploymentID)
	if err != nil {
		return State{}, err
	}
	if !st.Status.Valid() {
		return State{}, fmt.Errorf("rollout: loaded invalid status %q", st.Status)
	}
	return st, nil
}
