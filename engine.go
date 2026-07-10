package rollout

import (
	"context"
	"errors"
	"fmt"
	"sync"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// ErrNilPort is returned when the engine is constructed without a storage port
// or clock.
var ErrNilPort = errors.New("rollout: storage port and clock must not be nil")

// Engine is the staged-rollout engine. It is stateless beyond its ports: all
// rollout state lives behind [StoragePort], and the only clock it reads is the
// injected [Clock]. An Engine is safe to share across goroutines, INCLUDING
// concurrent calls against the SAME deployment id: Create/Start/Evaluate each
// perform a Load -> compute -> Save sequence, and the Engine serializes that
// whole sequence per deployment id with an internal lock (see [Engine.critical])
// so a concurrent stale read can never overwrite a just-committed transition
// (in particular, the halt-wins-over-advance safety invariant holds under
// concurrent Evaluate calls on one deployment id).
//
// This in-process serialization does NOT substitute for a storage layer's own
// consistency guarantees across multiple Engine instances / processes sharing
// one backing store (e.g. two control-plane replicas evaluating the same
// deployment id against the same database concurrently) -- that remains the
// storage layer's responsibility (e.g. row-level locking, optimistic
// concurrency / compare-and-swap on write).
type Engine struct {
	store StoragePort
	clock Clock

	// keyLocks holds one *sync.Mutex per deployment id, created on first use.
	// It serializes each deployment's Load-compute-Save critical section
	// within this Engine instance so concurrent Create/Start/Evaluate calls
	// on the SAME deployment id cannot interleave a stale read between
	// another call's read and write.
	keyLocks sync.Map // map[string]*sync.Mutex
}

// New constructs an Engine over the given ports. Both must be non-nil.
func New(store StoragePort, clock Clock) (*Engine, error) {
	if store == nil || clock == nil {
		return nil, ErrNilPort
	}
	return &Engine{store: store, clock: clock}, nil
}

// critical acquires the per-deployment-id lock and returns an unlock func the
// caller MUST defer immediately. Every method that performs a Load-compute-
// Save sequence against deploymentID MUST wrap that sequence with this lock so
// the sequence is atomic with respect to every other Create/Start/Evaluate
// call on the same deployment id from this Engine instance.
func (e *Engine) critical(deploymentID string) func() {
	v, _ := e.keyLocks.LoadOrStore(deploymentID, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
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
	defer e.critical(deploymentID)()
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
	defer e.critical(deploymentID)()
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
//
// The whole Load -> decide -> Save sequence is atomic with respect to every
// other Create/Start/Evaluate call on the SAME deploymentID from this Engine
// instance (see [Engine.critical]): two concurrent Evaluate calls on one
// deployment id can never both read the same pre-transition state and race to
// write, which would otherwise let a stale ActionAdvance silently overwrite a
// just-committed ActionHalt and violate the halt-wins-over-advance invariant.
func (e *Engine) Evaluate(ctx context.Context, deploymentID string, v HealthVerdict) (Decision, error) {
	if err := v.validate(); err != nil {
		return Decision{}, err
	}
	defer e.critical(deploymentID)()
	st, err := e.load(ctx, deploymentID)
	if err != nil {
		return Decision{}, err
	}

	// Idempotent terminal handling: nothing to do, no write.
	switch st.Status {
	case StatusHalted:
		// Report the ACTUAL halt cause recorded at halt time. Fall back to
		// ReasonErrorThreshold only for legacy states persisted before
		// HaltReason existed (empty value), preserving prior behaviour.
		haltReason := st.HaltReason
		if haltReason == "" {
			haltReason = ReasonErrorThreshold
		}
		return Decision{Action: ActionHalt, Reason: haltReason, Status: StatusHalted,
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
		// Persist the halt cause so a later re-evaluation of this terminal
		// state reports the true reason (error-threshold vs post-boot failure).
		st.HaltReason = dec.Reason
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
