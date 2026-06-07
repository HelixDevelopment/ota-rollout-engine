package rollout

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by a [StoragePort] when no rollout state exists for
// the requested deployment id. Implementations MUST return this sentinel (or an
// error wrapping it) so callers can distinguish "absent" from a real failure.
var ErrNotFound = errors.New("rollout: state not found")

// Clock is the time port. It is the only source of "now" the engine consults so
// that phase durations can be driven deterministically in tests with a fake.
type Clock interface {
	// Now returns the current instant.
	Now() time.Time
}

// StoragePort is the persistence port for rollout state. It is intentionally
// minimal and transport/storage agnostic: an implementation may be backed by an
// in-memory map (see the test fakes), a SQL table, or any KV store. The engine
// performs no I/O of its own and never assumes a particular backend.
//
// Implementations must be safe to call with a cancellable context and must
// return [ErrNotFound] (possibly wrapped) from Load when the deployment is
// unknown.
type StoragePort interface {
	// Load returns the persisted state for deploymentID, or an error wrapping
	// [ErrNotFound] if none exists.
	Load(ctx context.Context, deploymentID string) (State, error)
	// Save persists state. The deployment id is taken from state.DeploymentID.
	Save(ctx context.Context, state State) error
}

// systemClock is the default real-time [Clock]. It is unexported; callers that
// want the wall clock use [NewSystemClock].
type systemClock struct{}

// Now returns time.Now().
func (systemClock) Now() time.Time { return time.Now() }

// NewSystemClock returns a [Clock] backed by the real wall clock. Production
// callers use this; tests inject a fake clock instead.
func NewSystemClock() Clock { return systemClock{} }
