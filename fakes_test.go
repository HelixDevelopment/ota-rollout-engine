package rollout

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// memStore is an in-memory [StoragePort] fake used only by tests. It clones on
// the way in and out so the engine cannot alias test-owned slices.
type memStore struct {
	mu     sync.Mutex
	states map[string]State
	// failSave, when set, makes Save return this error (to exercise error paths).
	failSave error
	// failLoad, when set, makes Load return this error.
	failLoad error
	saves    int
}

func newMemStore() *memStore { return &memStore{states: map[string]State{}} }

func (m *memStore) Load(_ context.Context, deploymentID string) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failLoad != nil {
		return State{}, m.failLoad
	}
	st, ok := m.states[deploymentID]
	if !ok {
		return State{}, fmt.Errorf("load %q: %w", deploymentID, ErrNotFound)
	}
	return st.Clone(), nil
}

func (m *memStore) Save(_ context.Context, st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSave != nil {
		return m.failSave
	}
	m.states[st.DeploymentID] = st.Clone()
	m.saves++
	return nil
}

// fakeClock is a controllable [Clock]. advance moves it forward.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
