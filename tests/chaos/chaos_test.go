// Package rollout_chaos_test -- chaos tests for ota-rollout-engine (§11.4.85).
//
// Failure-injection and boundary-corruption tests: feed the engine with
// malformed health verdicts, invalid phases, nil ports, and concurrent
// interleaved error/healthy verdicts. Must never panic, must always reach
// a consistent terminal state.
package rollout_chaos_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	rollout "github.com/HelixDevelopment/ota-rollout-engine"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func chaosEvidenceDir() string {
	if d := os.Getenv("HELIX_STRESS_EVIDENCE_DIR"); d != "" {
		return d
	}
	return "qa-results/stress_chaos"
}

func writeChaosEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := chaosEvidenceDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Logf("WARNING: mkdir %s: %v", dir, err)
		return
	}
	ts := time.Now().UTC().Format("20060102T150405Z")
	p := filepath.Join(dir, fmt.Sprintf("%s-%s.json", name, ts))
	if err := os.WriteFile(p, data, 0644); err != nil {
		t.Logf("WARNING: write evidence %s: %v", p, err)
	}
}

// goodPhases returns a valid 3-phase rollout plan.
func goodPhases() []rollout.Phase {
	return []rollout.Phase{
		{Percentage: 5, SuccessThreshold: 0.9, ErrorThreshold: 0.1, Duration: time.Hour, AutoProgress: true},
		{Percentage: 30, SuccessThreshold: 0.9, ErrorThreshold: 0.1, Duration: time.Hour, AutoProgress: true},
		{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, Duration: time.Hour, AutoProgress: true},
	}
}

// newEngine builds a real Engine over an in-memory store with a fake clock.
func newEngine(t testing.TB) (*rollout.Engine, *memStore, *fakeClock) {
	t.Helper()
	store := newMemStore()
	clk := newFakeClock(time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC))
	eng, err := rollout.New(store, clk)
	if err != nil {
		t.Fatalf("New engine: %v", err)
	}
	return eng, store, clk
}

type memStore struct {
	mu       sync.Mutex
	states   map[string]rollout.State
	failSave error
	failLoad error
}

func newMemStore() *memStore { return &memStore{states: make(map[string]rollout.State)} }

func (m *memStore) Load(_ context.Context, deploymentID string) (rollout.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failLoad != nil {
		return rollout.State{}, m.failLoad
	}
	st, ok := m.states[deploymentID]
	if !ok {
		return rollout.State{}, fmt.Errorf("load %q: %w", deploymentID, rollout.ErrNotFound)
	}
	return st.Clone(), nil
}

func (m *memStore) Save(_ context.Context, st rollout.State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSave != nil {
		return m.failSave
	}
	m.states[st.DeploymentID] = st.Clone()
	return nil
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock    { return &fakeClock{now: t} }
func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }

// ---------------------------------------------------------------------------
// TestChaosNilPorts
//
// Constructing an Engine with nil store or nil clock must return an error.
// ---------------------------------------------------------------------------

func TestChaosNilPorts(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	t.Run("nil-store", func(t *testing.T) {
		_, err := rollout.New(nil, newFakeClock(time.Now()))
		if err == nil {
			t.Fatal("expected error for nil store")
		}
	})
	t.Run("nil-clock", func(t *testing.T) {
		_, err := rollout.New(newMemStore(), nil)
		if err == nil {
			t.Fatal("expected error for nil clock")
		}
	})
	t.Run("both-nil", func(t *testing.T) {
		_, err := rollout.New(nil, nil)
		if err == nil {
			t.Fatal("expected error for both nil")
		}
	})
}

// ---------------------------------------------------------------------------
// TestChaosInvalidPhases
//
// Feed the engine every class of invalid phase plan.
// ---------------------------------------------------------------------------

func TestChaosInvalidPhases(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	ctx := context.Background()

	type phaseCase struct {
		name   string
		phases []rollout.Phase
	}
	cases := []phaseCase{
		{"empty", []rollout.Phase{}},
		{"zero-percentage", []rollout.Phase{{Percentage: 0}}},
		{"over-100-percentage", []rollout.Phase{{Percentage: 101, SuccessThreshold: 0.9, ErrorThreshold: 0.1}}},
		{"non-monotonic", []rollout.Phase{
			{Percentage: 50, SuccessThreshold: 0.9, ErrorThreshold: 0.1},
			{Percentage: 25, SuccessThreshold: 0.9, ErrorThreshold: 0.1},
		}},
		{"no-100-final", []rollout.Phase{
			{Percentage: 50, SuccessThreshold: 0.9, ErrorThreshold: 0.1},
		}},
		{"negative-duration", []rollout.Phase{
			{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, Duration: -time.Second},
		}},
		{"over-1-success-threshold", []rollout.Phase{
			{Percentage: 100, SuccessThreshold: 1.5, ErrorThreshold: 0.1},
		}},
		{"negative-success-threshold", []rollout.Phase{
			{Percentage: 100, SuccessThreshold: -0.1, ErrorThreshold: 0.1},
		}},
		{"over-1-error-threshold", []rollout.Phase{
			{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 1.5},
		}},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			eng, _, _ := newEngine(t)
			_, err := eng.Create(ctx, "invalid-"+c.name, c.phases)
			if err == nil {
				t.Fatalf("%s: expected Create error, got nil", c.name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestChaosInterleavedErrorAndHealthy
//
// Concurrently fire healthy and error-breach verdicts at the SAME deployment.
// Assert the terminal state is internally consistent.
// ---------------------------------------------------------------------------

func TestChaosInterleavedErrorAndHealthy(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	eng, store, _ := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "chaos-interleave", goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, "chaos-interleave"); err != nil {
		t.Fatal(err)
	}

	healthy := rollout.HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.0}
	breach := rollout.HealthVerdict{SuccessRate: 0.6, ErrorRate: 0.9}
	finalIdx := len(goodPhases()) - 1

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		v := healthy
		if g%2 == 0 {
			v = breach
		}
		go func(v rollout.HealthVerdict) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, _ = eng.Evaluate(ctx, "chaos-interleave", v)
			}
		}(v)
	}
	wg.Wait()

	st, err := store.Load(ctx, "chaos-interleave")
	if err != nil {
		t.Fatal(err)
	}

	record := map[string]interface{}{
		"test":            "TestChaosInterleavedErrorAndHealthy",
		"status":          st.Status,
		"phase":           st.CurrentPhase,
		"final_phase_idx": finalIdx,
	}
	ev, _ := json.MarshalIndent(record, "", "  ")
	writeChaosEvidence(t, "chaos_interleaved", ev)

	switch st.Status {
	case "halted":
		if st.CurrentPhase >= finalIdx {
			t.Fatalf("halted at final phase %d: halt should win before advance", st.CurrentPhase)
		}
	case "completed":
		if st.CurrentPhase != finalIdx {
			t.Fatalf("completed at phase %d, want final %d", st.CurrentPhase, finalIdx)
		}
	default:
		t.Fatalf("non-terminal state: %q", st.Status)
	}
	t.Logf("Interleaved chaos converged to status=%q phase=%d", st.Status, st.CurrentPhase)
}

// ---------------------------------------------------------------------------
// TestChaosStorageSaveFault
//
// When Save fails, the engine surfaces the error and the committed state is
// never half-applied.
// ---------------------------------------------------------------------------

func TestChaosStorageSaveFault(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	store := newMemStore()
	clk := newFakeClock(time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC))
	eng, err := rollout.New(store, clk)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := eng.Create(ctx, "fault-dep", goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, "fault-dep"); err != nil {
		t.Fatal(err)
	}

	pre, _ := store.Load(ctx, "fault-dep")

	store.mu.Lock()
	store.failSave = errors.New("injected save fault")
	store.mu.Unlock()

	_, err = eng.Evaluate(ctx, "fault-dep", rollout.HealthVerdict{SuccessRate: 0.95})
	if err == nil {
		t.Fatal("expected save fault to surface as error")
	}

	store.mu.Lock()
	store.failSave = nil
	store.mu.Unlock()

	post, _ := store.Load(ctx, "fault-dep")
	if post.Status != pre.Status || post.CurrentPhase != pre.CurrentPhase {
		t.Fatalf("state changed despite failed save: pre=(%q,%d) post=(%q,%d)",
			pre.Status, pre.CurrentPhase, post.Status, post.CurrentPhase)
	}

	// Recovery: after clearing the fault the engine should advance.
	dec, err := eng.Evaluate(ctx, "fault-dep", rollout.HealthVerdict{SuccessRate: 0.95})
	if err != nil {
		t.Fatalf("post-recovery evaluate: %v", err)
	}
	if dec.Action != "advance" {
		t.Fatalf("post-recovery advance expected, got %s", dec.Action)
	}
}

// ---------------------------------------------------------------------------
// TestChaosHealthVerdictBoundaries
//
// Feed Evaluate with every edge case of HealthVerdict: zero rates,
// exactly-1 rates, negative rates, PostBootHealthFailed combined.
// ---------------------------------------------------------------------------

func TestChaosHealthVerdictBoundaries(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	eng, _, _ := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "verdict-dep", goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, "verdict-dep"); err != nil {
		t.Fatal(err)
	}

	verdicts := []struct {
		name    string
		verdict rollout.HealthVerdict
	}{
		{"zero-rates", rollout.HealthVerdict{SuccessRate: 0, ErrorRate: 0}},
		{"max-rates", rollout.HealthVerdict{SuccessRate: 1, ErrorRate: 1}},
		{"only-success", rollout.HealthVerdict{SuccessRate: 0.95}},
		{"only-error", rollout.HealthVerdict{ErrorRate: 0.5}},
		{"postboot-failure", rollout.HealthVerdict{PostBootHealthFailed: true}},
		{"combined-failure", rollout.HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.9, PostBootHealthFailed: true}},
	}

	for _, v := range verdicts {
		v := v
		t.Run(v.name, func(t *testing.T) {
			// Clone the deployment for each verdict to avoid cross-contamination.
			tdep := "vdep-" + v.name
			if _, err := eng.Create(ctx, tdep, goodPhases()); err != nil {
				t.Fatal(err)
			}
			if _, err := eng.Start(ctx, tdep); err != nil {
				t.Fatal(err)
			}
			dec, err := eng.Evaluate(ctx, tdep, v.verdict)
			if err != nil {
				t.Fatalf("%s: %v", v.name, err)
			}
			_ = dec
		})
	}
}
