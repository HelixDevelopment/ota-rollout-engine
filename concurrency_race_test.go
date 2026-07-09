package rollout

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Defect background (see qa-results/audit_20260710/FINDINGS.md for the full
// writeup + RED transcript captured against the pre-fix engine):
//
// Engine.Evaluate performed an unsynchronized Load -> pure decide() -> Save
// sequence with NO per-deployment mutual exclusion. Two concurrent Evaluate
// calls against the SAME deployment id could each Load the same
// pre-transition state before either had Saved. If one call's verdict
// resolved to ActionHalt (safety-critical) and the other's STALE read
// resolved to ActionAdvance, and the Advance call's Save landed after the
// Halt call's Save, the committed Halt was silently overwritten -- directly
// violating the documented safety invariant "halt wins over advance"
// (engine.go Evaluate doc comment, decide.go package doc).
//
// This was reproduced deterministically (not `-race`-timing luck) with a
// gated StoragePort test double that forced the exact interleaving: G1
// (breach) Loads first, G2 (healthy) Loads second reading the SAME pre-halt
// state, G1's Save (Halted) is released and completes, then G2's Save
// (Advance, computed from its stale load) is released and overwrites it.
// Captured RED evidence (pre-fix, 3/3 runs): final committed state was
// "active" phase=1, not the required "halted".
//
// The fix adds Engine.critical(deploymentID): a per-deployment-id mutex that
// serializes the WHOLE Load-decide-Save sequence for Create/Start/Evaluate.
// This makes the ORIGINAL interleaving unreachable through the public API --
// a concurrent Evaluate call now blocks on the mutex before it can even call
// Load, so the two tests below prove the fix from first principles instead of
// trying to force the (now-impossible) original interleaving:
//
//  1. TestConcurrentEvaluateEitherOrderConvergesToHalt: launches a breach and
//     a healthy Evaluate concurrently with NO artificial gating and asserts
//     the final state is Halted regardless of which one the scheduler runs
//     first (a case analysis of both total orders shows Halted is the only
//     possible outcome once the critical section is atomic; see the
//     per-test doc comment for the proof).
//  2. TestEvaluateCriticalSectionIsAtomicPerDeployment: directly proves
//     atomicity by observing Load/Save event ordering with a deliberately
//     slow first Load -- if the per-deployment lock were ever removed, a
//     concurrent second call would interleave its own Load into the slow
//     window, and the assertion would catch it.
// ---------------------------------------------------------------------------

// TestConcurrentEvaluateEitherOrderConvergesToHalt fires a breach verdict and
// a healthy verdict concurrently at the SAME deployment (no artificial
// gating) and asserts the final committed status is always Halted --
// regardless of which goroutine the scheduler happens to run to completion
// first. This holds now that Engine.Evaluate's critical section is atomic per
// deployment id:
//
//   - If the breach call completes first: it Halts directly (error_rate 0.9 >=
//     the phase's error_threshold 0.1). The healthy call then runs against the
//     already-committed Halted state, which is terminal/idempotent per
//     Evaluate's documented contract, so it stays Halted.
//   - If the healthy call completes first: it Advances (success_rate 0.95 >=
//     the phase's success_threshold 0.9, AutoProgress true, not final phase).
//     The breach call then runs against the freshly-advanced (still
//     non-terminal) state and Halts there instead (every phase in
//     goodPhases() shares the same 0.1 error_threshold).
//
// Both total orders converge to Halted, so this is NOT a "usually passes"
// probabilistic assertion -- with an atomic critical section it is
// deterministically true on every run, and is run with -count to demonstrate
// exactly that (§11.4.50 deterministic consistency).
func TestConcurrentEvaluateEitherOrderConvergesToHalt(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()
	const dep = "race-either-order-halts"
	if _, err := eng.Create(ctx, dep, goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, dep); err != nil {
		t.Fatal(err)
	}

	breach := HealthVerdict{SuccessRate: 0.0, ErrorRate: 0.9}
	healthy := HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.0}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := eng.Evaluate(ctx, dep, breach); err != nil {
			t.Errorf("breach Evaluate: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := eng.Evaluate(ctx, dep, healthy); err != nil {
			t.Errorf("healthy Evaluate: %v", err)
		}
	}()
	wg.Wait()

	final, err := store.Load(ctx, dep)
	if err != nil {
		t.Fatalf("final load: %v", err)
	}
	if final.Status != StatusHalted {
		t.Fatalf("safety invariant violated: final status = %q want %q (halt must win over advance under any concurrent ordering)",
			final.Status, StatusHalted)
	}
}

// TestEvaluateCriticalSectionIsAtomicPerDeployment directly proves that
// Engine.Evaluate's Load-decide-Save sequence is serialized end-to-end per
// deployment id: two concurrent Evaluate calls on the SAME deployment can
// never interleave -- one call's entire Load->Save unit always completes
// before the other's Load begins. It uses a StoragePort double whose FIRST
// Load call (post-arm) sleeps briefly, widening the window during which an
// unsynchronized second call would slip its own Load in; the recorded event
// order proves no such interleaving occurs.
//
// This is the mechanism-level regression guard for the fix: if the
// per-deployment lock in Engine.critical were ever removed, the second call's
// "load-start" would appear before the first call's "save-end" in the
// recorded sequence, and the pattern assertion below would fail.
func TestEvaluateCriticalSectionIsAtomicPerDeployment(t *testing.T) {
	ts := newTimingStore()
	clk := newFakeClock(baseTime)
	eng, err := New(ts, clk)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const dep = "atomicity-check"
	if _, err := eng.Create(ctx, dep, goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, dep); err != nil {
		t.Fatal(err)
	}

	// Only the two racing Evaluate calls below are recorded.
	ts.reset()
	ts.armSlowLoad(50 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = eng.Evaluate(ctx, dep, HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.0})
	}()
	// Give the first goroutine a head start so it is the one that consumes
	// the (single-use) slow-load latch.
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		_, _ = eng.Evaluate(ctx, dep, HealthVerdict{SuccessRate: 0.0, ErrorRate: 0.9})
	}()
	wg.Wait()

	events := ts.snapshot()
	wantPattern := []string{
		"load-start", "load-end", "save-start", "save-end",
		"load-start", "load-end", "save-start", "save-end",
	}
	if len(events) != len(wantPattern) {
		t.Fatalf("want %d events (2 fully-serialized load/save units), got %d: %v", len(wantPattern), len(events), events)
	}
	for i, want := range wantPattern {
		if events[i] != want {
			t.Fatalf("event %d = %q want %q; full sequence: %v (interleaving detected -- critical section not atomic)",
				i, events[i], want, events)
		}
	}
}

// timingStore is a StoragePort test double that records the order of
// Load/Save start/end events and can make exactly one future Load call sleep
// briefly (to widen a race window for a deliberately-triggered concurrency
// probe).
type timingStore struct {
	inner *memStore

	mu     sync.Mutex
	events []string

	slowDelay    time.Duration
	slowConsumed int32
}

func newTimingStore() *timingStore { return &timingStore{inner: newMemStore()} }

// reset clears the recorded event log.
func (s *timingStore) reset() {
	s.mu.Lock()
	s.events = nil
	s.mu.Unlock()
}

// armSlowLoad makes exactly the next Load call sleep for d before proceeding.
func (s *timingStore) armSlowLoad(d time.Duration) {
	s.slowDelay = d
	atomic.StoreInt32(&s.slowConsumed, 0)
}

func (s *timingStore) record(ev string) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
}

func (s *timingStore) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]string, len(s.events))
	copy(cp, s.events)
	return cp
}

func (s *timingStore) Load(ctx context.Context, deploymentID string) (State, error) {
	s.record("load-start")
	if atomic.CompareAndSwapInt32(&s.slowConsumed, 0, 1) && s.slowDelay > 0 {
		time.Sleep(s.slowDelay)
	}
	st, err := s.inner.Load(ctx, deploymentID)
	s.record("load-end")
	return st, err
}

func (s *timingStore) Save(ctx context.Context, st State) error {
	s.record("save-start")
	err := s.inner.Save(ctx, st)
	s.record("save-end")
	return err
}

// TestConcurrentStartRaceDoesNotCorruptPhaseClock exercises the analogous
// load-decide-save hazard in Engine.Start: many concurrent Start calls on the
// SAME pending deployment must not leave an inconsistent PhaseStartedAt
// visible to a third reader. With per-deployment serialization this
// converges to exactly one consistent state.
func TestConcurrentStartRaceDoesNotCorruptPhaseClock(t *testing.T) {
	eng, store, clk := newEngine(t)
	ctx := context.Background()
	const dep = "race-start"
	if _, err := eng.Create(ctx, dep, goodPhases()); err != nil {
		t.Fatal(err)
	}

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := eng.Start(ctx, dep); err != nil {
				t.Errorf("Start: %v", err)
			}
		}()
	}
	wg.Wait()

	st, err := store.Load(ctx, dep)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusActive || st.CurrentPhase != 0 {
		t.Fatalf("concurrent Start left inconsistent state: status=%q phase=%d", st.Status, st.CurrentPhase)
	}
	if !st.PhaseStartedAt.Equal(clk.Now()) {
		t.Fatalf("PhaseStartedAt = %v want %v (clock never advanced during this test)", st.PhaseStartedAt, clk.Now())
	}
	if st.PhaseStartedAt.IsZero() {
		t.Fatal("PhaseStartedAt not stamped after concurrent Start")
	}
}
