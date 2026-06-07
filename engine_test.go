package rollout

import (
	"context"
	"errors"
	"testing"
	"time"
)

func goodPhases() []Phase {
	return []Phase{
		{Percentage: 5, SuccessThreshold: 0.9, ErrorThreshold: 0.1, Duration: time.Hour, AutoProgress: true},
		{Percentage: 30, SuccessThreshold: 0.9, ErrorThreshold: 0.1, Duration: time.Hour, AutoProgress: true},
		{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, Duration: time.Hour, AutoProgress: true},
	}
}

func newEngine(t *testing.T) (*Engine, *memStore, *fakeClock) {
	t.Helper()
	store := newMemStore()
	clk := newFakeClock(baseTime)
	eng, err := New(store, clk)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return eng, store, clk
}

func TestNewNilPorts(t *testing.T) {
	if _, err := New(nil, newFakeClock(baseTime)); !errors.Is(err, ErrNilPort) {
		t.Errorf("nil store: got %v want ErrNilPort", err)
	}
	if _, err := New(newMemStore(), nil); !errors.Is(err, ErrNilPort) {
		t.Errorf("nil clock: got %v want ErrNilPort", err)
	}
}

func TestCreateValidation(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	tests := []struct {
		name    string
		dep     string
		phases  []Phase
		wantErr error
	}{
		{"empty deployment id", "", goodPhases(), ErrEmptyDeploymentID},
		{"no phases", "d", nil, ErrNoPhases},
		{"percentage zero", "d", []Phase{{Percentage: 0}}, ErrPercentageRange},
		{"percentage over 100", "d", []Phase{{Percentage: 101}}, ErrPercentageRange},
		{"non-monotonic", "d", []Phase{{Percentage: 30}, {Percentage: 30}}, ErrPercentageNotMonotonic},
		{"success threshold out of range", "d", []Phase{{Percentage: 100, SuccessThreshold: 1.5}}, ErrThresholdRange},
		{"error threshold negative", "d", []Phase{{Percentage: 100, ErrorThreshold: -0.1}}, ErrThresholdRange},
		{"negative duration", "d", []Phase{{Percentage: 100, Duration: -time.Second}}, ErrDurationNegative},
		{"final not 100", "d", []Phase{{Percentage: 50}}, ErrFinalPercentageNot100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := eng.Create(ctx, tt.dep, tt.phases); !errors.Is(err, tt.wantErr) {
				t.Errorf("got %v want %v", err, tt.wantErr)
			}
		})
	}
}

func TestCreateValidPersists(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()
	st, err := eng.Create(ctx, "dep1", goodPhases())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if st.Status != StatusPending {
		t.Errorf("status = %q want pending", st.Status)
	}
	loaded, err := store.Load(ctx, "dep1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Phases) != 3 {
		t.Errorf("phases len = %d want 3", len(loaded.Phases))
	}
}

func TestStartIdempotent(t *testing.T) {
	eng, _, clk := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "dep1", goodPhases()); err != nil {
		t.Fatal(err)
	}
	st1, err := eng.Start(ctx, "dep1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if st1.Status != StatusActive {
		t.Fatalf("status = %q want active", st1.Status)
	}
	firstStart := st1.PhaseStartedAt

	// Advance the clock and start again: must be a no-op that does NOT reset the
	// phase clock.
	clk.advance(time.Hour)
	st2, err := eng.Start(ctx, "dep1")
	if err != nil {
		t.Fatalf("re-Start: %v", err)
	}
	if !st2.PhaseStartedAt.Equal(firstStart) {
		t.Errorf("re-Start reset phase clock: %v != %v", st2.PhaseStartedAt, firstStart)
	}
}

func TestStartFromTerminalErrors(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "dep1", goodPhases()); err != nil {
		t.Fatal(err)
	}
	// Force halted state directly via the store.
	st, _ := store.Load(ctx, "dep1")
	st.Status = StatusHalted
	_ = store.Save(ctx, st)
	if _, err := eng.Start(ctx, "dep1"); err == nil {
		t.Error("expected error starting a halted rollout")
	}
}

func TestEvaluateNotFound(t *testing.T) {
	eng, _, _ := newEngine(t)
	if _, err := eng.Evaluate(context.Background(), "missing", HealthVerdict{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("got %v want ErrNotFound", err)
	}
}

func TestEvaluateVerdictValidation(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	_, _ = eng.Start(ctx, "dep1")
	bad := []HealthVerdict{
		{SuccessRate: 1.1},
		{ErrorRate: -0.1},
	}
	for _, v := range bad {
		if _, err := eng.Evaluate(ctx, "dep1", v); !errors.Is(err, ErrVerdictRange) {
			t.Errorf("verdict %+v: got %v want ErrVerdictRange", v, err)
		}
	}
}

// TestEvaluateFullProgression drives a rollout 5% -> 30% -> 100% -> complete.
func TestEvaluateFullProgression(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	_, _ = eng.Start(ctx, "dep1")
	healthy := HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.0}

	d1, _ := eng.Evaluate(ctx, "dep1", healthy)
	if d1.Action != ActionAdvance || d1.Status != StatusActive {
		t.Fatalf("phase0: %+v", d1)
	}
	d2, _ := eng.Evaluate(ctx, "dep1", healthy)
	if d2.Action != ActionAdvance {
		t.Fatalf("phase1: %+v", d2)
	}
	d3, _ := eng.Evaluate(ctx, "dep1", healthy)
	if d3.Action != ActionComplete || d3.Status != StatusCompleted {
		t.Fatalf("phase2 final: %+v", d3)
	}
}

// TestEvaluateAdvanceResetsPhaseClock: advancing stamps a fresh phase start.
func TestEvaluateAdvanceResetsPhaseClock(t *testing.T) {
	eng, store, clk := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	_, _ = eng.Start(ctx, "dep1")
	clk.advance(30 * time.Minute)
	if _, err := eng.Evaluate(ctx, "dep1", HealthVerdict{SuccessRate: 0.95}); err != nil {
		t.Fatal(err)
	}
	st, _ := store.Load(ctx, "dep1")
	if st.CurrentPhase != 1 {
		t.Fatalf("current phase = %d want 1", st.CurrentPhase)
	}
	if !st.PhaseStartedAt.Equal(clk.Now()) {
		t.Errorf("phase clock not reset on advance: %v != %v", st.PhaseStartedAt, clk.Now())
	}
}

// TestEvaluateHaltIsIdempotent: once halted, further evaluations are no-op
// halts and write nothing more to the store.
func TestEvaluateHaltIsIdempotent(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	_, _ = eng.Start(ctx, "dep1")

	d, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{ErrorRate: 0.5})
	if d.Action != ActionHalt || d.Status != StatusHalted {
		t.Fatalf("first eval: %+v", d)
	}
	savesAfterHalt := store.saves

	for i := 0; i < 3; i++ {
		d2, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{SuccessRate: 1.0})
		if d2.Action != ActionHalt || d2.Status != StatusHalted {
			t.Fatalf("idempotent eval %d: %+v", i, d2)
		}
	}
	if store.saves != savesAfterHalt {
		t.Errorf("terminal evaluations wrote to store: saves %d -> %d", savesAfterHalt, store.saves)
	}
}

// TestEvaluateCompleteIsIdempotent mirrors the halt case for completion.
func TestEvaluateCompleteIsIdempotent(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()
	single := []Phase{{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, AutoProgress: true}}
	_, _ = eng.Create(ctx, "dep1", single)
	_, _ = eng.Start(ctx, "dep1")
	d, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{SuccessRate: 1.0})
	if d.Action != ActionComplete {
		t.Fatalf("complete: %+v", d)
	}
	saves := store.saves
	d2, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{ErrorRate: 1.0})
	if d2.Action != ActionComplete {
		t.Errorf("post-complete eval changed action: %+v", d2)
	}
	if store.saves != saves {
		t.Errorf("post-complete eval wrote to store")
	}
}

// TestEvaluateHaltWinsViaEngine confirms the invariant end-to-end (not just in
// the pure decide()).
func TestEvaluateHaltWinsViaEngine(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	_, _ = eng.Start(ctx, "dep1")
	// success bar met AND error bar breached simultaneously.
	d, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{SuccessRate: 1.0, ErrorRate: 0.5})
	if d.Action != ActionHalt {
		t.Fatalf("halt must win: %+v", d)
	}
	st, _ := store.Load(ctx, "dep1")
	if st.CurrentPhase != 0 {
		t.Errorf("halt must not advance phase: current = %d", st.CurrentPhase)
	}
}

// TestEvaluateWindowHoldDoesNotChurnStatus: holding with an open window keeps the
// rollout active.
func TestEvaluateWindowHold(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	_, _ = eng.Start(ctx, "dep1")
	d, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{SuccessRate: 0.1})
	if d.Action != ActionHold || d.Reason != ReasonWindowOpen || d.Status != StatusActive {
		t.Fatalf("expected active window-open hold: %+v", d)
	}
}

// TestEvaluateWindowExpiredHolds: window elapses below the bar -> held.
func TestEvaluateWindowExpiredHolds(t *testing.T) {
	eng, _, clk := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	_, _ = eng.Start(ctx, "dep1")
	clk.advance(2 * time.Hour)
	d, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{SuccessRate: 0.1})
	if d.Action != ActionHold || d.Reason != ReasonWindowExpired || d.Status != StatusHeld {
		t.Fatalf("expected held on window expiry: %+v", d)
	}
}

// TestEvaluatePostBootAbort: post-boot failure aborts regardless of rates.
func TestEvaluatePostBootAbort(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	_, _ = eng.Start(ctx, "dep1")
	d, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{SuccessRate: 1.0, PostBootHealthFailed: true})
	if d.Action != ActionHalt || d.Reason != ReasonPostBootFailed {
		t.Fatalf("expected post-boot halt: %+v", d)
	}
}

// TestEvaluatePendingNotStarted: evaluating before Start yields a no-active-phase
// hold and does not crash.
func TestEvaluatePendingNotStarted(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	_, _ = eng.Create(ctx, "dep1", goodPhases())
	d, _ := eng.Evaluate(ctx, "dep1", HealthVerdict{SuccessRate: 1.0})
	if d.Action != ActionHold || d.Reason != ReasonNoActivePhase {
		t.Fatalf("expected no-active-phase hold: %+v", d)
	}
}

func TestSaveErrorPropagates(t *testing.T) {
	store := newMemStore()
	store.failSave = errors.New("disk full")
	eng, _ := New(store, newFakeClock(baseTime))
	if _, err := eng.Create(context.Background(), "dep1", goodPhases()); err == nil {
		t.Error("expected save error to propagate from Create")
	}
}

func TestLoadInvalidStatus(t *testing.T) {
	store := newMemStore()
	eng, _ := New(store, newFakeClock(baseTime))
	ctx := context.Background()
	_ = store.Save(ctx, State{DeploymentID: "dep1", Phases: goodPhases(), Status: Status("bogus")})
	if _, err := eng.Evaluate(ctx, "dep1", HealthVerdict{}); err == nil {
		t.Error("expected error loading invalid status")
	}
}

// TestStateClone ensures Clone does not alias the phase slice.
func TestStateClone(t *testing.T) {
	st := State{DeploymentID: "d", Phases: goodPhases()}
	cp := st.Clone()
	cp.Phases[0].Percentage = 999
	if st.Phases[0].Percentage == 999 {
		t.Error("Clone aliased the Phases slice")
	}
}

func TestSystemClockNow(t *testing.T) {
	c := NewSystemClock()
	if c.Now().IsZero() {
		t.Error("system clock returned zero time")
	}
}
