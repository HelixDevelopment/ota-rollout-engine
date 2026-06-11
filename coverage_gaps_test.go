package rollout

import (
	"context"
	"errors"
	"testing"
	"time"
)

// These tests close the remaining coverage gaps on the rollout decision +
// lifecycle internals — windowExpired (auto-progress timing), State.Phase cursor
// bounds, and the Engine.load / Engine.Start error + idempotency branches. Each
// asserts real behaviour that FAILs on regression (anti-bluff §11.4).

func TestWindowExpired(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		start time.Time
		now   time.Time
		d     time.Duration
		want  bool
	}{
		{"zero start is never expired", time.Time{}, base.Add(time.Hour), time.Minute, false},
		{"before window end not expired", base, base.Add(30 * time.Second), time.Minute, false},
		{"exactly at window end is expired (inclusive)", base, base.Add(time.Minute), time.Minute, true},
		{"past window end is expired", base, base.Add(2 * time.Minute), time.Minute, true},
	}
	for _, c := range cases {
		if got := windowExpired(c.start, c.now, c.d); got != c.want {
			t.Errorf("%s: windowExpired=%v want %v", c.name, got, c.want)
		}
	}
}

func TestStatePhaseCursorBounds(t *testing.T) {
	s := State{Phases: goodPhases()}
	s.CurrentPhase = 1
	if got, ok := s.Phase(); !ok || got.Percentage != 30 {
		t.Fatalf("valid cursor 1: got %+v ok=%v (want pct 30)", got, ok)
	}
	s.CurrentPhase = -1
	if _, ok := s.Phase(); ok {
		t.Error("negative cursor must return ok=false")
	}
	s.CurrentPhase = len(s.Phases) // == len -> out of range
	if _, ok := s.Phase(); ok {
		t.Error("cursor at len must return ok=false")
	}
	if _, ok := (State{}).Phase(); ok {
		t.Error("empty plan must return ok=false")
	}
}

func TestStartLoadErrorPaths(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()

	// Empty deployment id is rejected by load's guard.
	if _, err := eng.Start(ctx, ""); !errors.Is(err, ErrEmptyDeploymentID) {
		t.Fatalf("empty id: want ErrEmptyDeploymentID, got %v", err)
	}

	// A storage Load failure surfaces unchanged.
	wantErr := errors.New("boom-load")
	store.failLoad = wantErr
	if _, err := eng.Start(ctx, "d1"); !errors.Is(err, wantErr) {
		t.Fatalf("load failure: want boom-load, got %v", err)
	}
	store.failLoad = nil

	// A loaded state carrying an invalid status is rejected (not trusted).
	store.states["bad"] = State{DeploymentID: "bad", Status: Status("not-a-status"), Phases: goodPhases()}
	if _, err := eng.Start(ctx, "bad"); err == nil {
		t.Fatal("loaded invalid status must error")
	}
}

func TestStartIdempotentTerminalAndSaveError(t *testing.T) {
	eng, store, clk := newEngine(t)
	ctx := context.Background()

	// Already-active rollout: idempotent no-op (returns active, no re-save → no
	// phase-clock reset).
	store.states["act"] = State{
		DeploymentID: "act", Status: StatusActive, CurrentPhase: 0,
		Phases: goodPhases(), PhaseStartedAt: clk.Now(),
	}
	savesBefore := store.saves
	st, err := eng.Start(ctx, "act")
	if err != nil || st.Status != StatusActive {
		t.Fatalf("idempotent start: status=%q err=%v", st.Status, err)
	}
	if store.saves != savesBefore {
		t.Fatalf("idempotent start must not re-save (saves %d -> %d)", savesBefore, store.saves)
	}

	// Terminal rollout: starting it is a no-op error (cannot restart).
	store.states["done"] = State{DeploymentID: "done", Status: StatusCompleted, Phases: goodPhases()}
	if _, err := eng.Start(ctx, "done"); err == nil {
		t.Fatal("starting a terminal (completed) rollout must error")
	}

	// Pending rollout whose Save fails surfaces the save error.
	store.states["pend"] = State{DeploymentID: "pend", Status: StatusPending, Phases: goodPhases()}
	store.failSave = errors.New("boom-save")
	if _, err := eng.Start(ctx, "pend"); err == nil {
		t.Fatal("start with a failing Save must return an error")
	}
}
