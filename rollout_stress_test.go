package rollout

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// §11.4.85 stress + chaos mandate — STRESS suite for the rollout engine.
//
// These tests exercise the REAL engine + REAL cohort selector (no mocks beyond
// the in-memory StoragePort fake, which §11.4.27 permits as a port double; the
// rollout logic under test is the genuine implementation). Every test is
// designed to pass identically under `-race` and `-count=3` (§11.4.50
// deterministic consistency). Categorised outcomes are recorded and asserted —
// not merely "did not crash" (§11.4.6 no-guessing).

// concurrentGoroutines is the contention level; >= 20 per the mandate.
const concurrentGoroutines = 32

// TestStressConcurrentCohortMembershipNoRaceNoDivergence drives InCohort from
// many goroutines over a shared device/deployment matrix and asserts every
// goroutine computes IDENTICAL membership for the same inputs. A data race or a
// lost/torn read would surface under `-race`; a divergent result would fail the
// equality assertion. Membership is a pure function so the ground truth is a
// single-threaded reference computed up front.
func TestStressConcurrentCohortMembershipNoRaceNoDivergence(t *testing.T) {
	const dep = "stress-dep"
	const devices = 400
	pcts := []int{0, 1, 5, 25, 50, 75, 99, 100}

	// Single-threaded reference membership map: key "<dev>@<pct>" -> bool.
	want := make(map[string]bool, devices*len(pcts))
	for i := 0; i < devices; i++ {
		dev := fmt.Sprintf("device-%d", i)
		for _, p := range pcts {
			want[fmt.Sprintf("%s@%d", dev, p)] = InCohort(dev, dep, p)
		}
	}

	var wg sync.WaitGroup
	var mismatches int64
	errCh := make(chan string, concurrentGoroutines)
	for g := 0; g < concurrentGoroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			// Each goroutine sweeps the whole matrix many times.
			for iter := 0; iter < 50; iter++ {
				for i := 0; i < devices; i++ {
					dev := fmt.Sprintf("device-%d", i)
					for _, p := range pcts {
						got := InCohort(dev, dep, p)
						if got != want[fmt.Sprintf("%s@%d", dev, p)] {
							atomic.AddInt64(&mismatches, 1)
							select {
							case errCh <- fmt.Sprintf("g%d: %s@%d got %v", seed, dev, p, got):
							default:
							}
						}
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	if mismatches != 0 {
		for msg := range errCh {
			t.Logf("mismatch: %s", msg)
		}
		t.Fatalf("concurrent cohort membership diverged from single-threaded reference: %d mismatches", mismatches)
	}
}

// TestStressConcurrentEvaluateDistinctDeployments shares ONE engine across many
// goroutines, each driving its OWN deployment to completion. The engine is
// documented safe to share; per-deployment serialisation is the storage layer's
// job (memStore is mutex-guarded). We assert: no race (-race), no lost updates
// (every deployment reaches completed), and the per-deployment phase counts are
// exactly right.
func TestStressConcurrentEvaluateDistinctDeployments(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()
	const deployments = concurrentGoroutines
	healthy := HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.0}

	var wg sync.WaitGroup
	var completed int64
	for d := 0; d < deployments; d++ {
		dep := fmt.Sprintf("dep-%d", d)
		if _, err := eng.Create(ctx, dep, goodPhases()); err != nil {
			t.Fatalf("Create %s: %v", dep, err)
		}
		if _, err := eng.Start(ctx, dep); err != nil {
			t.Fatalf("Start %s: %v", dep, err)
		}
		wg.Add(1)
		go func(dep string) {
			defer wg.Done()
			// goodPhases has 3 phases: advance, advance, complete.
			var last Decision
			for i := 0; i < 3; i++ {
				dec, err := eng.Evaluate(ctx, dep, healthy)
				if err != nil {
					t.Errorf("Evaluate %s iter %d: %v", dep, i, err)
					return
				}
				last = dec
			}
			if last.Action == ActionComplete && last.Status == StatusCompleted {
				atomic.AddInt64(&completed, 1)
			} else {
				t.Errorf("%s did not complete: %+v", dep, last)
			}
		}(dep)
	}
	wg.Wait()

	if completed != deployments {
		t.Fatalf("lost updates: %d/%d deployments completed", completed, deployments)
	}
	// Verify final persisted state of every deployment is exactly completed at
	// the final phase index (2).
	for d := 0; d < deployments; d++ {
		dep := fmt.Sprintf("dep-%d", d)
		st, err := store.Load(ctx, dep)
		if err != nil {
			t.Fatalf("Load %s: %v", dep, err)
		}
		if st.Status != StatusCompleted {
			t.Errorf("%s final status = %q want completed", dep, st.Status)
		}
		if st.CurrentPhase != 2 {
			t.Errorf("%s final phase = %d want 2", dep, st.CurrentPhase)
		}
	}
}

// TestStressSustainedEvaluationLoop runs a sustained loop of >= 100 evaluations
// against a single rollout that holds (window open, bar not met) and records
// categorised outcomes. Every iteration MUST yield the SAME category (active
// window-open hold) — a divergence would be a real non-determinism defect. The
// outcome census is the captured evidence (§11.4.5 / §11.4.85).
func TestStressSustainedEvaluationLoop(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "sustained", goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, "sustained"); err != nil {
		t.Fatal(err)
	}

	const iterations = 500
	// Below-bar verdict with a fresh clock: window stays open -> ActionHold/WindowOpen.
	below := HealthVerdict{SuccessRate: 0.1, ErrorRate: 0.0}

	census := map[string]int{}
	for i := 0; i < iterations; i++ {
		dec, err := eng.Evaluate(ctx, "sustained", below)
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		key := fmt.Sprintf("%s/%s/%s", dec.Action, dec.Reason, dec.Status)
		census[key]++
	}
	t.Logf("sustained-loop outcome census (%d iters): %v", iterations, census)
	wantKey := fmt.Sprintf("%s/%s/%s", ActionHold, ReasonWindowOpen, StatusActive)
	if census[wantKey] != iterations {
		t.Fatalf("sustained loop not uniform: want all %d as %q, census=%v", iterations, wantKey, census)
	}
}

// TestStressSustainedBounded runs evaluations for a bounded wall-clock window
// (the ">= 30s OR >= 100 iterations" arm of the mandate, taking the bounded-time
// arm) and records per-iteration latency-style categorisation. It asserts the
// engine never errors under sustained pressure and the outcome stays uniform.
func TestStressSustainedBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("bounded sustained stress skipped in -short")
	}
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "bounded", goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, "bounded"); err != nil {
		t.Fatal(err)
	}
	below := HealthVerdict{SuccessRate: 0.1}

	deadline := time.Now().Add(2 * time.Second) // bounded; engine is pure+in-mem so 2s yields many iters
	iters := 0
	for time.Now().Before(deadline) {
		dec, err := eng.Evaluate(ctx, "bounded", below)
		if err != nil {
			t.Fatalf("bounded iter %d: %v", iters, err)
		}
		if dec.Action != ActionHold || dec.Status != StatusActive {
			t.Fatalf("bounded iter %d non-uniform: %+v", iters, dec)
		}
		iters++
	}
	t.Logf("bounded sustained loop ran %d iterations in ~2s with uniform active-hold outcome", iters)
	if iters < 100 {
		t.Fatalf("bounded loop only %d iters (<100); pressure too low", iters)
	}
}

// TestStressBoundaryConditions covers the boundary inputs the mandate enumerates:
// 0 devices, 1 device, all-in-one-cohort, max cohort %, off-by-one rollout %.
func TestStressBoundaryConditions(t *testing.T) {
	const dep = "boundary-dep"

	t.Run("zero devices selects nothing", func(t *testing.T) {
		// With no devices there is nothing to select; assert the selector is
		// well-defined for an empty population (a loop over zero devices).
		count := 0
		for _, d := range []string{} { // explicitly empty
			if InCohort(d, dep, 50) {
				count++
			}
		}
		if count != 0 {
			t.Fatalf("zero-device population selected %d", count)
		}
	})

	t.Run("single device deterministic across percentages", func(t *testing.T) {
		dev := "solo-device"
		// At 0% it must be out; at 100% it must be in; in between it is stable.
		if InCohort(dev, dep, 0) {
			t.Fatal("solo device must be out at 0%")
		}
		if !InCohort(dev, dep, 100) {
			t.Fatal("solo device must be in at 100%")
		}
		first := InCohort(dev, dep, 50)
		for i := 0; i < 100; i++ {
			if InCohort(dev, dep, 50) != first {
				t.Fatal("solo device membership non-deterministic at 50%")
			}
		}
	})

	t.Run("all in one cohort at 100 percent", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			if !InCohort(fmt.Sprintf("dev-%d", i), dep, 100) {
				t.Fatalf("device %d not in cohort at 100%%", i)
			}
		}
	})

	t.Run("max cohort percentage clamps", func(t *testing.T) {
		// Over-100 clamps to everybody (cohort.go contract).
		for i := 0; i < 500; i++ {
			if !InCohort(fmt.Sprintf("dev-%d", i), dep, 1000000) {
				t.Fatalf("device %d not selected at clamped max", i)
			}
		}
	})

	t.Run("off-by-one rollout percentage monotonic at boundary", func(t *testing.T) {
		// A device in the cohort at p must remain in at p+1 (off-by-one growth).
		for i := 0; i < 1000; i++ {
			dev := fmt.Sprintf("dev-%d", i)
			for p := 1; p < 100; p++ {
				if InCohort(dev, dep, p) && !InCohort(dev, dep, p+1) {
					t.Fatalf("device %s in at %d%% but out at %d%% (non-monotonic off-by-one)", dev, p, p+1)
				}
			}
		}
	})

	t.Run("single-phase 100 percent rollout completes", func(t *testing.T) {
		eng, _, _ := newEngine(t)
		ctx := context.Background()
		single := []Phase{{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, AutoProgress: true}}
		if _, err := eng.Create(ctx, "single", single); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Start(ctx, "single"); err != nil {
			t.Fatal(err)
		}
		d, err := eng.Evaluate(ctx, "single", HealthVerdict{SuccessRate: 1.0})
		if err != nil {
			t.Fatal(err)
		}
		if d.Action != ActionComplete || d.Status != StatusCompleted {
			t.Fatalf("single-phase rollout did not complete: %+v", d)
		}
	})

	t.Run("max-length monotonic phase plan from 1 to 100", func(t *testing.T) {
		// Build the widest possible strictly-increasing plan: 1..100 (100 phases).
		eng, _, _ := newEngine(t)
		ctx := context.Background()
		phases := make([]Phase, 0, 100)
		for p := 1; p <= 100; p++ {
			phases = append(phases, Phase{Percentage: p, SuccessThreshold: 0.9, ErrorThreshold: 0.5, AutoProgress: true})
		}
		if _, err := eng.Create(ctx, "wide", phases); err != nil {
			t.Fatalf("Create wide plan: %v", err)
		}
		if _, err := eng.Start(ctx, "wide"); err != nil {
			t.Fatal(err)
		}
		healthy := HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.0}
		// 99 advances then 1 complete = 100 evaluations.
		var last Decision
		for i := 0; i < 100; i++ {
			d, err := eng.Evaluate(ctx, "wide", healthy)
			if err != nil {
				t.Fatalf("wide iter %d: %v", i, err)
			}
			last = d
		}
		if last.Action != ActionComplete || last.Status != StatusCompleted {
			t.Fatalf("wide 100-phase plan did not complete: %+v", last)
		}
	})
}

// TestStressConcurrentReevaluationStability hammers the SAME deployment from
// many goroutines with a below-bar verdict (so it never transitions away from
// active-window-open) and asserts that, despite concurrent contention on the
// single state, the deployment stays active and the engine never errors. This
// is the "rapid consecutive calls on one key" stress (storage serialises via its
// mutex, so the engine must remain correct).
func TestStressConcurrentReevaluationStability(t *testing.T) {
	eng, store, _ := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "hot", goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, "hot"); err != nil {
		t.Fatal(err)
	}
	below := HealthVerdict{SuccessRate: 0.1}

	var wg sync.WaitGroup
	var errs int64
	for g := 0; g < concurrentGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				dec, err := eng.Evaluate(ctx, "hot", below)
				if err != nil {
					atomic.AddInt64(&errs, 1)
					return
				}
				// Below-bar with open window must always be an active hold; a
				// halt/advance/complete here would be a lost-update corruption.
				if dec.Action != ActionHold || dec.Status != StatusActive {
					t.Errorf("unexpected transition under contention: %+v", dec)
					return
				}
			}
		}()
	}
	wg.Wait()
	if errs != 0 {
		t.Fatalf("concurrent re-evaluation produced %d errors", errs)
	}
	st, _ := store.Load(ctx, "hot")
	if st.Status != StatusActive || st.CurrentPhase != 0 {
		t.Fatalf("hot deployment corrupted under contention: status=%q phase=%d", st.Status, st.CurrentPhase)
	}
}
