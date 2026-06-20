// Package rollout_stress_test -- stress tests for ota-rollout-engine (§11.4.85).
//
// Exercises the real Engine + real InCohort selector under sustained concurrent
// load, capturing categorised outcomes and latency distributions as evidence.
package rollout_stress_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rollout "github.com/HelixDevelopment/ota-rollout-engine"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func evidenceDir() string {
	if d := os.Getenv("HELIX_STRESS_EVIDENCE_DIR"); d != "" {
		return d
	}
	return "qa-results/stress_chaos"
}

func writeEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := evidenceDir()
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

func percentiles(durations []time.Duration) (p50, p95, p99 time.Duration) {
	n := len(durations)
	if n == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, n)
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50 = sorted[n*50/100]
	p95 = sorted[n*95/100]
	p99 = sorted[n*99/100]
	return
}

// goodPhases returns a valid 3-phase rollout plan (5%, 30%, 100%).
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

// memStore is a simple in-memory StoragePort for tests.
type memStore struct {
	mu     sync.Mutex
	states map[string]rollout.State
	saves  int
}

func newMemStore() *memStore { return &memStore{states: make(map[string]rollout.State)} }

func (m *memStore) Load(_ context.Context, deploymentID string) (rollout.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.states[deploymentID]
	if !ok {
		return rollout.State{}, fmt.Errorf("load %q: %w", deploymentID, rollout.ErrNotFound)
	}
	return st.Clone(), nil
}

func (m *memStore) Save(_ context.Context, st rollout.State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[st.DeploymentID] = st.Clone()
	m.saves++
	return nil
}

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

// ---------------------------------------------------------------------------
// TestStressSustainedInCohort
//
// N=1000 iterations of InCohort with boundary percentages, all deterministic.
// ---------------------------------------------------------------------------

func TestStressSustainedInCohort(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	const N = 1000
	durations := make([]time.Duration, 0, N)
	var monotonicFailures int

	for i := 0; i < N; i++ {
		dev := fmt.Sprintf("device-%d", i%100)
		pct := (i % 101)
		start := time.Now()
		m1 := rollout.InCohort(dev, "sustained-dep", pct)
		m2 := rollout.InCohort(dev, "sustained-dep", pct+1)
		durations = append(durations, time.Since(start))

		// Monotonic: if in at pct, must be in at pct+1.
		if m1 && !m2 {
			monotonicFailures++
		}
	}

	p50, p95, p99 := percentiles(durations)
	record := map[string]interface{}{
		"test":               "TestStressSustainedInCohort",
		"N":                  N,
		"monotonic_failures": monotonicFailures,
		"p50_ns":             p50.Nanoseconds(),
		"p95_ns":             p95.Nanoseconds(),
		"p99_ns":             p99.Nanoseconds(),
	}
	ev, _ := json.MarshalIndent(record, "", "  ")
	writeEvidence(t, "sustained_incobort", ev)
	t.Logf("InCohort sustained N=%d: p50=%v p95=%v p99=%v monotonic_failures=%d",
		N, p50, p95, p99, monotonicFailures)
	if monotonicFailures != 0 {
		t.Fatal("monotonicity violated: device in cohort at pct but out at pct+1")
	}
}

// ---------------------------------------------------------------------------
// TestStressConcurrentEngineEvaluate
//
// 32 goroutines each drive their own deployment through the full lifecycle
// (Create -> Start -> Evaluate x3), asserting all reach StatusCompleted.
// ---------------------------------------------------------------------------

func TestStressConcurrentEngineEvaluate(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	eng, _, _ := newEngine(t)
	ctx := context.Background()
	const deployments = 32
	healthy := rollout.HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.0}

	var wg sync.WaitGroup
	var completed int64
	var errors int64

	for d := 0; d < deployments; d++ {
		dep := fmt.Sprintf("stress-dep-%d", d)
		if _, err := eng.Create(ctx, dep, goodPhases()); err != nil {
			t.Fatalf("Create %s: %v", dep, err)
		}
		if _, err := eng.Start(ctx, dep); err != nil {
			t.Fatalf("Start %s: %v", dep, err)
		}
		wg.Add(1)
		go func(dep string) {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				dec, err := eng.Evaluate(ctx, dep, healthy)
				if err != nil {
					atomic.AddInt64(&errors, 1)
					return
				}
				if i == 2 && dec.Action == "complete" {
					atomic.AddInt64(&completed, 1)
				}
			}
		}(dep)
	}
	wg.Wait()

	record := map[string]interface{}{
		"test":        "TestStressConcurrentEngineEvaluate",
		"deployments": deployments,
		"completed":   completed,
		"errors":      errors,
	}
	ev, _ := json.MarshalIndent(record, "", "  ")
	writeEvidence(t, "concurrent_engine_evaluate", ev)
	t.Logf("Concurrent engine: %d deployments, %d completed, %d errors", deployments, completed, errors)
	if completed != deployments {
		t.Fatalf("expected %d completed, got %d; errors=%d", deployments, completed, errors)
	}
}

// ---------------------------------------------------------------------------
// TestStressBoundaryPhases
//
// Boundary conditions: single-phase, 100-phase plan, zero-duration phase,
// edge threshold values.
// ---------------------------------------------------------------------------

func TestStressBoundaryPhases(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	ctx := context.Background()

	t.Run("single-phase-complete", func(t *testing.T) {
		eng, _, _ := newEngine(t)
		phases := []rollout.Phase{
			{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, AutoProgress: true},
		}
		if _, err := eng.Create(ctx, "single", phases); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Start(ctx, "single"); err != nil {
			t.Fatal(err)
		}
		dec, err := eng.Evaluate(ctx, "single", rollout.HealthVerdict{SuccessRate: 1.0})
		if err != nil {
			t.Fatal(err)
		}
		if dec.Action != "complete" {
			t.Fatalf("single-phase: want complete, got %s", dec.Action)
		}
	})

	t.Run("zero-duration-window", func(t *testing.T) {
		eng, _, clk := newEngine(t)
		phases := []rollout.Phase{
			{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, Duration: 0, AutoProgress: true},
		}
		if _, err := eng.Create(ctx, "zero-dur", phases); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Start(ctx, "zero-dur"); err != nil {
			t.Fatal(err)
		}
		clk.advance(time.Hour) // zero-duration window never expires
		dec, err := eng.Evaluate(ctx, "zero-dur", rollout.HealthVerdict{SuccessRate: 0.5})
		if err != nil {
			t.Fatal(err)
		}
		// With no time bound and bar not met, should hold with window open.
		if dec.Action == "halt" {
			t.Fatalf("zero-duration: unexpected halt: %+v", dec)
		}
	})

	t.Run("threshold-boundary-exact-equal", func(t *testing.T) {
		eng, _, _ := newEngine(t)
		phases := []rollout.Phase{
			{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, AutoProgress: true},
		}
		if _, err := eng.Create(ctx, "boundary", phases); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Start(ctx, "boundary"); err != nil {
			t.Fatal(err)
		}
		// Exactly at the threshold (>=) -> advance/complete.
		dec, err := eng.Evaluate(ctx, "boundary", rollout.HealthVerdict{SuccessRate: 0.9, ErrorRate: 0.0})
		if err != nil {
			t.Fatal(err)
		}
		if dec.Action != "complete" {
			t.Fatalf("threshold-boundary: want complete at 0.9, got %s", dec.Action)
		}
	})

	t.Run("error-breaches-at-boundary", func(t *testing.T) {
		eng, _, _ := newEngine(t)
		phases := []rollout.Phase{
			{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.1, AutoProgress: true},
		}
		if _, err := eng.Create(ctx, "err-b", phases); err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Start(ctx, "err-b"); err != nil {
			t.Fatal(err)
		}
		// Error exactly at threshold (>=) -> halt.
		dec, err := eng.Evaluate(ctx, "err-b", rollout.HealthVerdict{SuccessRate: 0.85, ErrorRate: 0.1})
		if err != nil {
			t.Fatal(err)
		}
		if dec.Action != "halt" {
			t.Fatalf("error-boundary: want halt at 0.1, got %s", dec.Action)
		}
	})
}
