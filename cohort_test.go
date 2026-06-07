package rollout

import (
	"fmt"
	"testing"
)

// TestInCohortDeterminism: membership is a pure, stable function of
// (deviceID, deploymentID, percentage) — identical inputs always give identical
// results across many re-evaluations.
func TestInCohortDeterminism(t *testing.T) {
	const dep = "deployment-xyz"
	for i := 0; i < 500; i++ {
		dev := fmt.Sprintf("device-%d", i)
		for _, pct := range []int{1, 5, 10, 30, 50, 100} {
			first := InCohort(dev, dep, pct)
			for r := 0; r < 5; r++ {
				if got := InCohort(dev, dep, pct); got != first {
					t.Fatalf("non-deterministic membership for %s @ %d%%: %v then %v", dev, pct, first, got)
				}
			}
		}
	}
}

// TestInCohortBoundaries: 0% (and below) selects nobody, 100% (and above)
// selects everybody.
func TestInCohortBoundaries(t *testing.T) {
	tests := []struct {
		name string
		pct  int
		want bool
	}{
		{"zero selects nobody", 0, false},
		{"negative selects nobody", -10, false},
		{"hundred selects everybody", 100, true},
		{"over hundred selects everybody", 250, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 200; i++ {
				dev := fmt.Sprintf("dev-%d", i)
				if got := InCohort(dev, "dep", tt.pct); got != tt.want {
					t.Fatalf("%s: device %s got %v want %v", tt.name, dev, got, tt.want)
				}
			}
		})
	}
}

// TestInCohortMonotonicGrowth: a device that is a member at percentage p must
// remain a member at every q >= p — cohorts only grow as the rollout widens.
func TestInCohortMonotonicGrowth(t *testing.T) {
	const dep = "dep-mono"
	pcts := []int{1, 5, 10, 20, 30, 50, 70, 90, 100}
	for i := 0; i < 1000; i++ {
		dev := fmt.Sprintf("device-%d", i)
		enteredAt := -1
		for _, p := range pcts {
			in := InCohort(dev, dep, p)
			if in && enteredAt == -1 {
				enteredAt = p
			}
			if enteredAt != -1 && p >= enteredAt && !in {
				t.Fatalf("device %s left cohort: in at %d%% but out at %d%%", dev, enteredAt, p)
			}
		}
	}
}

// TestInCohortApproximatesPercentage: across many devices the selected fraction
// is close to the requested percentage (hash distributes reasonably). This is a
// sanity check, not a strict statistical claim.
func TestInCohortApproximatesPercentage(t *testing.T) {
	const dep = "dep-dist"
	const n = 10000
	for _, pct := range []int{5, 30, 60} {
		count := 0
		for i := 0; i < n; i++ {
			if InCohort(fmt.Sprintf("dev-%d", i), dep, pct) {
				count++
			}
		}
		frac := float64(count) / float64(n) * 100
		if frac < float64(pct)-5 || frac > float64(pct)+5 {
			t.Fatalf("pct=%d got selected fraction %.2f%% (n=%d), outside ±5%%", pct, frac, n)
		}
	}
}

// TestInCohortDeploymentIsolation: the same device can fall into different
// buckets for different deployments (separator prevents trivial coupling).
func TestInCohortDeploymentIsolation(t *testing.T) {
	differ := false
	for i := 0; i < 200; i++ {
		dev := fmt.Sprintf("dev-%d", i)
		if InCohort(dev, "depA", 50) != InCohort(dev, "depB", 50) {
			differ = true
			break
		}
	}
	if !differ {
		t.Fatal("expected cohort membership to vary across deployments for at least one device")
	}
}
