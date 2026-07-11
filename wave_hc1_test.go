package rollout

import (
	"testing"
)

// TestDecide_HC1_ZeroErrorThreshold is the permanent HC-1 regression guard.
// ErrorThreshold==0 is a LEGAL "zero tolerance" value (validatePhases only
// rejects <0 / >1 / NaN). It must mean "halt on ANY observed failure", NOT
// "always halt". Before the fix, `v.ErrorRate >= phase.ErrorThreshold` with
// both operands 0 evaluated `0 >= 0 == true`, halting a perfectly healthy
// final phase as FAILED and making the success/advance path unreachable for
// every zero-tolerance rollout (§11.4.108: a healthy rollout reported FAILED).
//
// Anti-tautology anchor: reverting `v.ErrorRate > 0 && v.ErrorRate >= ...`
// back to `v.ErrorRate >= ...` flips the zero-error subtest RED.
func TestDecide_HC1_ZeroErrorThreshold(t *testing.T) {
	phase := Phase{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: 0.0, AutoProgress: true}

	// Healthy final phase: 0% errors, success bar met -> COMPLETE, not HALT.
	healthy := HealthVerdict{SuccessRate: 1.0, ErrorRate: 0.0}
	if d := decide(phase, healthy, baseTime, baseTime, true); d.Action != ActionComplete {
		t.Fatalf("HC-1: ErrorThreshold=0 + 0%% errors + success bar met must COMPLETE; got Action=%v Reason=%v Status=%v",
			d.Action, d.Reason, d.Status)
	}

	// Positive control: ErrorThreshold=0 still halts the instant ANY failure appears.
	withErr := HealthVerdict{SuccessRate: 1.0, ErrorRate: 0.01}
	if d := decide(phase, withErr, baseTime, baseTime, true); d.Action != ActionHalt || d.Reason != ReasonErrorThreshold {
		t.Fatalf("HC-1: ErrorThreshold=0 must HALT on ANY observed error (0.01); got Action=%v Reason=%v",
			d.Action, d.Reason)
	}
}
