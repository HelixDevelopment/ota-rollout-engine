package rollout

import (
	"context"
	"errors"
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// Wave-8 adversarial audit — IEEE-754 NaN range-check defect (§11.4.107 class
// "float NaN range checks: NaN<0 && NaN>1 both false").
//
// Two production validators reject out-of-range floats with the idiom
//
//	if x < lo || x > hi { return errRange }
//
// which is CORRECT for finite numbers but SILENTLY ACCEPTS NaN, because every
// ordered comparison against NaN is false (NaN<lo == false AND NaN>hi ==
// false). The two loci:
//
//   - types.go validatePhases: a Phase.SuccessThreshold / Phase.ErrorThreshold
//     of NaN passes validation, so Create() persists it.
//   - verdict.go HealthVerdict.validate: a NaN SuccessRate / ErrorRate passes
//     validation, so Evaluate() acts on it.
//
// The SAFETY HARM is concrete: decide() gates the halt on
// `v.ErrorRate >= phase.ErrorThreshold`. If EITHER operand is NaN the
// comparison is false, so the error-threshold HALT — the engine's documented
// primary safety invariant ("halt wins over advance ... when in doubt, stop")
// — is silently bypassed. A NaN error_rate is a realistic input: the caller
// computes error_rate = count(failure)/count(terminal); when count(terminal)
// is 0 that is 0.0/0.0 == NaN in Go float64.
//
// These tests exercise the PUBLIC API (Create/Evaluate). On the pre-fix code
// they FAIL because the NaN is accepted (no ErrThresholdRange / ErrVerdictRange
// is returned) and the logged Decision proves the halt was bypassed. After the
// validators add math.IsNaN guards they PASS.
// ---------------------------------------------------------------------------

// TestValidatePhasesRejectsNaNThresholds proves Create must reject a phase
// whose success/error threshold is NaN. A NaN threshold silently disables the
// halt (error) or advance/complete (success) comparison for the whole rollout.
func TestValidatePhasesRejectsNaNThresholds(t *testing.T) {
	cases := []struct {
		name  string
		phase Phase
	}{
		{"NaN error threshold", Phase{Percentage: 100, SuccessThreshold: 0.9, ErrorThreshold: math.NaN(), AutoProgress: true}},
		{"NaN success threshold", Phase{Percentage: 100, SuccessThreshold: math.NaN(), ErrorThreshold: 0.1, AutoProgress: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			eng, _, _ := newEngine(t)
			_, err := eng.Create(context.Background(), "dep-nan", []Phase{c.phase})
			if !errors.Is(err, ErrThresholdRange) {
				t.Fatalf("Create accepted a phase with a %s (err=%v); a NaN threshold makes every >= comparison in decide() false, silently disabling the halt/advance safety gate for the whole rollout — validatePhases must reject it with ErrThresholdRange",
					c.name, err)
			}
		})
	}
}

// TestEvaluateRejectsNaNVerdictRates proves Evaluate must reject a health
// verdict carrying a NaN rate. With a NaN error_rate the error-threshold HALT
// is silently bypassed (the exact "when in doubt, stop" invariant the engine
// promises), so a degenerate 0/0 telemetry rate must be rejected, not acted on.
func TestEvaluateRejectsNaNVerdictRates(t *testing.T) {
	cases := []struct {
		name string
		v    HealthVerdict
	}{
		{"NaN error rate", HealthVerdict{SuccessRate: 0.0, ErrorRate: math.NaN()}},
		{"NaN success rate", HealthVerdict{SuccessRate: math.NaN(), ErrorRate: 0.0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			eng, _, _ := newEngine(t)
			ctx := context.Background()
			if _, err := eng.Create(ctx, "dep1", goodPhases()); err != nil {
				t.Fatal(err)
			}
			if _, err := eng.Start(ctx, "dep1"); err != nil {
				t.Fatal(err)
			}
			dec, err := eng.Evaluate(ctx, "dep1", c.v)
			if !errors.Is(err, ErrVerdictRange) {
				t.Fatalf("Evaluate accepted a verdict with a %s (err=%v, decision=%+v); a NaN rate makes v.ErrorRate>=ErrorThreshold false so the error-threshold HALT is silently bypassed — HealthVerdict.validate must reject it with ErrVerdictRange",
					c.name, err, dec)
			}
		})
	}
}
