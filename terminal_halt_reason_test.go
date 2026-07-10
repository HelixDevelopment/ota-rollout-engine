package rollout

import (
	"context"
	"testing"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// ---------------------------------------------------------------------------
// Adversarial 2nd-pass audit (2026-07-10) — terminal-halt Reason misreport.
//
// DEFECT (pre-fix): Engine.Evaluate's idempotent terminal branch for a halted
// rollout returned a HARDCODED Reason=ReasonErrorThreshold regardless of WHY
// the rollout actually halted. A rollout can halt for two distinct reasons in
// decide():
//
//   - ReasonErrorThreshold  (error_rate >= error_threshold)
//   - ReasonPostBootFailed  (post-boot health-window failure, §6)
//
// The FIRST Evaluate that halts returns the correct Reason. But every
// SUBSEQUENT Evaluate on the now-halted rollout (routine control-plane polling
// / reconciliation) hit the terminal no-op branch and returned
// ReasonErrorThreshold — misreporting a post-boot-failure halt as an
// error-threshold breach. Decision.Reason is documented "for audit/alerting"
// (verdict.go), so downstream alerting keyed on the reason (boot-failure
// runbook vs error-budget dashboard) is fed the WRONG classification on every
// re-poll of a post-boot-halted deployment.
//
// This is a correctness bug in a documented output field (not a safety-invariant
// violation: halt still wins and DeviceStatus stays DeviceDeployFailed for both
// halt causes). It is reproduced deterministically below via the PUBLIC API.
// ---------------------------------------------------------------------------

// TestEvaluateTerminalHaltReasonReflectsPostBootFailure proves that once a
// rollout is halted by a post-boot health failure, re-evaluating it reports the
// ACTUAL halt reason (post_boot_health_failed), not a hardcoded
// error_threshold_breached.
func TestEvaluateTerminalHaltReasonReflectsPostBootFailure(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "pb-reason", goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, "pb-reason"); err != nil {
		t.Fatal(err)
	}

	// Halt via a post-boot health-window failure. The FIRST decision must (and
	// already does) carry ReasonPostBootFailed.
	first, err := eng.Evaluate(ctx, "pb-reason", HealthVerdict{SuccessRate: 1.0, ErrorRate: 0.0, PostBootHealthFailed: true})
	if err != nil {
		t.Fatal(err)
	}
	if first.Action != ActionHalt || first.Reason != ReasonPostBootFailed {
		t.Fatalf("first eval: got action=%q reason=%q want halt/post_boot_health_failed", first.Action, first.Reason)
	}

	// Re-evaluate the now-halted rollout (idempotent terminal no-op). The
	// reported Reason MUST still be the actual halt cause, not a hardcoded
	// error_threshold_breached.
	for i := 0; i < 3; i++ {
		again, err := eng.Evaluate(ctx, "pb-reason", HealthVerdict{SuccessRate: 1.0, ErrorRate: 0.0})
		if err != nil {
			t.Fatal(err)
		}
		if again.Action != ActionHalt || again.Status != StatusHalted {
			t.Fatalf("re-eval %d: got action=%q status=%q want halt/halted", i, again.Action, again.Status)
		}
		if again.DeviceStatus != otaprotocol.DeviceDeployFailed {
			t.Fatalf("re-eval %d: device status = %q want DeviceDeployFailed", i, again.DeviceStatus)
		}
		if again.Reason != ReasonPostBootFailed {
			t.Fatalf("re-eval %d: terminal halt reason = %q want %q; a post-boot-failure halt is being misreported as an error-threshold breach on re-poll (Decision.Reason is documented for audit/alerting)",
				i, again.Reason, ReasonPostBootFailed)
		}
	}
}

// TestEvaluateTerminalHaltReasonReflectsErrorThreshold is the companion guard:
// an error-threshold halt must keep reporting error_threshold_breached on
// re-evaluation. This pins the common case so the fix cannot regress it.
func TestEvaluateTerminalHaltReasonReflectsErrorThreshold(t *testing.T) {
	eng, _, _ := newEngine(t)
	ctx := context.Background()
	if _, err := eng.Create(ctx, "et-reason", goodPhases()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Start(ctx, "et-reason"); err != nil {
		t.Fatal(err)
	}

	first, err := eng.Evaluate(ctx, "et-reason", HealthVerdict{SuccessRate: 0.0, ErrorRate: 0.9})
	if err != nil {
		t.Fatal(err)
	}
	if first.Action != ActionHalt || first.Reason != ReasonErrorThreshold {
		t.Fatalf("first eval: got action=%q reason=%q want halt/error_threshold_breached", first.Action, first.Reason)
	}
	again, err := eng.Evaluate(ctx, "et-reason", HealthVerdict{SuccessRate: 1.0, ErrorRate: 0.0})
	if err != nil {
		t.Fatal(err)
	}
	if again.Reason != ReasonErrorThreshold {
		t.Fatalf("re-eval: terminal halt reason = %q want %q (error-threshold halt must keep its reason on re-poll)",
			again.Reason, ReasonErrorThreshold)
	}
}
