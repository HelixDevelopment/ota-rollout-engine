package rollout

import (
	"time"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// decide is the pure decision core of the engine. Given an active phase, the
// caller's health verdict, when the phase started, the evaluation instant, and
// whether this is the final phase, it returns the [Decision] with NO side
// effects and no I/O. Keeping it pure makes the safety invariant exhaustively
// table-testable.
//
// The ordering below IS the safety invariant: the error-threshold (and post-boot
// failure) branches are evaluated BEFORE the success branch, so when both the
// error and success thresholds are simultaneously satisfiable in one window the
// engine HALTS rather than ADVANCES. telemetry_processing §5: "halt wins over
// advance ... when in doubt, stop."
func decide(phase Phase, v HealthVerdict, phaseStartedAt, now time.Time, finalPhase bool) Decision {
	// 1. Post-boot health-window failure -> abort (HALT). (§6)
	if v.PostBootHealthFailed {
		return Decision{
			Action: ActionHalt, Reason: ReasonPostBootFailed, Status: StatusHalted,
			DeviceStatus: otaprotocol.DeviceDeployFailed,
		}
	}

	// 2. SAFETY INVARIANT: error threshold breach -> HALT, checked before any
	// advance path so a concurrent success+error breach always halts. The
	// comparison is >= so a rate exactly at the threshold is a breach — BUT an
	// ErrorRate of 0 never breaches (HC-1): ErrorThreshold==0 is a LEGAL value
	// (validatePhases only rejects <0 / >1 / NaN) meaning "zero tolerance — halt
	// on ANY observed failure", NOT "always halt". Without the `> 0` guard a
	// perfectly healthy final phase (ErrorRate 0.0, ErrorThreshold 0.0) would
	// halt-as-failed on `0 >= 0`, making the success/advance path unreachable
	// for every zero-tolerance rollout (§11.4.108: a healthy rollout reported
	// permanently FAILED).
	if v.ErrorRate > 0 && v.ErrorRate >= phase.ErrorThreshold {
		return Decision{
			Action: ActionHalt, Reason: ReasonErrorThreshold, Status: StatusHalted,
			DeviceStatus: otaprotocol.DeviceDeployFailed,
		}
	}

	// 3. Success bar met (>= threshold) -> complete / advance / hold.
	if v.SuccessRate >= phase.SuccessThreshold {
		if finalPhase {
			return Decision{
				Action: ActionComplete, Reason: ReasonSuccessThreshold, Status: StatusCompleted,
				DeviceStatus: otaprotocol.DeviceDeploySuccess,
			}
		}
		if phase.AutoProgress {
			return Decision{
				Action: ActionAdvance, Reason: ReasonSuccessThreshold, Status: StatusActive,
				DeviceStatus: otaprotocol.DeviceDeployVerifying,
			}
		}
		// Bar met but operator must advance.
		return Decision{
			Action: ActionHold, Reason: ReasonAutoProgressOff, Status: StatusHeld,
			DeviceStatus: otaprotocol.DeviceDeployVerifying,
		}
	}

	// 4. Bar not met yet: if the window is still open, keep waiting (HOLD).
	// A zero Duration means "no time bound" so the window is never considered
	// expired; the phase simply holds until the success bar is met.
	if phase.Duration == 0 || !windowExpired(phaseStartedAt, now, phase.Duration) {
		return Decision{
			Action: ActionHold, Reason: ReasonWindowOpen, Status: StatusActive,
			DeviceStatus: otaprotocol.DeviceDeployVerifying,
		}
	}

	// 5. Window elapsed without meeting the bar -> HOLD for operator (§5
	// "success_rate < success_threshold at duration expiry").
	return Decision{
		Action: ActionHold, Reason: ReasonWindowExpired, Status: StatusHeld,
		DeviceStatus: otaprotocol.DeviceDeployVerifying,
	}
}

// windowExpired reports whether the evaluation window of length d that began at
// start has elapsed by now. With a zero start time (phase not stamped) the
// window is treated as not yet expired.
func windowExpired(start, now time.Time, d time.Duration) bool {
	if start.IsZero() {
		return false
	}
	return !now.Before(start.Add(d))
}
