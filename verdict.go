package rollout

import (
	"errors"
	"fmt"
	"math"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// ErrVerdictRange is returned when a [HealthVerdict] carries rates outside the
// valid [0,1] fraction range.
var ErrVerdictRange = errors.New("rollout: health verdict rates must be fractions in [0,1]")

// HealthVerdict is the telemetry-derived health summary for the CURRENT phase
// cohort, supplied by the caller (telemetry_processing §4/§5). The engine does
// not ingest telemetry itself; it consumes this verdict through the port-style
// argument to [Engine.Evaluate].
//
// SuccessRate and ErrorRate are fractions in [0,1] computed over the cohort's
// devices that reached a terminal state (telemetry_processing §4):
//
//	success_rate = count(success) / count(terminal)
//	error_rate   = count(failure) / count(terminal)
//
// PostBootHealthFailed reports the post-boot health-window outcome (§6): when
// true the cohort failed its health window and the rollout must abort regardless
// of the raw rates.
type HealthVerdict struct {
	// SuccessRate is the cohort success fraction in [0,1].
	SuccessRate float64
	// ErrorRate is the cohort error/failure fraction in [0,1].
	ErrorRate float64
	// PostBootHealthFailed signals a post-boot health-window breach (§6).
	PostBootHealthFailed bool
}

// validate checks the verdict rates are well-formed fractions.
func (v HealthVerdict) validate() error {
	// NaN must be rejected explicitly: every ordered comparison against NaN is
	// false, so the range test below would pass a NaN rate. A NaN error_rate
	// (e.g. a degenerate 0/0 = count(failure)/count(terminal) with no terminal
	// devices) would then make `v.ErrorRate >= phase.ErrorThreshold` false and
	// silently bypass the error-threshold HALT — the engine's primary safety
	// invariant. (±Inf is already caught by the >1 / <0 bounds below.)
	if math.IsNaN(v.SuccessRate) || math.IsNaN(v.ErrorRate) ||
		v.SuccessRate < 0 || v.SuccessRate > 1 || v.ErrorRate < 0 || v.ErrorRate > 1 {
		return fmt.Errorf("%w: success=%v error=%v", ErrVerdictRange, v.SuccessRate, v.ErrorRate)
	}
	return nil
}

// Action is the decision the engine reaches for one evaluation.
type Action string

const (
	// ActionHalt stops the rollout (error threshold breach or post-boot failure).
	// It wins over advance under the safety invariant.
	ActionHalt Action = "halt"
	// ActionAdvance moves to the next phase (success bar met, AutoProgress on).
	ActionAdvance Action = "advance"
	// ActionHold keeps the rollout where it is, awaiting more telemetry, the
	// window to elapse, or an operator decision.
	ActionHold Action = "hold"
	// ActionComplete marks the rollout finished (final phase met its bar).
	ActionComplete Action = "complete"
)

// Reason explains why an [Action] was chosen, for auditability.
type Reason string

const (
	// ReasonErrorThreshold: error_rate >= error_threshold.
	ReasonErrorThreshold Reason = "error_threshold_breached"
	// ReasonPostBootFailed: the post-boot health window failed.
	ReasonPostBootFailed Reason = "post_boot_health_failed"
	// ReasonSuccessThreshold: success_rate >= success_threshold within duration.
	ReasonSuccessThreshold Reason = "success_threshold_met"
	// ReasonWindowOpen: the evaluation window has not elapsed yet.
	ReasonWindowOpen Reason = "evaluation_window_open"
	// ReasonWindowExpired: the window elapsed without meeting the success bar.
	ReasonWindowExpired Reason = "window_expired_below_threshold"
	// ReasonAutoProgressOff: success bar met but AutoProgress is disabled.
	ReasonAutoProgressOff Reason = "auto_progress_disabled"
	// ReasonNoActivePhase: the rollout is already in a terminal/non-active state.
	ReasonNoActivePhase Reason = "no_active_phase"
)

// Decision is the immutable result of [Engine.Evaluate]: the chosen action, the
// reason, the resulting status, and the device-deployment status the cohort
// devices map to (reusing the shared ota-protocol enum so consumers speak one
// vocabulary).
type Decision struct {
	// Action is what the engine decided to do.
	Action Action
	// Reason explains the action for audit/alerting.
	Reason Reason
	// Status is the rollout status after applying the action.
	Status Status
	// DeviceStatus maps the cohort outcome onto the shared per-device enum:
	// failure for a halt, success for completion, verifying while in flight.
	DeviceStatus otaprotocol.DeviceDeploymentStatus
}
