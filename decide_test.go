package rollout

import (
	"testing"
	"time"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

var baseTime = time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

// TestDecide is the table-driven core covering every branch of the pure decision
// function, including the threshold boundaries and the halt-wins invariant.
func TestDecide(t *testing.T) {
	phase := func(success, errTh float64, dur time.Duration, auto bool) Phase {
		return Phase{Percentage: 50, SuccessThreshold: success, ErrorThreshold: errTh, Duration: dur, AutoProgress: auto}
	}

	tests := []struct {
		name       string
		phase      Phase
		verdict    HealthVerdict
		started    time.Time
		now        time.Time
		finalPhase bool
		wantAction Action
		wantReason Reason
		wantDevice otaprotocol.DeviceDeploymentStatus
	}{
		{
			name:    "post-boot failure halts even with perfect success",
			phase:   phase(0.9, 0.1, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 1.0, ErrorRate: 0.0, PostBootHealthFailed: true},
			started: baseTime, now: baseTime,
			wantAction: ActionHalt, wantReason: ReasonPostBootFailed, wantDevice: otaprotocol.DeviceDeployFailed,
		},
		{
			name:    "SAFETY INVARIANT halt wins over advance when both trigger",
			phase:   phase(0.9, 0.2, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.25}, // both bars crossed
			started: baseTime, now: baseTime,
			wantAction: ActionHalt, wantReason: ReasonErrorThreshold, wantDevice: otaprotocol.DeviceDeployFailed,
		},
		{
			name:    "error rate exactly at threshold is a breach (>=)",
			phase:   phase(0.9, 0.2, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 0.0, ErrorRate: 0.2},
			started: baseTime, now: baseTime,
			wantAction: ActionHalt, wantReason: ReasonErrorThreshold, wantDevice: otaprotocol.DeviceDeployFailed,
		},
		{
			name:    "error rate just below threshold does not halt (holds, window open)",
			phase:   phase(0.9, 0.2, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 0.0, ErrorRate: 0.199},
			started: baseTime, now: baseTime,
			wantAction: ActionHold, wantReason: ReasonWindowOpen, wantDevice: otaprotocol.DeviceDeployVerifying,
		},
		{
			name:    "success exactly at threshold advances (auto, non-final)",
			phase:   phase(0.9, 0.2, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 0.9, ErrorRate: 0.0},
			started: baseTime, now: baseTime,
			wantAction: ActionAdvance, wantReason: ReasonSuccessThreshold, wantDevice: otaprotocol.DeviceDeployVerifying,
		},
		{
			name:    "success bar met on final phase completes",
			phase:   phase(0.9, 0.2, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 0.99, ErrorRate: 0.0},
			started: baseTime, now: baseTime, finalPhase: true,
			wantAction: ActionComplete, wantReason: ReasonSuccessThreshold, wantDevice: otaprotocol.DeviceDeploySuccess,
		},
		{
			name:    "success bar met but auto-progress off holds for operator",
			phase:   phase(0.9, 0.2, time.Hour, false),
			verdict: HealthVerdict{SuccessRate: 0.95, ErrorRate: 0.0},
			started: baseTime, now: baseTime,
			wantAction: ActionHold, wantReason: ReasonAutoProgressOff, wantDevice: otaprotocol.DeviceDeployVerifying,
		},
		{
			name:    "bar not met, window still open -> hold (window open)",
			phase:   phase(0.9, 0.2, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 0.5, ErrorRate: 0.0},
			started: baseTime, now: baseTime.Add(30 * time.Minute),
			wantAction: ActionHold, wantReason: ReasonWindowOpen, wantDevice: otaprotocol.DeviceDeployVerifying,
		},
		{
			name:    "bar not met, window expired -> hold (window expired)",
			phase:   phase(0.9, 0.2, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 0.5, ErrorRate: 0.0},
			started: baseTime, now: baseTime.Add(2 * time.Hour),
			wantAction: ActionHold, wantReason: ReasonWindowExpired, wantDevice: otaprotocol.DeviceDeployVerifying,
		},
		{
			name:    "window expiry boundary: exactly at duration is expired",
			phase:   phase(0.9, 0.2, time.Hour, true),
			verdict: HealthVerdict{SuccessRate: 0.5, ErrorRate: 0.0},
			started: baseTime, now: baseTime.Add(time.Hour),
			wantAction: ActionHold, wantReason: ReasonWindowExpired, wantDevice: otaprotocol.DeviceDeployVerifying,
		},
		{
			name:    "zero duration never expires, holds open until bar met",
			phase:   phase(0.9, 0.2, 0, true),
			verdict: HealthVerdict{SuccessRate: 0.5, ErrorRate: 0.0},
			started: baseTime, now: baseTime.Add(1000 * time.Hour),
			wantAction: ActionHold, wantReason: ReasonWindowOpen, wantDevice: otaprotocol.DeviceDeployVerifying,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decide(tt.phase, tt.verdict, tt.started, tt.now, tt.finalPhase)
			if got.Action != tt.wantAction {
				t.Errorf("action = %q want %q", got.Action, tt.wantAction)
			}
			if got.Reason != tt.wantReason {
				t.Errorf("reason = %q want %q", got.Reason, tt.wantReason)
			}
			if got.DeviceStatus != tt.wantDevice {
				t.Errorf("device status = %q want %q", got.DeviceStatus, tt.wantDevice)
			}
		})
	}
}
