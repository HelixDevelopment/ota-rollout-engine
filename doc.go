// Package rollout implements an OS-agnostic, I/O-free staged-rollout engine for
// Helix OTA (spec 1.0.1 §2, telemetry_processing §5).
//
// The engine turns an all-at-once delivery into a sequence of percentage-gated
// phases, each guarded by success/error thresholds and a duration. It decides,
// from a health verdict supplied by the caller, whether to HALT, ADVANCE, or
// HOLD a rollout — enforcing the safety invariant that halt wins over advance
// when both could trigger in the same evaluation window.
//
// # Purity (HelixConstitution §11.4.28 decoupling)
//
// This package contains NO HTTP, NO database, NO disk, NO network and NO OS
// specifics. All side effects are expressed through two ports:
//
//   - [StoragePort] persists and loads rollout state.
//   - [Clock] supplies the current time (so durations are deterministically
//     testable with a fake clock).
//
// Telemetry is never ingested here: the caller computes a [HealthVerdict] from
// its own telemetry pipeline and hands it to [Engine.Evaluate]. The engine
// depends on the Go standard library plus github.com/HelixDevelopment/ota-protocol
// for shared status enums only.
//
// # Cohort selection
//
// Cohort membership is deterministic and stable: a device is in the cohort of a
// phase when hash(deviceID+deploymentID) mod 100 < cumulativePercentage. Because
// the hash is fixed and percentages are monotonically non-decreasing across
// ordered phases, a device that enters the rollout never leaves it as the
// rollout widens (monotonic cohort growth).
package rollout
