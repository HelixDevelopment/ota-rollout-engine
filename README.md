# ota-rollout-engine

| Field | Value |
|---|---|
| Revision | 1 |
| Created | 2026-06-07 |
| Status | scaffold |
| Part of | [Helix OTA](https://github.com/HelixDevelopment/helix_ota) |
| Language | go |
| License | Apache-2.0 |

## Purpose

OS-agnostic staged-rollout engine: percentage cohorts (5/10/30..100), success/error thresholds, halt/advance, deterministic cohort selection.

## Boundary (decoupling)

No HTTP, no OS specifics; a pure engine over a storage port + telemetry port. Reusable for any fleet rollout.

This is a **reusable, independently versioned** building brick (HelixConstitution
§11.4.28 submodules-as-equal-codebase). It is consumed by Helix OTA and is designed
to be reusable by other projects. It must ship in-depth documentation, user guides,
and full test coverage (§1 four-layer) before leaving `scaffold` status.

## Status

Scaffold. Implementation tracked in the Helix OTA spec corpus
(`docs/research/main_specs/`). See the master design and the submodule reuse map.

## Mirrors

- GitHub: https://github.com/HelixDevelopment/ota-rollout-engine
- GitLab: https://gitlab.com/helixdevelopment1/ota-rollout-engine
