# ota-rollout-engine — CLAUDE.md

## INHERITED FROM Helix Constitution

This module is a submodule of a project that includes the Helix
Constitution submodule. All rules in `constitution/CLAUDE.md` and the
`constitution/Constitution.md` it references apply unconditionally.
Locate the constitution submodule from any arbitrary nested depth using
its `find_constitution.sh` helper (it walks up parent directories and
follows the git superproject pointer — no hardcoded depth).

Canonical reference: https://github.com/HelixDevelopment/HelixConstitution

Project-specific rules below extend the universal rules; they never
weaken any universal clause. When this file disagrees with the
constitution, the constitution wins.

## Module overview

`ota-rollout-engine` is a reusable Helix OTA building brick, consumed by the
`helix_ota` control plane and designed to be reusable by other
projects. See `README` for its API, tests, and usage.
