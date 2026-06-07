package rollout

import "hash/fnv"

// cohortModulus is the granularity of percentage-based cohort selection. A
// device's stable bucket is hash(...) mod cohortModulus, compared against the
// cumulative percentage (0..100). Using 100 means percentages map 1:1 to
// buckets.
const cohortModulus = 100

// deviceBucket returns the stable bucket in [0, cohortModulus) for a device in a
// given deployment. The bucket is a pure function of (deviceID, deploymentID):
// it never depends on time, phase, or call order, which is what makes cohort
// membership deterministic and stable across re-evaluation.
//
// FNV-1a (stdlib hash/fnv) is used because it is deterministic across processes
// and architectures — unlike Go's built-in map hash — so a device computes the
// same bucket on every server and every run.
func deviceBucket(deviceID, deploymentID string) uint32 {
	h := fnv.New32a()
	// Write order is fixed; a separator prevents ("ab","c") and ("a","bc")
	// from colliding into the same byte stream.
	_, _ = h.Write([]byte(deviceID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(deploymentID))
	return h.Sum32() % cohortModulus
}

// InCohort reports whether the device belongs to the cohort covered by
// cumulativePercentage for the given deployment. A device is in the cohort when
// its stable bucket is strictly less than the cumulative percentage:
//
//	bucket < cumulativePercentage  ⇒  member
//
// cumulativePercentage is clamped to [0, 100]; 0 selects nobody and 100 selects
// everybody. Membership is monotonic in cumulativePercentage: if a device is a
// member at percentage p it is a member at every q >= p, which guarantees
// monotonic cohort growth as a rollout advances through its ordered phases.
func InCohort(deviceID, deploymentID string, cumulativePercentage int) bool {
	if cumulativePercentage <= 0 {
		return false
	}
	if cumulativePercentage >= cohortModulus {
		return true
	}
	return int(deviceBucket(deviceID, deploymentID)) < cumulativePercentage
}
