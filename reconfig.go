package quorum

import (
	"slices"
	"strings"
)

// ballotMode selects which sets of acknowledgement count toward commitment.
type ballotMode int

const (
	// ballotNormal requires one majority of the active voters.
	ballotNormal ballotMode = iota
	// ballotJoint requires both an old-config majority and a new-config
	// majority. The two majorities always intersect, so two disjoint groups
	// cannot each commit while a change is in progress.
	ballotJoint
)

func majorityOf(n int) int { return n/2 + 1 }

// quorumForLocked reports the single-number quorum used for "can the shard
// form a majority at all" diagnostics in each mode.
func (s *ReplicaSet) quorumForLocked(mode ballotMode) int {
	switch mode {
	case ballotJoint:
		return majorityOf(len(s.oldVoters))
	default:
		return majorityOf(len(s.voters))
	}
}

// activeBallotModeLocked exposes the mode the next commit must satisfy.
func (s *ReplicaSet) activeBallotModeLocked() ballotMode {
	if s.joint {
		return ballotJoint
	}
	return ballotNormal
}

// reachableVotersLocked lists voting replicas reachable from id in the given
// membership list (id included when alive).
func (s *ReplicaSet) reachableMembersLocked(from string, members []string) []string {
	out := make([]string, 0, len(members))
	for _, id := range members {
		if s.reachableLocked(from, id) {
			out = append(out, id)
		}
	}
	return out
}

func containsID(ids []string, id string) bool {
	return slices.Contains(ids, id)
}

// saveManifestLocked flushes the current durable membership configuration.
func (s *ReplicaSet) saveManifestLocked() error {
	if s.persister == nil || !s.persister.enabled() {
		return nil
	}
	return s.persister.saveManifest(shardManifest{
		ShardID:   s.spec.ID,
		Voters:    append([]string(nil), s.voters...),
		Learners:  append([]string(nil), s.learners...),
		Joint:     s.joint,
		OldVoters: append([]string(nil), s.oldVoters...),
		NewVoters: append([]string(nil), s.newVoters...),
	})
}

// expandReplicasLocked is implemented in reconfig_expand.go.
func validateNewReplicas(existing []string, added []string) error {
	if len(added) == 0 {
		return newError(KindInvalidArgument, "expand", "", "no new replicas supplied")
	}
	seen := make(map[string]bool, len(existing)+len(added))
	for _, id := range existing {
		if strings.TrimSpace(id) == "" {
			return newError(KindInvalidConfig, "expand", "", "existing replica id is empty")
		}
		if seen[id] {
			return newError(KindInvalidConfig, "expand", "", "duplicate existing replica id "+id)
		}
		seen[id] = true
	}
	for _, id := range added {
		if strings.TrimSpace(id) == "" {
			return newError(KindInvalidConfig, "expand", "", "new replica id is empty")
		}
		if seen[id] {
			return newError(KindInvalidConfig, "expand", "", "replica "+id+" is already a member")
		}
		seen[id] = true
	}
	if len(existing)+len(added) <= len(existing) {
		return newError(KindInvalidConfig, "expand", "", "expansion must increase the replica count")
	}
	if len(existing)+len(added) < 3 {
		return newError(KindInvalidConfig, "expand", "", "expanded replica set must have at least three members")
	}
	return nil
}
