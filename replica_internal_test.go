package quorum

import "testing"

func TestOverwriteCommittedPrefixTruncatesSameTermDivergentTail(t *testing.T) {
	r := NewReplica("r", "s")
	r.mu.Lock()
	defer r.mu.Unlock()

	r.appendEntryLocked(Entry{Index: 1, Term: 1, RequestID: "admin", Admin: true})
	if err := r.applyCommittedLocked(1); err != nil {
		t.Fatal(err)
	}
	r.appendEntryLocked(Entry{Index: 2, Term: 1, RequestID: "stale", Key: "k", Value: []byte("old")})

	leader := []Entry{
		{Index: 1, Term: 1, RequestID: "admin", Admin: true},
		{Index: 2, Term: 1, RequestID: "current", Key: "k", Value: []byte("new")},
	}
	if err := r.overwriteCommittedLocked(leader, 2); err != nil {
		t.Fatalf("overwrite committed prefix: %v", err)
	}
	if got := string(r.data["k"].Data); got != "new" {
		t.Fatalf("divergent uncommitted command survived catch-up: got %q want new", got)
	}
	if _, ok := r.results["stale"]; ok {
		t.Fatal("stale request ID was committed during catch-up")
	}
	if result, ok := r.results["current"]; !ok || string(result.value) != "new" {
		t.Fatalf("current request result was not committed: %+v ok=%t", result, ok)
	}
}

func TestAppendEntriesTruncatesDivergentEntryImmediatelyAfterCommit(t *testing.T) {
	r := NewReplica("r", "s")
	r.mu.Lock()
	defer r.mu.Unlock()

	r.appendEntryLocked(Entry{Index: 1, Term: 1, RequestID: "committed", Key: "k", Value: []byte("v1")})
	if err := r.applyCommittedLocked(1); err != nil {
		t.Fatal(err)
	}
	r.appendEntryLocked(Entry{Index: 2, Term: 1, RequestID: "stale-uncommitted", Key: "k", Value: []byte("stale")})

	next := []Entry{{Index: 2, Term: 2, RequestID: "current", Key: "k", Value: []byte("v2")}}
	if !r.appendEntriesLocked(2, 1, 1, 0, next) {
		t.Fatal("append entries rejected a divergent uncommitted suffix immediately after commit")
	}
	if uint64(len(r.log)) != 2 || r.log[1].RequestID != "current" {
		t.Fatalf("divergent suffix was not replaced: %+v", r.log)
	}
	if r.commitIndex != 1 || r.data["k"].Version != 1 || string(r.data["k"].Data) != "v1" {
		t.Fatalf("uncommitted replacement changed committed state: commit=%d value=%+v", r.commitIndex, r.data["k"])
	}
}
