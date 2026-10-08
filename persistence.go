package quorum

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// replicaSnapshot is one replica's durable state. Only the committed log
// prefix is ever snapshotted: an entry that appeared on a minority but was
// never acknowledged by a quorum must not come back as committed data after a
// full restart.
type replicaSnapshot struct {
	ID          string
	CurrentTerm uint64
	VotedFor    string
	CommitIndex uint64
	Entries     []Entry
}

// shardManifest records the membership configuration last made durable. It is
// written only after a configuration entry has been committed by the quorum
// that was active at commit time, so the manifest can never jump ahead of the
// replicated log.
type shardManifest struct {
	ShardID   string   `json:"ShardID"`
	Voters    []string `json:"Voters"`
	Learners  []string `json:"Learners,omitempty"`
	Joint     bool     `json:"Joint,omitempty"`
	OldVoters []string `json:"OldVoters,omitempty"`
	NewVoters []string `json:"NewVoters,omitempty"`
}

// persistence owns the on-disk layout for one store's data directory.
//
// Layout (all writes are temp-file + rename, hence atomic with respect to a
// process crash):
//
//	<dir>/shards/<safe-shard-id>/manifest.json
//	<dir>/shards/<safe-shard-id>/replicas/<safe-replica-id>.json
type persistence struct {
	dir string
}

func newPersistence(dir string) *persistence {
	return &persistence{dir: dir}
}

func (p *persistence) enabled() bool {
	return p != nil && p.dir != ""
}

// safeName keeps file names human-inspectable while encoding characters that
// are awkward on local file systems (separators, control bytes, ...).
func safeName(in string) string {
	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		c := in[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.' || c == '-' || c == '_':
			out = append(out, c)
		default:
			out = append(out, []byte(fmt.Sprintf("%%%02X", c))...)
		}
	}
	return string(out)
}

func (p *persistence) shardDir(shardID string) string {
	return filepath.Join(p.dir, "shards", safeName(shardID))
}

func (p *persistence) replicaPath(shardID, replicaID string) string {
	return filepath.Join(p.shardDir(shardID), "replicas", safeName(replicaID)+".json")
}

func (p *persistence) manifestPath(shardID string) string {
	return filepath.Join(p.shardDir(shardID), "manifest.json")
}

// writeAtomic serializes JSON and renames it into place after fsyncing the
// temp file, so a crash mid-write never leaves a half-written state file.
func writeAtomic(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return false, err
	}
	return true, nil
}

func (p *persistence) saveReplica(shardID string, snap replicaSnapshot) error {
	if !p.enabled() {
		return nil
	}
	return writeAtomic(p.replicaPath(shardID, snap.ID), snap)
}

// loadReplica returns ok=false when the replica has no snapshot yet.
func (p *persistence) loadReplica(shardID, replicaID string) (snap replicaSnapshot, ok bool, err error) {
	if !p.enabled() {
		return replicaSnapshot{}, false, nil
	}
	snap, err = replicaSnapshot{}, nil
	ok, err = readJSON(p.replicaPath(shardID, replicaID), &snap)
	return snap, ok, err
}

func (p *persistence) saveManifest(m shardManifest) error {
	if !p.enabled() {
		return nil
	}
	return writeAtomic(p.manifestPath(m.ShardID), m)
}

func (p *persistence) loadManifest(shardID string) (m shardManifest, ok bool, err error) {
	if !p.enabled() {
		return shardManifest{}, false, nil
	}
	m, err = shardManifest{}, nil
	ok, err = readJSON(p.manifestPath(shardID), &m)
	return m, ok, err
}
