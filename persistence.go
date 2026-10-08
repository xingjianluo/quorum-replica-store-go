package quorum

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
)

// durableResult 是 committedResult 的落盘形态。
type durableResult struct {
	RequestID      string  `json:"request_id"`
	Key            string  `json:"key"`
	Value          []byte  `json:"value"`
	CompareVersion *uint64 `json:"compare_version,omitempty"`
	Version        uint64  `json:"version"`
	CommitIndex    uint64  `json:"commit_index"`
	CommitTerm     uint64  `json:"commit_term"`
}

// durableFormatVersion 标记镜像格式，未来不兼容变更时必须提升。
const durableFormatVersion = 1

// DurableRecord 是单个副本某一时刻的完整持久化镜像。
//
// 不变量：
//   - Log 只保留提交前缀（len(Log) == CommitIndex）。只在少数派上短暂
//     存在、从未对外确认的条目永远不进镜像。
//   - Data 与 Results 只由 Log 中的已提交用户条目物化而来。
//   - CurrentTerm/VotedFor 是 Raft 硬状态，重启后必须原样恢复，避免同
//     一任期内重复投票导致两个多数派各自选出主。
type DurableRecord struct {
	FormatVersion int                      `json:"format_version"`
	ShardID       string                   `json:"shard_id"`
	ReplicaID     string                   `json:"replica_id"`
	CurrentTerm   uint64                   `json:"current_term"`
	VotedFor      string                   `json:"voted_for"`
	CommitIndex   uint64                   `json:"commit_index"`
	Log           []Entry                  `json:"log"`
	Data          map[string]Value         `json:"data"`
	Results       map[string]durableResult `json:"results"`
}

func cloneDurableRecord(in DurableRecord) DurableRecord {
	out := in
	if in.Log != nil {
		out.Log = make([]Entry, len(in.Log))
		for i := range in.Log {
			out.Log[i] = cloneEntry(in.Log[i])
		}
	}
	if in.Data != nil {
		out.Data = make(map[string]Value, len(in.Data))
		for k, v := range in.Data {
			v.Data = cloneBytes(v.Data)
			out.Data[k] = v
		}
	}
	if in.Results != nil {
		out.Results = make(map[string]durableResult, len(in.Results))
		for k, v := range in.Results {
			v.Value = cloneBytes(v.Value)
			v.CompareVersion = cloneUint64Ptr(v.CompareVersion)
			out.Results[k] = v
		}
	}
	return out
}

// durability 抽象副本集对持久化介质的依赖；nil 表示纯内存模式。
type durability interface {
	enabled() bool
	save(shardID string, rec DurableRecord) error
	loadShard(shardID string, bootstrap []string) (map[string]DurableRecord, []string, error)
}

// diskDurability 把每个副本镜像写到 <root>/<shard>/<replica>.json，
// 通过临时文件 + rename 保证单个镜像要么是旧文件要么是新文件。
type diskDurability struct {
	root string
}

func newDiskDurability(root string) (*diskDurability, error) {
	if root == "" {
		return nil, newError(KindInvalidConfig, "durability", "", "data directory is empty")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, newError(KindDurability, "durability", "",
			"cannot create data directory "+root).wrap(err)
	}
	return &diskDurability{root: root}, nil
}

func (d *diskDurability) enabled() bool { return true }

func (d *diskDurability) shardDir(shardID string) string {
	return filepath.Join(d.root, shardDirName(shardID))
}

func shardDirName(shardID string) string {
	return "shard-" + url.PathEscape(shardID)
}

func replicaFileName(replicaID string) string {
	return url.PathEscape(replicaID) + ".json"
}

func (d *diskDurability) save(shardID string, rec DurableRecord) error {
	if rec.ShardID != shardID {
		return newError(KindDurability, "durability", shardID,
			"record shard "+rec.ShardID+" does not match target "+shardID)
	}
	dir := d.shardDir(shardID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return newError(KindDurability, "durability", shardID, "mkdir failed").wrap(err)
	}
	payload, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return newError(KindDurability, "durability", shardID, "marshal failed").wrap(err)
	}
	final := filepath.Join(dir, replicaFileName(rec.ReplicaID))
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return newError(KindDurability, "durability", shardID,
			"write temp image failed").wrap(err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return newError(KindDurability, "durability", shardID,
			"publish image via rename failed").wrap(err)
	}
	return nil
}

// loadShard 读取一个分片目录下的全部镜像，并以提交最完整（日志最长）的
// 镜像为准重放成员变更条目，还原最后一次已提交配置中的完整成员顺序。
// bootstrap 提供初始成员及顺序；扩容加入的副本即使尚未被初始配置提及也
// 会从磁盘发现。
func (d *diskDurability) loadShard(shardID string, bootstrap []string) (map[string]DurableRecord, []string, error) {
	dir := d.shardDir(shardID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, append([]string(nil), bootstrap...), nil
		}
		return nil, nil, newError(KindDurability, "restore", shardID,
			"cannot read data directory "+dir).wrap(err)
	}
	records := make(map[string]DurableRecord, len(entries))
	var authoritative *DurableRecord
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		payload, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
		if readErr != nil {
			return nil, nil, newError(KindDurability, "restore", shardID,
				"cannot read image "+entry.Name()).wrap(readErr)
		}
		var rec DurableRecord
		if jsonErr := json.Unmarshal(payload, &rec); jsonErr != nil {
			return nil, nil, newError(KindDurability, "restore", shardID,
				"cannot parse image "+entry.Name()).wrap(jsonErr)
		}
		if rec.ShardID != shardID {
			return nil, nil, newError(KindDurability, "restore", shardID,
				"image "+entry.Name()+" belongs to shard "+rec.ShardID)
		}
		if rec.FormatVersion != durableFormatVersion {
			return nil, nil, newError(KindDurability, "restore", shardID,
				fmt.Sprintf("image %s has unsupported format version %d",
					entry.Name(), rec.FormatVersion))
		}
		records[rec.ReplicaID] = rec
		if authoritative == nil || rec.CommitIndex > authoritative.CommitIndex {
			recCopy := cloneDurableRecord(rec)
			authoritative = &recCopy
		}
	}

	order := append([]string(nil), bootstrap...)
	if authoritative != nil {
		for _, entry := range authoritative.Log {
			if entry.ConfigChange != nil && entry.ConfigChange.Type == ConfigChangeAddReplica {
				if !slices.Contains(order, entry.ConfigChange.ReplicaID) {
					order = append(order, entry.ConfigChange.ReplicaID)
				}
			}
		}
	}
	return records, order, nil
}

// removeShardData deletes a shard directory; used by tests that need isolation.
func (d *diskDurability) removeShardData(shardID string) error {
	if err := os.RemoveAll(d.shardDir(shardID)); err != nil {
		return newError(KindDurability, "durability", shardID, "remove failed").wrap(err)
	}
	return nil
}
