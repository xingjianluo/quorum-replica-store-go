# quorum-replica-store-go

一个**本地、离线可复现**的分片复制键值存储（Go）。按键区间分片，每分片多副本，
写入需多数派确认；支持主副本失效自动切换、强一致/最终一致两种读、请求级幂等、
乐观版本控制（CAS）以及离线副本追赶。所有故障（崩溃、网络分区、恢复）都通过
内存故障注入在单进程内可控触发，**不依赖外部服务、真实凭据或付费组件**。

## 特性

- **区间分片**：左闭右开键区间，首尾相接；跨分片写以 `KeyOutOfRange` 明确拒绝。
- **多数派写入**：当前任期日志复制到多数派才提交并返回成功；已提交数据切换不丢不回滚。
- **两种读语义**：`ReadStrong`（主 + 多数派领导权确认）与 `ReadEventual`（任一存活副本本地读）。
- **自动主切换**：带 Pre-Vote 的 Raft 选举；不足多数派时只拒绝、绝不降级。
- **幂等**：请求标识复用，重复提交（含并发与"响应丢失后重试"）只生效一次。
- **版本与 CAS**：版本单调递增，删除留墓碑；版本冲突以 `CasConflict` 拒绝、不静默覆盖。
- **离线追赶**：落后副本恢复后由主补齐日志，最终与已提交状态收敛。
- **可解释失败**：区分 `KeyOutOfRange / NoQuorum / NoShardLeader / NotLeader /
  ReadModeConflict / CasConflict / KeyNotFound / ReplicasUnavailable / Unavailable` 等原因码，
  日志打印投票与多数派判定依据。

## 目录

- `internal/store/`：核心实现与测试（路由、节点选举/复制、持久日志、状态机、故障网络、客户端）。
- `cmd/demo/`：一键离线演示。
- `doc/design.md`：一致性语义、故障切换、幂等规则、已知边界的详细说明。

## 快速开始

需要 Go 1.25+（仅用标准库）。

```bash
# 运行离线演示：多数派写 -> 强/最终读 -> 幂等 -> 主崩溃切换 -> 少数派拒绝
go run ./cmd/demo

# 全部单元测试（含分区、崩溃恢复、主从切换、并发读写、重复提交等）
go test ./...

# 带竞态检测
go test ./... -race

# 查看某个故障场景的详细判定日志
go test ./internal/store/ -run TestFailoverLeaderCrashElectsNewLeaderAndPreservesData -v
```

## 本地验证方式（测试场景）

| 测试 | 覆盖的不变量 |
| --- | --- |
| `TestSmokeBasicWriteRead` | 多数派写入、强/最终读基础链路 |
| `TestWriteKeyNotInShardRejected` / `TestReadMissingKeyDistinctFromUnavailable` | 键越界与键不存在的原因可区分 |
| `TestFailoverLeaderCrashElectsNewLeaderAndPreservesData` | 主崩溃后自动选新主，已提交数据不丢 |
| `TestFailoverMinorityCannotWrite` | 不足多数派拒绝写、拒绝强一致读，不降级；恢复后继续服务 |
| `TestFailoverOldLeaderRestartCannotOverwrite` | 旧主重启只能追赶、不得覆盖新主已提交数据 |
| `TestIdempotentDuplicateSubmitAppliesOnce` | 重复/并发重复请求只生效一次 |
| `TestCASConflictDoesNotSilentlyOverwrite` | 版本冲突明确拒绝、失败不占版本；并发 CAS 一胜一冲突 |
| `TestNetworkPartitionHealsAndConverges` | 少数派旧主被围栏、多数派继续服务、愈合后收敛 |
| `TestLongOfflineReplicaCatchesUp` | 长期离线（落后 20 条）副本恢复后追平 |
| `TestCrashRecoveryPreservesCommittedData` | 全集群崩溃后从磁盘重载，数据与版本连续 |
| `TestConcurrentReadWriteConsistency` | 60 并发写恰好产生 60 个连续版本，强读版本不倒退 |
| `TestMultiShardRoutingAndIndependentFailover` | 多分片独立路由与独立容错 |
| `TestDeleteSemantics` | 删除墓碑、版本续增、过期 CAS 被拒 |

## 最小用法

```go
router, _ := store.NewRouter([]store.ShardConfig{{
    ID: 0, Range: store.Range{Start: "", End: ""},
    Replicas: []string{"n0", "n1", "n2"},
}})
c := store.NewCluster(router, store.NewNetwork(), "", logger) // dataDir 非空则落盘
for _, id := range []string{"n0", "n1", "n2"} { _ = c.AddReplica(0, id) }
c.StartAll(); defer c.ShutdownAll()

leader, _ := c.WaitForLeader(0, 3*time.Second)

cl := store.NewClient(c)
id := cl.NewRequestID("order")                 // 重试链复用同一 ID 保证幂等
wr, _ := cl.Put(ctx, 0, "k", []byte("v"), -1) // 无条件写
_, _  = cl.Put(ctx, 0, "k", []byte("v2"), wr.Version) // CAS：版本不符返回 CasConflict
strong, _ := cl.Get(ctx, 0, "k", store.ReadStrong)    // 强一致
stale,  _ := cl.Get(ctx, 0, "k", store.ReadEventual)  // 最终一致

// 故障注入：
c.Network().CutLink("n0", "n1") // 分区
c.CrashNode(leader)             // 崩溃（日志保留）
c.RestartNode(leader)           // 从磁盘恢复并追赶
_ = leader
_ = stale
```

详见 [doc/design.md](doc/design.md)。
