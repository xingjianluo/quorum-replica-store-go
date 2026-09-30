# 本地分片复制键值存储

这是一个只依赖 Go 标准库、可完全离线运行的进程内分片复制键值存储。项目用确定性的本地故障注入模拟副本崩溃、网络分区和恢复，目标是让分片路由、多数派写入、主副本切换、副本追赶和客户端重试在故障前后都产生一致且可解释的结论。

> 当前实现是教学与可验证性优先的库，不是生产网络服务；没有磁盘 WAL、后台心跳或真实 RPC。崩溃是测试可控的逻辑故障，副本的内存日志和键值状态会保留，用于模拟“进程停止但持久日志仍在”的副本。

## 1. 包结构

| 文件 | 职责 |
| --- | --- |
| `range.go` | 半开字典序区间 `[Start, End)` 与键归属判断 |
| `store.go` | 校验分片配置、按键路由到对应副本集 |
| `replica.go` | 单个副本的任期、投票、日志、提交点、键值状态和请求标识结果 |
| `replicaset.go` | 多数派、选主、复制、强/最终读、故障注入和追赶 |
| `client.go` | 带请求标识的安全重试客户端 |
| `errors.go` | 可机器区分的错误分类 |
| `types.go` | 读写请求、响应、日志条目和诊断接口 |

## 2. 分片与副本配置

分片区间采用字符串字典序的半开区间：

- `Start == ""`：从最小键开始。
- `End == ""`：到最大键结束。
- `key` 属于某分片当且仅当 `Start <= key < End`。
- 所有分片必须首尾相接，完整覆盖键空间，不能有缺口或重叠。

每个分片至少配置 2 个副本。多数派为：

```text
quorum = floor(replica_count / 2) + 1
```

例如 3 副本需要 2 个确认，5 副本需要 3 个确认。2 副本也需要 2 个确认，因此任意一个副本失效都不能继续写入。

示例：

```go
store, err := quorum.NewStore([]quorum.ShardSpec{
    {
        ID:    "low",
        Range: quorum.ShardRange{Start: "", End: "m"},
        ReplicaIDs: []string{"l1", "l2", "l3"},
    },
    {
        ID:    "high",
        Range: quorum.ShardRange{Start: "m", End: ""},
        ReplicaIDs: []string{"h1", "h2", "h3"},
    },
})
```

`"l"` 属于 `low`，`"m"` 属于 `high`。

## 3. 一致性语义

### 3.1 写入

写入必须携带非空 `RequestID`。主副本把写入追加到自己的日志后，复制给当前可达副本；只有收到包括自己在内的多数派确认，才推进提交点并物化键值。

返回结果包含：

- `Version`：该键的新版本号，从 1 开始递增。
- `CommitIndex` / `CommitTerm`：提交位置和主副本任期。
- `LeaderID`：服务本次写入的主副本。
- `Duplicate`：是否命中了已提交的相同请求。

可选 `CompareVersion` 实现 compare-and-swap：

- `CompareVersion == nil`：普通覆盖写，但仍产生新版本。
- `CompareVersion != nil`：只有当前版本等于该值才提交。
- 版本不匹配返回 `version_conflict`，不会静默覆盖。

### 3.2 强一致读

`ReadModeStrong` 必须通过当前主副本服务。服务前会确认：

1. 存活副本数至少为多数派。
2. 当前主副本仍能联系多数派。
3. 可达副本中不存在更高任期。
4. 如没有合法主副本，会先尝试在可达多数派中选主。

强读返回的 `Stale` 恒为 `false`。若不能证明主副本有效，则拒绝，不会从少数派猜测结果。

`MinIndex` 可要求读取的提交点至少达到某个日志位置；达不到时返回 `strong_read_unsafe`。

### 3.3 最终一致读

`ReadModeEventual` 可通过 `ReplicaID` 指定任意存活副本读取。它允许读到旧数据，因此：

- 响应中的 `Stale=true` 表示该副本落后于其他存活副本，或它无法联系多数派、无法证明自己不旧。
- 少数派副本仍可执行最终读，但一定会被标记为可能旧。
- 最终读不能指定 `MinIndex`，因为这种读模式不承诺读法定人数。

## 4. 主副本切换

选主规则采用 Raft 风格的任期、投票和日志新旧判断：

1. 候选人只能统计自己可达的副本。
2. 候选人和投票者都必须存活且未被注入网络分区。
3. 候选人日志至少与投票者一样新：先比较最后一条日志任期，再比较日志长度。
4. 得到多数派投票才能成为主。
5. 新主上任后先复制一条当前任期的 admin no-op 并提交，再处理用户写入。
6. 达不到多数派时不会产生新主，写入和强读必须拒绝。

旧主恢复或分区愈合后：

- `CatchUp` 只把当前主的已提交前缀复制给落后副本。
- 旧主未提交的旧尾项会被截断，不会覆盖新主任期内的已提交数据。
- 追赶不会把当前新主替换成旧主。
- 若某副本本地任期高于当前主，系统不会强行把它降级，而是要求重新选主。

普通写入、强读或显式 `ElectLeader` 都会在需要时触发选主。最终读不触发选主。

## 5. 幂等与离线副本追赶

### 请求标识

`RequestID` 的作用域是单个分片。同一个 `RequestID`：

- 相同 `Key`、`Value`、`CompareVersion` 的重复提交返回第一次提交的版本、索引和任期，`Duplicate=true`，不会重复产生版本。
- 相同 ID 但键、值或比较版本不同，返回 `duplicate_conflict`。
- 重试中的 CAS 版本不匹配返回 `version_conflict`，不会覆盖当前值。

客户端只重试以下可能恢复的错误：

- `quorum_unavailable`
- `no_leader`
- `not_leader`
- 强读的 `strong_read_unsafe`

以下确定性错误不重试：

- 键区间错误
- 非法读模式
- 缺少请求标识
- CAS 冲突
- 同请求标识但参数不同
- 参数非法

### 离线追赶

副本长时间离线期间，多数派可以继续提交多个版本。副本恢复后调用 `CatchUp`：

1. 确认当前仍有存活多数派。
2. 必要时重新选主。
3. 将当前主的完整已提交日志前缀复制给离线副本。
4. 离线副本一次性应用缺失的已提交条目，并重建键值和请求标识结果。
5. 未提交尾项不会被发布，也不会回滚已提交状态。

## 6. 本地故障注入 API

这些方法只在本进程内改变可达性和存活状态，不依赖网络或外部服务：

```go
shard, _ := store.Shard("users")

shard.CrashReplica("r1")
shard.RecoverReplica("r1")

shard.AddPartition("r1", "r2")    // 注入 r1/r2 之间的无向断边
shard.RemovePartition("r1", "r2")
shard.HealPartitions()

shard.CatchUp()                   // 恢复已提交前缀
shard.ElectLeader("")             // 显式触发选主
shard.Status()                    // 查看任期、存活、提交点和日志长度
```

分区模型是“无向断边”。复杂拓扑需要显式添加所有相关断边；注入不会自动推导多跳不可达。

## 7. 错误分类

错误可通过 `errors.As` 转成 `*quorum.Error`，读取稳定的 `Kind`：

| Kind | 含义 |
| --- | --- |
| `invalid_config` | 分片区间、副本数量或 ID 配置非法 |
| `key_out_of_range` | 键不属于被寻址分片，或没有分片拥有该键 |
| `no_shard` | 指定的分片不存在 |
| `no_replica` | 指定的副本不存在 |
| `quorum_unavailable` | 存活或可达副本不足多数派，不能写入或强确认 |
| `no_leader` | 当前没有主副本，且操作需要主副本 |
| `not_leader` | 假定主副本已被更高任期取代 |
| `replica_unavailable` | 指定副本已崩溃，或故障状态转换非法 |
| `version_conflict` | CAS 比较版本与当前版本不同，或日志追赶发现已提交冲突 |
| `duplicate_conflict` | 同一请求标识已用不同参数提交 |
| `missing_request_id` | 写入缺少用于安全重试的请求标识 |
| `invalid_read_mode` | 读模式非法，或读模式与 `ReplicaID` / `MinIndex` 组合冲突 |
| `strong_read_unsafe` | 无法证明强读或最低提交点要求安全 |
| `invalid_argument` | 空键等通用参数错误 |

## 8. 本地验证

所有命令都不需要外部服务、凭据或网络：

```bash
go test ./...
go test -v ./...
go test -race ./...
go vet ./...
```

重点场景：

```bash
# 初始多数派写入、强读、最终读、重复请求
go test -v -run TestSmokeWriteStrongReadAndDuplicate ./...

# 主副本崩溃、剩余多数派选新主、旧主恢复追赶
go test -v -run TestLeaderCrashElectionAndOldLeaderCatchUp ./...

# 网络分区、多数派继续服务、愈合后收敛
go test -v -run TestNetworkPartitionElectsMajorityLeaderAndConvergesAfterHeal ./...

# 多数派边界：3/5 可服务，2/5 拒绝，恢复后继续
go test -v -run TestFiveReplicaQuorumBoundary ./...

# 客户端同一请求标识重试，不重复生效
go test -v -run TestClientRetriesSameRequestIDUntilMajorityRestored ./...

# 并发读写与精确一次提交
go test -race -v -run TestConcurrent ./...

# 可区分拒绝原因
go test -v -run TestDistinguishableRejectionReasons ./...
```

测试使用 `t.Logf` 打印：

- 候选人、投票人、任期、日志新旧比较和票数。
- 可达副本数、多数派大小和拒绝原因。
- 哪条日志在哪个索引/任期提交。
- 副本恢复前后的提交点。
- 最终读是否可能旧。

测试失败时可直接根据日志判断破坏的是多数派、任期、日志匹配、提交点收敛、版本冲突还是请求标识幂等不变量。

## 9. 已知边界

- 数据保存在内存中，没有磁盘 WAL、快照或真实持久化；崩溃测试保留内存以模拟有持久日志的节点。
- 没有真实网络、gRPC、HTTP、服务发现、认证或跨进程客户端。
- 没有后台心跳和随机选举超时；选主由写入、强读、显式调用或追赶确定性触发。
- 操作在副本集内串行执行，重点是可解释和易测，不是高吞吐生产实现。
- 不提供删除、TTL、跨分片事务或二次索引。
- 请求标识去重结果会随已提交日志长期保存在内存中，没有垃圾回收。
- 单次配置变更、副本扩缩容和分片迁移不在当前范围内。
