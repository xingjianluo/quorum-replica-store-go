# 本地分片复制键值存储

这是一个只依赖 Go 标准库、可完全离线运行的进程内分片复制键值存储。项目用确定性的本地故障注入模拟副本崩溃、网络分区和恢复，目标是让分片路由、多数派写入、主副本切换、副本追赶和客户端重试在故障前后都产生一致且可解释的结论。

> 当前实现是教学与可验证性优先的库，不是生产网络服务；没有真实 RPC，故障是测试可控的逻辑故障。与早期版本相比，本版本新增了三项“长期本地服务”能力：**无客户端请求时的后台自动主副本接管**、**整套实例停掉再拉起的磁盘持久化恢复**、**分片副本数在线扩容（如 3 → 5）**。这三项都通过新增的选项 API 开启，旧的纯内存、请求驱动用法保持兼容。

## 0. 本次新增能力速览

| 能力 | 入口 | 默认是否开启 |
| --- | --- | --- |
| 后台心跳/选举监督（无人请求也自动接管） | `NewStoreWithOptions(..., Options{TickInterval: ...})` | 使用 options 构造时开启；`TickInterval` 为 0 时取默认 50ms |
| 整套重启持久化 | `Options{DataDir: "..."}` | 仅当 `DataDir` 非空时落盘；为空时与旧版一致纯内存 |
| 在线扩副本（联合共识，不中断读写） | `Store.ExpandReplicas(shardID, []string{...})` | 显式调用 |
| 对外可写性/主副本诊断 | `Store.ClusterStatus()` / `ReplicaSet.ClusterStatus()` | 随时可用 |

旧 API（`NewStore`、`Write`/`Read` 的请求与响应结构、`Client` 重试、故障注入方法、错误 `Kind`）全部保留；用 `NewStore` 构造的存储行为与旧版一致（不启动后台协程、不落盘），现有用例无需改动。

## 1. 包结构

| 文件 | 职责 |
| --- | --- |
| `range.go` | 半开字典序区间 `[Start, End)` 与键归属判断 |
| `store.go` | 校验分片配置、按键路由到对应副本集、分片级扩容入口 |
| `replica.go` | 单个副本的任期、投票、日志、提交点、键值状态、请求标识结果，以及快照/重放 |
| `replicaset.go` | 多数派、选主、复制（含联合双多数派）、强/最终读、故障注入和追赶 |
| `supervisor.go` | 后台心跳、确定性错开的选举超时与无人请求时的自动接管 |
| `persistence.go` | 数据目录布局、JSON 快照、临时文件 + rename 的原子落盘 |
| `restore.go` | 重启后恢复成员清单与各副本已提交前缀并重放 |
| `reconfig.go` / `reconfig_expand.go` | 选民/学习者、联合共识选票策略与 3→5 在线扩容 |
| `status.go` | 对外主副本与可写性诊断 `ClusterStatus` |
| `options.go` | `Options`、`NewStoreWithOptions`、`Close` 生命周期 |
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

### 4.1 无人请求时的自动主副本接管

通过 `NewStoreWithOptions` 构造的存储会为每个分片启动一个后台监督协程（可通过 `Close` 停止）：

- 每个 tick（默认 50ms，可用 `Options.TickInterval` 调小）在任主副本确认自己仍能联系**存活选民多数派**，并向可达选民发送心跳、重置其选举计时。
- 从节点在错过心跳达到自己的超时时发起竞选。超时按副本在成员列表中的序号**确定性错开**：`timeout = 10 + 序号*4` 个 tick，因此不依赖随机数也能稳定地一次选出唯一主，接管时间有上界、可预期（默认最快约 500ms，测试常用 5ms tick 做到几十毫秒）。
- 在任主副本一旦发现自己联系不到选民多数派（崩溃或被分区），监督逻辑立即把它从对外主副本记录中摘除；凑不出多数派时**明确停在不可写状态**，不会继续宣称旧主在服务，也不会悄悄放行写入。
- 多数派恢复后，不需要任何客户端请求，监督协程会自动重新选出主并恢复可写。

判定依据都通过 logger 输出：谁超时、几号位、看到几票、为什么达不到多数派、为什么旧主被摘除。

### 4.2 对外可写性诊断

`Store.ClusterStatus()` / `ReplicaSet.ClusterStatus()` 返回一个不触发选主、只读的快照 `ShardStatus`：

- `LeaderID` / `Term`：当前**已确认**的主与任期；存活选民不足多数派且主已崩溃时不会再对外暴露那个旧主 ID。
- `Writable`：当前是否能安全写入。
- `WriteState`：`writable` / `no_leader`（等待或正在选举）/ `no_quorum`（存活或可达选民不足多数派）/ `reconfiguring`（联合扩容中，需要新旧双多数派）。
- `WriteBlockReason`：机器可读的人类说明，精确到“存活 1/2”“主只能联系 2/3”“联合旧侧 2/2、新侧 2/3”等。
- `ConfiguredVoters` / `Learners` / `Quorum`，以及联合期间的 `OldVoters`、`NewVoters`、`OldQuorum`、`NewQuorum`。

`ReplicaStatus` 另外增加了 `Voter` 和 `Learner` 标志。这个诊断只描述取样瞬间的真实状态，不会为了“看起来有主”而触发或跳过选举。

### 4.3 整套实例停掉再拉起（持久化恢复）

`Options{DataDir: dir}` 非空时，每个副本在以下边界把状态原子写入 `dir/shards/<shard>/...`：

- 任期或投票变化时（候选人自投、投票给他人、被更高任期降级）；
- 提交点推进时（主提交后、跟随者追赶或追加已提交条目后）。

写入一律是“临时文件 + fsync + rename”，写到一半崩溃不会留下半截状态文件。落盘内容**只包含已提交日志前缀**、任期、投票和成员清单：

- 重启后各副本从已提交前缀重放，逐条重建键值和请求标识结果，因此**已确认成功的写入原样还在**，每个键的版本从重放日志继续递增，不会回退、跳回旧值或出现重复版本。
- 已确认成功的 `RequestID` 随已提交日志持久化；重启后用同一请求再提一次仍然命中旧结果（`Duplicate=true`、返回原版本），不会再生效第二遍。同一 ID 换参数仍然是 `duplicate_conflict`。
- **只在少数副本上短暂出现、从未达到多数派确认的写入不会落进任何已提交前缀**，因此重启后不可能被当成已提交数据放出；它的请求标识也不会被记住。
- 成员清单（含扩容后的五选民配置）只在配置条目被相应多数派确认后才落盘，所以磁盘上的成员关系永远不会领先于已提交日志。
- 重启完成后后台监督自动重新选主，无需手工选举即可回到可读可写状态。

磁盘布局：

```text
<DataDir>/shards/<safe-shard-id>/manifest.json
<DataDir>/shards/<safe-shard-id>/replicas/<safe-replica-id>.json
```

这是教学级持久化（每个状态一个 JSON 快照，无 WAL/SST），崩溃语义建模的是“状态原子落盘”的本地服务，而不是物理介质故障恢复。

### 4.4 在线扩容（联合共识）

`Store.ExpandReplicas(shardID, newReplicaIDs)`（或分片上的同名方法）在不中断现有读写的前提下增加副本数，例如 3 → 5：

1. **学习者阶段**：新副本先以非投票学习者加入，被主副本把已提交前缀完整追平；这期间它们不投票、不计入任何多数派。
2. **联合配置提交**：写一条 admin 配置条目，必须同时拿到**旧配置多数派**和**新配置多数派**的确认。因为旧选民集合是新集合的子集，两个多数派必然相交——扩容期间不可能出现“旧侧一拨、新侧一拨各自以为多数派、各自提交冲突结果”。
3. **新配置生效**：联合条目一确认，新成员立即成为选民，随后提交新配置条目；从这一瞬间起后续写入按**新的多数派门槛**（5 副本需 3 票）执行，成员清单同时落盘。

整个过程在分片锁内与普通读写串行，任何并发写要么落在旧门槛、要么落在更严格的联合双门槛、要么落在新门槛上，没有中间灰色窗口。扩容失败（例如某一侧凑不齐多数派）不会激活任何成员变更，旧配置继续服务，学习者保持“只追数据、不投票”，恢复连通后可以原样重试。若主在联合条目与新配置条目之间崩溃，新主上任后会自动把待决的新配置条目补提交完成；补不齐时安全地停在更严格的联合态。

约束：新副本 ID 不能为空、不能与现有成员重复；扩容后总数至少为 3 且必须严格增加。当前只实现**纯增加投票成员**这一种单分片变更，不做缩容、不做成员替换、一次只进行一个变更。

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

长期运行用法（开启自动接管与持久化）：

```go
store, err := quorum.NewStoreWithOptions(specs, quorum.Options{
    DataDir:      "./data/quorum", // 空串 = 纯内存（旧行为）
    TickInterval: 20 * time.Millisecond, // 0 = 默认 50ms
    Logger:       logger,
})
defer store.Close()

// 不中断读写地把 users 分片从 3 副本扩到 5 副本
if err := store.ExpandReplicas("users", []string{"u4", "u5"}); err != nil {
    // quorum_unavailable 表示当前新旧多数派凑不齐，可在恢复后原样重试
}

// 随时查看主副本是谁、能不能写、为什么不能写
for _, st := range store.ClusterStatus() {
    log.Printf("shard=%s leader=%s writable=%t state=%s reason=%s",
        st.ShardID, st.LeaderID, st.Writable, st.WriteState, st.WriteBlockReason)
}
```

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

# 无人请求时后台自动接管、缺多数派明确停写、分区自动换主
go test -v -run 'TestAutoFailover|TestClusterStatus' ./...

# 整套重启：已确认写入/版本/请求标识保留，未确认写入不复活
go test -v -run TestRestart ./...

# 在线扩 3→5、并发写交错、扩容后成员关系跨重启保留、联合防双多数派
go test -v -run 'TestExpand|TestExpansion|TestJoint' ./...
```

测试使用 `t.Logf` 打印：

- 候选人、投票人、任期、日志新旧比较和票数。
- 可达副本数、多数派大小和拒绝原因。
- 哪条日志在哪个索引/任期提交。
- 副本恢复前后的提交点。
- 最终读是否可能旧。

测试失败时可直接根据日志判断破坏的是多数派、任期、日志匹配、提交点收敛、版本冲突还是请求标识幂等不变量。

## 9. 已知边界

- 持久化是每个副本一份 JSON 状态快照（临时文件 + rename 原子替换），没有 WAL、SST、块校验或 fsync 目录项；它建模“本地进程崩溃后已确认状态不丢”，不覆盖磁盘损坏、误删数据目录等介质级故障。
- 没有真实网络、gRPC、HTTP、服务发现、认证或跨进程客户端；后台监督通过进程内 tick 推进，时钟来自 Go 的 `time.Ticker`，不是逻辑时钟。
- 自动接管的耗时是“确定性错开超时”而非随机退避：默认 50ms tick、10+4n tick 超时，最快约 500ms、相邻候选相差 200ms。极端调度抖动下首个候选可能失败，随后按序重试，上界仍然可预期。
- 操作在副本集内串行执行（一把分片锁），重点是可解释和易测，不是高吞吐生产实现；后台 tick 与读写并发安全，`go test -race` 覆盖。
- 配置变更只支持单分片、单个进行中的**纯增加投票成员**扩容；不支持缩容、选民替换、一次跨多分片变更，也不支持变更分片键区间。
- 扩容期间主在“联合条目已确认、新配置条目未确认”之间崩溃时，新主会尝试自动补完；若新多数派暂时不可达会停在安全的联合态（读写受双多数派约束），需等新侧恢复后由新主或重试完成。
- 不提供删除、TTL、跨分片事务或二次索引。
- 请求标识去重结果随已提交日志长期保存（内存 + 已提交快照），没有垃圾回收。
- `NewStore`（旧构造函数）保持纯内存、请求驱动、不起后台协程的旧行为；后台自动接管与落盘只通过 `NewStoreWithOptions` 开启。用 options 构造的存储应在进程退出前调用 `Close` 停止监督协程。
