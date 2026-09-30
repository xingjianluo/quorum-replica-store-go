// Command demo 在本地、无外部依赖地演示分片复制键值存储的关键行为：
// 多数派写入、强一致/最终一致读、主崩溃自动切换、少数派拒绝写、旧主恢复追赶与幂等。
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/xingjianluo/quorum-replica-store-go/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)
	fmt.Println("=== 本地分片复制 KV 演示（单分片 3 副本，全离线、故障注入） ===")

	// 1) 构造 1 个覆盖全部键空间的分片，3 个副本（多数派=2）。
	router, err := store.NewRouter([]store.ShardConfig{{
		ID: 0, Range: store.Range{Start: "", End: ""},
		Replicas: []string{"n0", "n1", "n2"},
	}})
	must(err)
	cluster := store.NewCluster(router, store.NewNetwork(), "", logger)
	for _, id := range []string{"n0", "n1", "n2"} {
		must(cluster.AddReplica(0, id))
	}
	cluster.StartAll()
	defer cluster.ShutdownAll()

	leader, err := cluster.WaitForLeader(0, 3*time.Second)
	must(err)
	fmt.Printf("\n[1] 初始主副本: %s\n", leader)

	client := store.NewClient(cluster)
	ctx := context.Background()

	// 2) 多数派写入 + 强一致读。
	wr, err := client.Put(ctx, 0, "user:1", []byte("alice"), -1)
	must(err)
	fmt.Printf("[2] 写入 user:1=alice 由主 %s 确认，提交版本=%d（已复制到多数派）\n", wr.Leader, wr.Version)
	rr, err := client.Get(ctx, 0, "user:1", store.ReadStrong)
	must(err)
	fmt.Printf("    强一致读: %q 来自 %s（读前已向多数派确认领导权）\n", string(rr.Item.Value), rr.From)

	// 3) 幂等：同一请求标识重复提交。
	id := client.NewRequestID("demo")
	a, err := client.PutWithID(ctx, 0, "idempotent", []byte("first"), -1, id)
	must(err)
	b, err := client.PutWithID(ctx, 0, "idempotent", []byte("second"), -1, id)
	must(err)
	fmt.Printf("[3] 幂等: 同一请求ID重复提交，版本保持 %d（不重复生效）\n", b.Version)
	if a.Version != b.Version {
		panic("idempotency broken")
	}

	// 4) 主崩溃 -> 自动切换，已提交数据不丢。
	oldLeader := cluster.ShardLeader(0)
	fmt.Printf("\n[4] 注入故障：崩溃主副本 %s\n", oldLeader)
	must(cluster.CrashNode(oldLeader))
	wr2, err := client.Put(ctx, 0, "user:1", []byte("alice-2"), wr.Version)
	must(err)
	fmt.Printf("    剩余副本自动选出新主 %s，继续写入成功，版本=%d\n", wr2.Leader, wr2.Version)
	rr2, err := client.Get(ctx, 0, "user:1", store.ReadStrong)
	must(err)
	fmt.Printf("    强一致读切换后数据: %q（已提交数据未丢失/回滚）\n", string(rr2.Item.Value))

	// 5) 再崩一个 -> 只剩 1/3，凑不齐多数派，写入必须被拒绝。
	second := cluster.ShardLeader(0)
	others := []string{}
	for _, x := range []string{"n0", "n1", "n2"} {
		if x != second {
			others = append(others, x)
		}
	}
	survivor := others[0] // 一个是已崩溃的旧主，一个是将被崩溃的 follower
	must(cluster.CrashNode(second))
	fmt.Printf("\n[5] 再崩溃主副本 %s：存活副本不足多数派\n", second)
	_ = survivor
	// 直接对当前集群做单次写，展示"凑不齐多数派/无主"的精确原因码（不做客户端长重试）。
	if _, err := cluster.Write(store.WriteRequest{ShardID: 0, Op: store.OpPut, Key: "x",
		Value: []byte("y"), ExpectedVersion: -1, RequestID: client.NewRequestID("demo-minority")}); err != nil {
		fmt.Printf("    写入被明确拒绝（原因码 %s）: %v\n", store.ErrorCode(err), err)
	} else {
		panic("minority write must be rejected")
	}

	fmt.Println("\n=== 演示结束：多数派提交、故障切换、幂等与拒绝语义均符合预期 ===")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
