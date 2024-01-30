// 关于时间
package raft

import (
	"math/rand"
	"time"
)

const (

	// 随机生成投票过期时间范围: MoreVoteTime+MinVoteTime ~ MinVoteTime
	MoreVoteTime = 100
	MinVoteTime  = 75

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 35
	AppliedSleep   = 15
)

// 通过不同的随机种子生成不同的过期时间
func generateOverTime(server int64) int {
	randSource := rand.NewSource(time.Now().Unix() + server)
	r := rand.New(randSource)
	// Tip: r.Intn(MoreVoteTime)从 r 这个随机数生成器生成的随机整数中选择一个不超过 MoreVoteTime 的非负整数。
	return r.Intn(MoreVoteTime) + MinVoteTime
}