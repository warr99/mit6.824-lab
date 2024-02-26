package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sort"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	seqMap    map[int64]int
	waitChMap map[int]chan Op
}

type Op struct {
	// Your data here.
	SeqId       int
	ClientId    int64
	Index       int
	OpType      OpType
	JoinServers map[int][]string
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

func (sc *ShardCtrler) isDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

// sortGroupShard 对组进行排序，按照负载从小到大排序
//
//	@param GroupMap
//	@return []int 根据负载从大到小排序的gid切片
func sortGroupShard(GroupMap map[int]int) []int {
	// 创建一个包含所有组ID的切片
	gids := make([]int, 0, len(GroupMap))
	for gid := range GroupMap {
		gids = append(gids, gid)
	}

	// 使用排序函数对组ID进行排序，根据负载从大到小排序
	sort.Slice(gids, func(i, j int) bool {
		return GroupMap[gids[i]] > GroupMap[gids[j]]
	})

	return gids
}

// moreAllocations 判断给定组的负载是否需要更多的分配
func moreAllocations(length, remainder, index int) bool {
	// 如果余数大于0，则前remainder个组的分配数量为平均值+1
	if remainder > 0 {
		return index < remainder
	}
	return false
}

// loadBalance
//
//	@receiver sc
//	@param GroupMap  每个 Replication Group 的当前负载情况,键为 Replication Group ID (GID),值为对应的负载数量
//	@param lastShards 上一个配置中各 Shard 所属的 Replication Group ID
//	@return [NShards]int 经过负载均衡操作后，新的 Shard 分配情况
func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GroupMap)
	ave := NShards / length
	remainder := NShards % length
	sortGids := sortGroupShard(GroupMap)

	// 先把负载多的部分free
	for i := 0; i < length; i++ {
		target := ave

		// 判断这个数是否需要更多分配，因为不可能完全均分，在前列的应该为ave+1
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		// 超出负载
		if GroupMap[sortGids[i]] > target {
			overLoadGid := sortGids[i]
			changeNum := GroupMap[overLoadGid] - target
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == overLoadGid {
					lastShards[shard] = 0
					changeNum--
				}
			}
			GroupMap[overLoadGid] = target
		}
	}

	// 为负载少的group分配多出来的group
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		if GroupMap[sortGids[i]] < target {
			freeGid := sortGids[i]
			changeNum := target - GroupMap[freeGid]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shard] = freeGid
					changeNum--
				}
			}
			GroupMap[freeGid] = target
		}

	}
	return lastShards
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	DPrintf("S%d <- C%d Received Get Join", sc.me, args.ClientId)
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = WrongLeader
		DPrintf("S%d not leader, return WrongLeader", sc.me)
		return
	}
	op := Op{OpType: JoinOp, SeqId: args.SeqId, ClientId: args.ClientId, JoinServers: args.Servers}
	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	select {
	case replyOp := <-ch:
		DPrintf("S%d receive a %v Ask, replyOp:%v", sc.me, replyOp, replyOp.OpType)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = WrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = WrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)
	go sc.applyMsgHandlerLoop()
	return sc
}

func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				index := msg.CommandIndex
				op := msg.Command.(Op)
				if !sc.isDuplicate(op.ClientId, op.SeqId) {
					switch op.OpType {
					case JoinOp:
						DPrintf("S%d Receive Join,op: %v", sc.me, op)
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
					}
				}
				sc.getWaitCh(index) <- op
			}
		}
	}
}

// Shardctrler 应该通过创建新的包括了新副本组的新配置来进行响应。
// 新的配置应该在所有组中尽可能平均地分配分片。
// 并尽可能少地移动分片来实现这一目标。
// 如果有 gid 不是当前配置的一部分，那么 Shardctrler 应该允许重用它(例如，允许 gid 加入，然后离开，然后再加入)。
func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	// 获取系统当前的最新配置
	lastConfig := sc.configs[len(sc.configs)-1]
	// 复制原有的 Replication Group
	newGroups := make(map[int][]string)
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	// 加入新的 Replication Group
	for gid, serverLists := range servers {
		newGroups[gid] = serverLists
	}
	// 统计当前每一个 Replication Group 对应了多少个 Shard (用于负载均衡)
	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		GroupMap[gid]++
	}

	DPrintf("S%d Before loadBalance - GroupMap: %v, Shards: %v\n", sc.me, GroupMap, lastConfig.Shards)
	// 负载均衡
	newShards := sc.loadBalance(GroupMap, lastConfig.Shards)
	DPrintf("S%d After loadBalance - GroupMap: %v, Shards: %v\n", sc.me, GroupMap, lastConfig.Shards)

	// 返回新的配置
	return &Config{
		Num:    len(sc.configs),
		Shards: newShards,
		Groups: newGroups,
	}
}
