package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
	OpType   Operation // "get" "put" "append"
	Key      string
	Value    string
	UpConfig shardctrler.Config
	ShardId  int
	Shard    Shard
	SeqMap   map[int64]int
}

type Shard struct {
	StateMachine map[string]string
	ConfigNum    int // what version this Shard is in
}

type OpReply struct {
	ClientId int64
	SeqId    int
	Err      Err
}

const (
	UpConfigLoopInterval = 100 * time.Millisecond // poll configuration period
	GetTimeout           = 500 * time.Millisecond
	AppOrPutTimeout      = 500 * time.Millisecond
	UpConfigTimeout      = 500 * time.Millisecond
	AddShardsTimeout     = 500 * time.Millisecond
	RemoveShardsTimeout  = 500 * time.Millisecond
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	dead int32 // set by Kill()

	LastConfig shardctrler.Config
	Config     shardctrler.Config

	shardsPersist []Shard
	waitChMap     map[int]chan OpReply
	SeqMap        map[int64]int
	mck           *shardctrler.Clerk // sck is a client used to contact shard master

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardId := key2shard(args.Key)

	kv.mu.Lock()
	// 检查当前服务器是否属于该分片的组
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].StateMachine == nil {
		reply.Err = ShardNotReady
	}
	kv.mu.Unlock()

	if reply.Err == ErrWrongGroup || reply.Err == ShardNotReady {
		return
	}

	command := Op{
		OpType:   GetType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
	}

	err := kv.startCommand(command, GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}

	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].StateMachine == nil {
		reply.Err = ShardNotReady
	} else {
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardId].StateMachine[args.Key]
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		DPrintf("S%d config: %v", kv.me, kv.Config)
		DPrintf("S%d return ErrWrongGroup, kv.Config.Shards[%d]:%d != kv.gid:%d", kv.me, shardId, kv.Config.Shards[shardId], kv.gid)
	} else if kv.shardsPersist[shardId].StateMachine == nil {
		reply.Err = ShardNotReady
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotReady {
		return
	}
	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}
	reply.Err = kv.startCommand(command, AppOrPutTimeout)
}

func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	DPrintf("S%d <- G%d receive add shard req", kv.me, args.ClientId)
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedRequestId,
	}
	reply.Err = kv.startCommand(command, AddShardsTimeout)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {

		case msg := <-kv.applyCh:
			DPrintf("S%d Received Raft Code commit msg", kv.me)
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}

				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {

					shardId := key2shard(op.Key)

					if kv.Config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.shardsPersist[shardId].StateMachine == nil {
						reply.Err = ShardNotReady
					} else {

						if !kv.isDuplicate(op.ClientId, op.SeqId) {

							kv.SeqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								DPrintf("S%d handle Raft Code Put applyMsg, Put val to StateMachine", kv.me)
								kv.shardsPersist[shardId].StateMachine[op.Key] = op.Value
							case AppendType:
								DPrintf("S%d handle Raft Code Append applyMsg, Append val to StateMachine", kv.me)
								kv.shardsPersist[shardId].StateMachine[op.Key] += op.Value
							case GetType:
							default:
								DPrintf("S%d invalid command type: %v.", kv.me, op.OpType)
							}
						} else {
							DPrintf("S%d applyMsg is duplicate, op.ClientId:%d, op.SeqId:%d", kv.me, op.ClientId, op.SeqId)
						}
					}
				} else {
					// request from server of other group
					switch op.OpType {
					case UpConfigType:
						DPrintf("S%d handle Raft Code Put UpConfigType", kv.me)
						kv.upConfigHandler(op)
					case AddShardType:
						DPrintf("S%d handle Raft Code Put AddShardType", kv.me)
						if kv.Config.Num < op.SeqId {
							reply.Err = ConfigOutdated
							break
						}
						kv.addShardHandler(op)
					case RemoveShardType:
						DPrintf("S%d handle Raft Code Put RemoveShardType", kv.me)
						kv.removeShardHandler(op)
						// remove operation is from previous UpConfig
					default:
						DPrintf("S%d invalid command type: %v.", kv.me, op.OpType)
					}
				}

				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					DPrintf("S%d greater than maxraftstate, Snapshot", kv.me)
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				ch := kv.getWaitCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()

			}

			if msg.SnapshotValid {
				kv.mu.Lock()
				DPrintf("S%d handle Raft Code Snapshot, DecodeSnapShot", kv.me)
				kv.DecodeSnapShot(msg.Snapshot)
				kv.mu.Unlock()
				continue
			}
		}
	}
}

func (kv *ShardKV) ConfigDetectedLoop() {
	kv.mu.Lock()
	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		// only leader needs to deal with configuration tasks
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()
		if !kv.allSent() {
			SeqMap := make(map[int64]int)
			for k, v := range kv.SeqMap {
				SeqMap[k] = v
			}
			for shard, gid := range kv.LastConfig.Shards {
				// 对于当前组（kv.gid）的分片，如果新配置中的对应分片不属于当前组，
				// 并且当前分片的配置号小于新配置的配置号，说明该分片需要被发送给其他组。
				if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
					// 拷贝分片的数据
					sendDate := kv.cloneShard(kv.Config.Num, kv.shardsPersist[shard].StateMachine)
					args := SendShardArg{
						LastAppliedRequestId: SeqMap,
						ShardId:              shard,
						Shard:                sendDate,
						ClientId:             int64(gid),
						RequestId:            kv.Config.Num,
					}
					// shardId -> gid -> server names 找出分片对于的复制组的服务器集合
					serversList := kv.Config.Groups[kv.Config.Shards[shard]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.make_end(name)
					}
					DPrintf("S%d Send shards to %v", kv.me, servers)
					// 使用 goroutine 将切片发送给对应的复制组的所有服务器
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {
						index := 0
						start := time.Now()
						for {
							var reply AddShardReply
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)
							if ok && reply.Err == OK || time.Since(start) >= 2*time.Second {
								// 如果成功将切片发送给对应的服务器，GC 掉不属于自己的切片
								kv.mu.Lock()
								command := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.Config.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.startCommand(command, RemoveShardsTimeout)
								break
							}
							index = (index + 1) % len(servers)
							// 如果已经发送了一轮
							if index == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		curConfig = kv.Config
		sck := kv.mck
		kv.mu.Unlock()
		newConfig := sck.Query(curConfig.Num + 1)
		DPrintf("S%d found newConfig: %v", kv.me, newConfig)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		command := Op{
			OpType:   UpConfigType,
			ClientId: int64(kv.gid),
			SeqId:    newConfig.Num,
			UpConfig: newConfig,
		}
		kv.startCommand(command, UpConfigTimeout)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.shardsPersist = make([]Shard, shardctrler.NShards)

	kv.SeqMap = make(map[int64]int)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitChMap = make(map[int]chan OpReply)
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applyMsgHandlerLoop()
	go kv.ConfigDetectedLoop()
	return kv
}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	DPrintf("S%d send %v command to Raft Code", kv.me, command.OpType)
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()
	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()
	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err
	case <-timer.C:
		return ErrOverTime
	}
}

func (kv *ShardKV) isDuplicate(clientId int64, seqId int) bool {

	lastSeqId, exist := kv.SeqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shardsPersist) != nil {
		DPrintf("S%d fails to encode shardsPersist.", kv.me)
	}
	if e.Encode(kv.SeqMap) != nil {
		DPrintf("S%d fails to encode SeqMap.", kv.me)
	}
	if e.Encode(kv.maxraftstate) != nil {
		DPrintf("S%d fails to encode maxraftstate.", kv.me)
	}
	if e.Encode(kv.Config) != nil {
		DPrintf("S%d fails to encode Config.", kv.me)
	}
	if e.Encode(kv.LastConfig) != nil {
		DPrintf("S%d fails to encode LastConfig.", kv.me)
	}
	return w.Bytes()
}

func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardsPersist []Shard
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	if d.Decode(&shardsPersist) != nil || d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
		DPrintf("S%v Failed to decode snapshot", kv.me)
	} else {
		kv.shardsPersist = shardsPersist
		kv.SeqMap = SeqMap
		kv.maxraftstate = MaxRaftState
		kv.Config = Config
		kv.LastConfig = LastConfig
		DPrintf("S%d DecodeSnapShot kv.Config:%v", kv.me, kv.Config)
	}
}

func (kv *ShardKV) upConfigHandler(op Op) {
	curConfig := kv.Config
	upConfig := op.UpConfig
	if curConfig.Num >= upConfig.Num {
		return
	}
	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			kv.shardsPersist[shard].StateMachine = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = upConfig.Num
			if kv.shardsPersist[shard].StateMachine != nil {
				DPrintf("S%d kv.shardsPersist[%d].StateMachine != nil\n", kv.me, shard)
			}
		}
	}
	kv.LastConfig = curConfig
	kv.Config = upConfig
	DPrintf("S%d new kv.Config:%v", kv.me, kv.Config)
}

func (kv *ShardKV) addShardHandler(op Op) {
	if kv.shardsPersist[op.ShardId].StateMachine != nil || op.Shard.ConfigNum < kv.Config.Num {
		return
	}
	kv.shardsPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.StateMachine)
	for clientId, seqId := range op.SeqMap {
		if r, ok := kv.SeqMap[clientId]; !ok || r < seqId {
			kv.SeqMap[clientId] = seqId
		}
	}
}

func (kv *ShardKV) removeShardHandler(op Op) {
	if op.SeqId < kv.Config.Num {
		return
	}
	kv.shardsPersist[op.ShardId].StateMachine = nil
	kv.shardsPersist[op.ShardId].ConfigNum = op.SeqId
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// 对于当前组（kv.gid）的分片，如果新配置中的对应分片不属于当前组，
		// 并且当前分片的配置号小于新配置的配置号，说明该分片需要被发送给其他组。
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// gid != kv.gid                                     -> 在旧的配置中不属于当前复制组,也就是在现有的配置中可能需要被当前服务器接收
		// kv.Config.Shards[shard] == kv.gid                 -> 切片属于当前复制组
		// kv.shardsPersist[shard].ConfigNum < kv.Config.Num -> 当前分片的配置号小于新配置的配置号，说明该分片需要被接收
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) Shard {

	migrateShard := Shard{
		StateMachine: make(map[string]string),
		ConfigNum:    ConfigNum,
	}

	for k, v := range KvMap {
		migrateShard.StateMachine[k] = v
	}

	return migrateShard
}
