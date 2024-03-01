package shardkv

import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"
import "6.5840/shardctrler"
import "time"
import "sync/atomic"
import "bytes"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
	OpType   Operation // "get" "put" "append"
	Key      string
	Value    string
}

type Shard struct {
	stateMachine map[string]string
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

	Config shardctrler.Config

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
	} else if kv.shardsPersist[shardId].stateMachine == nil {
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
	} else if kv.shardsPersist[shardId].stateMachine == nil {
		reply.Err = ShardNotReady
	} else {
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardId].stateMachine[args.Key]
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].stateMachine == nil {
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
					} else if kv.shardsPersist[shardId].stateMachine == nil {
						reply.Err = ShardNotReady
					} else {

						if !kv.isDuplicate(op.ClientId, op.SeqId) {

							kv.SeqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								DPrintf("S%d handle Raft Code Put applyMsg, Put val to stateMachine", kv.me)
								kv.shardsPersist[shardId].stateMachine[op.Key] = op.Value
							case AppendType:
								DPrintf("S%d handle Raft Code Append applyMsg, Append val to stateMachine", kv.me)
								kv.shardsPersist[shardId].stateMachine[op.Key] += op.Value
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
					case AddShardType:
						DPrintf("S%d handle Raft Code Put AddShardType", kv.me)
					case RemoveShardType:
						DPrintf("S%d handle Raft Code Put RemoveShardType", kv.me)
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

	return kv
}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
	}
}
