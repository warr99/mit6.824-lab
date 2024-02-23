package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type OpType string

const timeOut = 100 * time.Millisecond

const (
	GetOp    OpType = "Get"
	PutOp    OpType = "Put"
	AppendOp OpType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   OpType
	Key      string
	Value    string
	SeqId    int64
	ClientId int64
	Err      Err
}

type IndexAndTerm struct {
	Index int
	Term  int
}

type CommandResponse struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	lastApplied int

	// Your definitions here.
	seqMap       map[int64]int64                       //为了确保seq只执行一次	clientId / seqId
	waitChMap    map[IndexAndTerm]chan CommandResponse //传递由下层Raft服务的appCh传过来的command	index / chan(Op)
	stateMachine KVStateMachine                        // KV stateMachine
}

func (kv *KVServer) getWaitCh(IndexAndTerm IndexAndTerm) chan CommandResponse {
	ch, exist := kv.waitChMap[IndexAndTerm]
	if !exist {
		kv.waitChMap[IndexAndTerm] = make(chan CommandResponse, 1)
		ch = kv.waitChMap[IndexAndTerm]
	}
	return ch
}

func (kv *KVServer) isDuplicate(clientId int64, seqId int64) bool {
	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	cmd := Op{OpType: GetOp,
		Key:      args.Key,
		SeqId:    args.SeqId,
		ClientId: args.ClientId}
	reply.Err, reply.Value = kv.clientRequestProcessHandler(cmd)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := Op{
		OpType:   OpType(args.Op),
		Key:      args.Key,
		Value:    args.Value,
		SeqId:    args.SeqId,
		ClientId: args.ClientId}
	reply.Err, _ = kv.clientRequestProcessHandler(cmd)
}

func (kv *KVServer) clientRequestProcessHandler(cmd Op) (Err, string) {
	kv.mu.Lock()
	if cmd.OpType != GetOp && kv.isDuplicate(cmd.ClientId, cmd.SeqId) {
		DPrintf("S%d cmd is duplicate, ClientId:%d, SeqId:%d, lastSeqId:%d",
			kv.me,
			cmd.ClientId,
			cmd.SeqId,
			kv.seqMap[cmd.ClientId])
		kv.mu.Unlock()
		return OK, ""
	}

	kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		DPrintf("S%d is not Leader", kv.me)
		return ErrWrongLeader, ""
	}

	it := IndexAndTerm{index, term}
	kv.mu.Lock()
	ch := kv.getWaitCh(it)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, it)
		kv.mu.Unlock()
	}()

	select {
	case response := <-ch:
		return response.Err, response.Value
	case <-time.After(timeOut):
		DPrintf("S%d Raft Code handle cmd timeout", kv.me)
		return TimeOut, ""
	}
}

func (kv *KVServer) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine.GetMap())
	e.Encode(kv.seqMap)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	return data
}

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int64
	var lastApplied int

	if d.Decode(&kvPersist) != nil || d.Decode(&seqMap) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf("S%d error to read the snapshot data", kv.me)
	} else {
		kv.stateMachine.SetMap(kvPersist)
		kv.seqMap = seqMap
		kv.lastApplied = lastApplied
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyOp() {
	for !kv.killed() {
		for m := range kv.applyCh {
			if m.CommandValid {
				kv.mu.Lock()
				if m.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				op := m.Command.(Op)
				kv.lastApplied = m.CommandIndex

				var response CommandResponse
				if op.OpType != GetOp && kv.isDuplicate(op.ClientId, op.SeqId) {
					DPrintf("S%d op has been processed,SeqId:%d", kv.me, op.SeqId)
					response = CommandResponse{OK, ""}
				} else {
					kv.seqMap[op.ClientId] = op.SeqId
					switch op.OpType {
					case PutOp:
						DPrintf("S%d handle Raft Code Put applyMsg, Put val to stateMachine", kv.me)
						response.Err = kv.stateMachine.Put(op.Key, op.Value)
					case AppendOp:
						DPrintf("S%d handle Raft Code Append applyMsg, Append val to stateMachine", kv.me)
						response.Err = kv.stateMachine.Append(op.Key, op.Value)
					case GetOp:
						DPrintf("S%d handle Raft Code Get applyMsg, Get val to stateMachine", kv.me)
						response.Value, response.Err = kv.stateMachine.Get(op.Key)
					}
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader {
					DPrintf("S%d ChanRespone Command:%v Response:%v commitIndex:%v currentTerm: %v", kv.me, op, response, m.CommandIndex, currentTerm)
					ch := kv.getWaitCh(IndexAndTerm{m.CommandIndex, currentTerm})
					ch <- response
				}
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					DPrintf("S%d log grows maxraftstate,snapshot", kv.me)
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(m.CommandIndex, snapshot)
				}
				kv.mu.Unlock()
			} else if m.SnapshotValid {
				kv.mu.Lock()
				DPrintf("handle Raft Code SnapShot msg, load snapshot to stateMachine")
				kv.DecodeSnapShot(m.Snapshot)
				kv.lastApplied = m.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.lastApplied = -1
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.seqMap = make(map[int64]int64)
	kv.stateMachine = NewMemoryKV()
	kv.waitChMap = make(map[IndexAndTerm]chan CommandResponse)

	go kv.applyOp()
	return kv
}
