package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

type OpType string

const timeOut time.Duration = 100 //超时时间(s)

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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap       map[int64]int64 //为了确保seq只执行一次	clientId / seqId
	waitChMap    map[int]chan Op //传递由下层Raft服务的appCh传过来的command	index / chan(Op)
	stateMachine KVStateMachine  // KV stateMachine
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *KVServer) isDuplicate(clientId int64, seqId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("S%d <- C%d Received Get Req", kv.me, args.ClientId)
	if kv.killed() {
		DPrintf("S%d -> C%d send Get reply, ErrWrongLeader", kv.me, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		DPrintf("S%d -> C%d send Get reply, ErrWrongLeader", kv.me, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: GetOp, Key: args.Key, SeqId: args.SeqId, ClientId: args.ClientId}
	DPrintf("S%d send Get to Raft Code", kv.me)
	index, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(timeOut * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		DPrintf("S%d Received Raft Code resp", kv.me)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			DPrintf("S%d Received Raft Code Get resp, ErrWrongLeader", kv.me)
			reply.Err = ErrWrongLeader
		} else {
			kv.mu.Lock()
			DPrintf("S%d Received Raft Code resp, OK, get val from stateMachine", kv.me)
			reply.Value, reply.Err = kv.stateMachine.Get(op.Key)
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		DPrintf("S%d Timeout, ErrWrongLeader", kv.me)
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:   OpType(args.Op),
		Key:      args.Key,
		Value:    args.Value,
		SeqId:    args.SeqId,
		ClientId: args.ClientId}
	DPrintf("S%d send Put/Append to Raft Code", kv.me)
	index, _, _ := kv.rf.Start(op)

	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(timeOut * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			DPrintf("S%d Received Raft Code Put/Append resp, ErrWrongLeader", kv.me)
			reply.Err = ErrWrongLeader
		} else {
			DPrintf("S%d Received Raft Code Put/Append resp, OK", kv.me)
			reply.Err = OK
		}

	case <-timer.C:
		reply.Err = ErrWrongLeader
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

func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		msg := <-kv.applyCh
		DPrintf("S%d Received Raft Code commit msg", kv.me)
		index := msg.CommandIndex
		op := msg.Command.(Op)
		if !kv.isDuplicate(op.ClientId, op.SeqId) {
			kv.mu.Lock()
			switch op.OpType {
			case PutOp:
				DPrintf("S%d handler Raft Code Put applyMsg, Put val to stateMachine", kv.me)
				kv.stateMachine.Put(op.Key, op.Value)
			case AppendOp:
				DPrintf("S%d handler Raft Code Append applyMsg, Append val to stateMachine", kv.me)
				kv.stateMachine.Append(op.Key, op.Value)
			}
			kv.seqMap[op.ClientId] = op.SeqId
			kv.mu.Unlock()
		} else {
			DPrintf("S%d applyMsg is duplicate, op.ClientId:%d, op.SeqId:%d", kv.me, op.ClientId, op.SeqId)
		}
		// 通知可能正在等待该操作结果的客户端
		kv.getWaitCh(index) <- op
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.seqMap = make(map[int64]int64)
	kv.stateMachine = NewMemoryKV()
	kv.waitChMap = make(map[int]chan Op)

	go kv.applyMsgHandlerLoop()
	return kv
}
