package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

//----------------------------------------------------结构体定义部分------------------------------------------------------

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // raft服务层传来的Index
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap    map[int64]int     //为了确保seq只执行一次	clientId / seqId
	waitChMap map[int]chan Op   //传递由下层Raft服务的appCh传过来的command	index / chan(Op)
	kvPersist map[string]string // 存储持久化的KV键值对	K / V

	lastIncludeIndex int // raft对应的点
}

//-------------------------------------------------初始化(Start)部分------------------------------------------------------

// StartKVServer
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
//
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
	kv.seqMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

	kv.lastIncludeIndex = -1

	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.applyMsgHandlerLoop()
	return kv
}

//------------------------------------------------------Rpc部分---------------------------------------------------------

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op传到下层start
	op := Op{OpType: "Get", Key: args.Key, SeqId: args.SeqId, ClientId: args.ClientId}
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", kv.me, op)
	lastIndex, _, _ := kv.rf.Start(op)

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", kv.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op传到下层start
	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, SeqId: args.SeqId, ClientId: args.ClientId}
	//fmt.Printf("[ ----Server[%v]----] : send a %v,op is :%+v \n", kv.me, args.OpType, op)
	lastIndex, _, _ := kv.rf.Start(op)

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a %vAsk :%+v,OpType:%+v\n", kv.me, args.OpType, args, replyOp)
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}

	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

	defer timer.Stop()
}

// ------------------------------------------------------Loop部分--------------------------------------------------------
// 处理applyCh发送过来的ApplyMsg
func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:

			if msg.CommandValid {

				// 传来的信息快照已经存储了
				if msg.CommandIndex <= kv.lastIncludeIndex {
					return
				}

				index := msg.CommandIndex
				op := msg.Command.(Op)
				//fmt.Printf("[ ~~~~applyMsgHandlerLoop~~~~ ]: %+v\n", msg)
				if !kv.ifDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case "Put":
						kv.kvPersist[op.Key] = op.Value
					case "Append":
						kv.kvPersist[op.Key] += op.Value
					}
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				}

				// 如果需要snapshot，且超出其stateSize
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				// 将返回的ch返回waitCh
				kv.getWaitCh(index) <- op
			}

			if msg.SnapshotValid {
				kv.mu.Lock()
				// 判断此时有没有竞争
				// if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// 读取快照的数据
					kv.DecodeSnapShot(msg.Snapshot)
					kv.lastIncludeIndex = msg.SnapshotIndex
				// }
				kv.mu.Unlock()
			}

		}
	}
}

//------------------------------------------------------持久化部分--------------------------------------------------------

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int

	if d.Decode(&kvPersist) == nil && d.Decode(&seqMap) == nil {
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
	} else {
		fmt.Printf("[Server(%v)] Failed to decode snapshot！！！", kv.me)

	}
}

// PersistSnapShot 持久化快照对应的map
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}

//------------------------------------------------------utils部分--------------------------------------------------------

// Kill
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

func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
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
