package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int   // 领导者节点id
	clientId int64 // 当前客户端id
	seqId    int64 // 操作序列号
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// init Clerk
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.seqId = -1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	atomic.AddInt64(&ck.seqId, 1)
	serverId := ck.leaderId
	args := GetArgs{
		SeqId: ck.seqId,
		Key:   key,
	}
	for {
		reply := GetReply{}
		Debug(dClient, "S%d -> S%d send Get, seqId:%d", ck.clientId, serverId, ck.seqId)
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				Debug(dClient, "S%d <- S%d Received Get reply (NoKey), confirm LeaderId:", ck.clientId, ck.leaderId, serverId)
				return ""
			} else if reply.Err == OK {
				ck.leaderId = serverId
				Debug(dClient, "S%d <- S%d Received Get reply (OK), confirm LeaderId:", ck.clientId, ck.leaderId, serverId)
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				Debug(dClient, "S%d <- S%d Received Get reply (WrongLeader), change LeaderId:", ck.clientId, ck.leaderId, serverId)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	// You will have to modify this function.
	atomic.AddInt64(&ck.seqId, 1)
	serverId := ck.leaderId
	args := PutAppendArgs{
		ClientId: ck.clientId,
		Op:       op,
		SeqId:    ck.seqId,
		Key:      key,
		Value:    value,
	}
	for {
		reply := PutAppendReply{}
		Debug(dClient, "C%d -> S%d send putAppend, seqId:%d", ck.clientId, serverId, ck.seqId)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				Debug(dClient, "S%d <- S%d Received putAppend reply, confirm LeaderId:", ck.clientId, ck.leaderId, serverId)
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				Debug(dClient, "S%d <- S%d Received putAppend reply, change LeaderId:", ck.clientId, ck.leaderId, serverId)
				continue
			}
		} else {
			Debug(dClient, "S%d <- S%d No response received, change LeaderId:", ck.clientId, ck.leaderId, serverId)
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
