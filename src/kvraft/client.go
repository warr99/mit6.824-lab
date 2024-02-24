package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	seqId    int
	leaderId int // 确定哪个服务器是leader，下次直接发送给该服务器
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

// Get
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
	ck.seqId++
	args := GetArgs{Key: key, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId
	for {

		reply := GetReply{}
		DPrintf("C%d -> S%d send Get, seqId:%d", ck.clientId, serverId, ck.seqId)
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				DPrintf("C%d <- S%d Received Get reply (NoKey), confirm LeaderId:%d", ck.clientId, ck.leaderId, serverId)
				return ""
			} else if reply.Err == OK {
				ck.leaderId = serverId
				DPrintf("C%d <- S%d Received Get reply (OK), confirm LeaderId:%d", ck.clientId, ck.leaderId, serverId)
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				DPrintf("C%d <- S%d Received Get reply (WrongLeader), change LeaderId:%d", ck.clientId, ck.leaderId, serverId)
				continue
			}
		}
		DPrintf("C%d <- S%d No Get response received, change LeaderId:%d", ck.clientId, ck.leaderId, serverId)
		// 节点发生crash等原因
		serverId = (serverId + 1) % len(ck.servers)

	}

}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqId: ck.seqId}
	for {

		reply := PutAppendReply{}
		DPrintf("C%d -> S%d send putAppend, seqId:%d", ck.clientId, serverId, ck.seqId)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				DPrintf("C%d <- S%d Received putAppend reply, confirm LeaderId:%d", ck.clientId, ck.leaderId, serverId)
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				DPrintf("C%d <- S%d Received putAppend reply, change LeaderId:%d", ck.clientId, ck.leaderId, serverId)
				continue
			}
		}
		DPrintf("C%d <- S%d No PutAppend response received, change LeaderId:%d", ck.clientId, ck.leaderId, serverId)
		serverId = (serverId + 1) % len(ck.servers)

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
