package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seqId    int
	leaderId int
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
	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	ck.seqId = 0
	return ck
}

// 获取系统的配置信息
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 用于添加新的复制组
func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	// Your code here.
	ck.seqId++
	args.Servers = servers
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	serverId := ck.leaderId

	for {
		reply := JoinReply{}
		DPrintf("C%v -> S%v send a Join, args:%v", ck.clientId, serverId, args)
		ok := ck.servers[serverId].Call("ShardCtrler.Join", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.WrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// 移除先前加入的复制组
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 将特定分片移动到指定的复制组
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
