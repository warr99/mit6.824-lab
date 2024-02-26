package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

//		 +-------------------+
//		 | Shard Controller  |
//		 +-------------------+
//		/          |          \
//	   /		   |		   \
//
// +-------------+   +-------------+   	+-------------+
// | Shard 1 (a) |   | Shard 2 (b) |   	| Shard 3 (c) |
// +-------------+   +-------------+   	+-------------+
//
//	|                   |                    |
//	v                   v                    v
//
// +-------------------+ +-------------------+ +-------------------+
// | Replication Group | | Replication Group | | Replication Group |
// |   (gid=1)         | |   (gid=2)         | |   (gid=3)         |
// |   Node 1 Node 2   | |   Node 3 Node 4   | |   Node 5  Node 6  |
// +-------------------+ +-------------------+ +-------------------+
// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // 配置的编号,用于唯一标识不同的配置
	Shards [NShards]int     // shard -> gid 分片集合,在这个lab中一共有10个分片
	Groups map[int][]string // gid -> servers 一个分片对应的节点的集合
}

const (
	OK          = "OK"
	WrongLeader = "WrongLeader"
)

const (
	JoinOp  OpType = "JoinType"
	LeaveOp OpType = "LeaveType"
	MoveOp  OpType = "MoveType"
	QueryOp OpType = "QueryType"
)

const (
	JoinOverTime  = 100
	LeaveOverTime = 100
	MoveOverTime  = 100
	QueryOverTime = 100
)

type Err string
type OpType string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	SeqId    int
	ClientId int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	SeqId    int
	ClientId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	SeqId    int
	ClientId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	SeqId    int
	ClientId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
