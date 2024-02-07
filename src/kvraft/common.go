package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId int64  // 客户端id
	Op       OpType // "Put" or "Append"
	SeqId    int64  // 序列号
	Key      string // 键
	Value    string // 值
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SeqId int64 // 序列号
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
