package raft

type AppendEntriesArgs struct {
	Term         int        // leader 任期
	LeaderId     int        // 领导人id
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，对于领导人而言 它会更新自己的任期
	Success bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	XTerm   int  // 冲突条目的任期
	XIndex  int  // 第一个与 XTerm 相同的日志条目的索引。领导者可以使用这个信息来找到从哪里开始重新发送日志。
	XLen    int  // log length
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderSendEntries(args *AppendEntriesArgs, server int) {
}

func (rf *Raft) sendEntries(isHeartbeat bool) {
	Debug(dTimer, "S%d Resetting HBT, wait for next heartbeat broadcast.", rf.me)
}

// 处理心跳请求、同步日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
}
