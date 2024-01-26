package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 投票请求的结构体
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人所在的任期
	CandidateId  int // 请求选票的候选人的 ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 投票响应的结构体
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int         // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool        // 候选人赢得了此张选票时为真
	Replystatus VotedStatus // 投票状态枚举
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votedNums *int) bool {
	Debug(dVote, "S%d send a voting request to %v", rf.me, server)
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// 失败重传
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	// 加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return false
	}
	switch reply.Replystatus {
	// 接收请求的节点 clash
	case Clash:
		{
			return false
		}
	// 当前竞选者过期了
	case Outdated:
		{
			rf.status = Follower
			rf.timer.Reset(rf.electionTimeout)
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
			}
		}
	// 正常的选举（获得选票/请求的节点已经把票给出去了）
	case Voted, Normal:
		{
			if reply.VoteGranted && reply.Term == rf.currentTerm && *votedNums <= (len(rf.peers)/2) {
				*votedNums++
			}
			// 如果选票达到多数派
			if *votedNums >= (len(rf.peers)/2)+1 {
				*votedNums = 0
				if rf.status == Leader {
					return ok
				}
				// 本身不是leader，初始化nextIndex数组
				rf.status = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				lastLogIndex, _ := rf.lastLogInfo()
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = lastLogIndex + 1
					rf.matchIndex[i] = 0
				}
				rf.timer.Reset(HeartBeatTimeout)
				Debug(dTerm, "S%d Reaching the majority and becoming the leader\n", rf.me)
			}
		}
	}
	// 如果没有获得选票
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 当前节点crash
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		reply.Replystatus = Clash
		return
	}
	// 该竞选者已经过时
	if args.Term < rf.currentTerm {
		// 告诉该竞选者当前的 Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.Replystatus = Outdated
		return
	}

	if args.Term > rf.currentTerm {
		// 重置自身的状态
		rf.status = Follower
		rf.currentTerm = args.Term
		// 单有 args.Term > rf.currentTerm 还不能直接投票
		rf.votedFor = -1
		rf.persist()
	}
	// 如果 args.Term > rf.currentTerm
	if rf.votedFor == -1 {
		lastLogIndex, lastLogTerm := rf.lastLogInfo()
		// 如果不能满足 args.LastLogIndex < currentLogIndex  args.LastLogTerm < currentLogTerm 任一条件，都不能投票
		// 请求投票（RequestVote） RPC 实现了这样的限制：RPC 中包含了候选人的日志信息，然后投票人会拒绝掉那些日志没有自己新的投票请求。
		// Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。\
		// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {

			reply.Replystatus = Outdated
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			Debug(dVote, "S%d refuse voted for S%d. Outdated",
				rf.me,
				args.CandidateId)
			return
		}

		// 给票数，并且返回true
		rf.votedFor = args.CandidateId
		reply.Replystatus = Normal
		reply.Term = rf.currentTerm
		rf.persist()
		reply.VoteGranted = true

		rf.timer.Reset(rf.electionTimeout)
		Debug(dVote, "S%d voted for S%d\n", rf.me, rf.votedFor)
	} else {
		// 如果 args.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.Replystatus = Voted
		// 票已经给了同一轮选举的另外的竞争者
		if rf.votedFor != args.CandidateId {
			return
		} else {
			// 票已经给过当前发送请求的节点了
			rf.status = Follower
		}
		rf.timer.Reset(rf.electionTimeout)
	}
}