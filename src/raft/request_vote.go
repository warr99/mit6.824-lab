package raft

import (
	"time"
)

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

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 开启协程对各个节点发起选举
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(server, &args, &reply)

			if res == true {
				rf.mu.Lock()
				// 判断自身是否还是竞选者，且任期不冲突
				if rf.status != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > args.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				// 返回结果正确判断是否大于一半节点同意
				if reply.VoteGranted == true && rf.currentTerm == args.Term {
					rf.voteNum += 1
					if rf.voteNum >= len(rf.peers)/2+1 {

						//fmt.Printf("[++++elect++++] :Rf[%v] to be leader,term is : %v\n", rf.me, rf.currentTerm)
						rf.status = Leader
						rf.votedFor = -1
						rf.voteNum = 0
						rf.persist()

						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()

						rf.votedTimer = time.Now()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
				return
			}

		}(i)

	}

}

// RequestVote
// example RequestVote RPC handler.
// 个人认为定时刷新的地方应该是别的节点与当前节点在数据上不冲突时才要刷新
// 因为如果不是数据冲突那么定时相当于防止自身去选举的一个心跳
// 如果是因为数据冲突，那么这个节点不用刷新定时是为了当前整个raft能尽快有个正确的leader
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 由于网络分区或者是节点crash，导致的任期比接收者还小，直接返回
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	// 预期的结果:任期大于当前节点，进行重置
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.persist()
	}

	// If votedFor is null or candidateId, and candidate’s logs is at
	// least as up-to-date as receiver’s logs, grant vote
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) || // 判断日志是否conflict
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term { // paper中的第二个条件votedFor is null

		// 满足以上两个其中一个都返回false，不给予投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedTimer = time.Now()
		rf.persist()
		return
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
