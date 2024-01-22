package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   创建一个新的 Raft 服务器实例。这个函数可能会初始化一些必要的数据结构和状态。
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   查询 Raft 节点的当前任期（term）和它是否认为自己是领导者（isLeader）。
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 节点的角色
type Status int
type LogEntries []LogEntry

// 节点角色枚举
const (
	Follower  Status = iota // 跟随者
	Candidate               // 竞争者
	Leader                  // 领导者
)

// 投票响应的类型
type VotedStatus int

const (
	Clash    VotedStatus = iota // 节点clash
	Outdated                    // 竞选者过时（任期落后或者日落后）
	Voted                       // 该节点的票已经投出去了
	Normal                      // 投票，竞选者获得选票
)

type AppendEntriesStatus int

const (
	Killed              AppendEntriesStatus = iota // 节点clash
	Expire                                         // 领导者任期落后
	LogMismatch                                    // 日志不匹配
	Applied                                        // Leader 日志落后, Follower 已经应用
	AppendEntriesNormal                            // 正常

)

// 日志条目
type LogEntry struct {
	Term    int         // 领导人接收到该条目时的任期（初始索引为1）
	Command interface{} // 用于状态机的命令（空接口->可以存储任何类型的数据）
}

// 定义一个全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)

	currentTerm int        // 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor    int        // 当前任期内收到选票的 candidateId ，如果没有投给任何候选人则为空
	logs        LogEntries //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）

	// 所有服务器上的易失性状态

	commitIndex int // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	// 领导人（服务器）上的易失性状态 (选举后已经重新初始化)

	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	// Others
	status          Status        // 当前节点状态
	electionTimeout time.Duration // 追随者在成为候选人之前等待的时间
	timer           *time.Ticker  // 计时器

	applyChan chan ApplyMsg // 用来写入通道
}

// Get the index of first entry and last entry with the given term.
// Return (-1,-1) if no such term is found
func (logEntries LogEntries) getBoundsWithTerm(term int) (minIndex int, maxIndex int) {
	if term == 0 {
		return 0, 0
	}
	minIndex, maxIndex = math.MaxInt, -1
	for i := 1; i <= len(logEntries); i++ {
		if logEntries[i-1].Term == term {
			minIndex = int(math.Min(float64(minIndex), float64(i)))
			maxIndex = int(math.Max(float64(maxIndex), float64(i)))
		}
	}
	if maxIndex == -1 {
		return -1, -1
	}
	return
}
func (logEntries LogEntries) lastLogInfo() (index, term int) {
	index = len(logEntries)
	logEntry := logEntries.getEntry(index)
	return index, logEntry.Term
}

func (logEntries LogEntries) getEntry(index int) *LogEntry {
	if index < 0 {
		log.Panic("LogEntries.getEntry: index < 0.\n")
	}
	if index == 0 {
		return &LogEntry{
			Command: nil,
			Term:    0,
		}
	}
	if index > len(logEntries) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logEntries[index-1]
}

func (logEntries LogEntries) getSlice(startIndex, endIndex int) LogEntries {
	if startIndex <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			startIndex, len(logEntries))
		log.Panic("LogEntries.getSlice: startIndex out of range. \n")
	}
	if endIndex > len(logEntries)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			endIndex, len(logEntries))
		log.Panic("LogEntries.getSlice: endIndex out of range.\n")
	}
	if startIndex > endIndex {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
		log.Panic("LogEntries.getSlice: startIndex > endIndex.\n")
	}
	return logEntries[startIndex-1 : endIndex-1]
}

// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
// Check if current term is out of date when hearing from other peers,
// update term, revert to follower state and return true if necesarry
func (rf *Raft) checkTerm(term int) bool {
	if rf.currentTerm < term {
		Debug(dTerm, "S%d Term is higher, updating term to T%d, setting state to follower. (%d > %d)",
			rf.me, term, term, rf.currentTerm)
		rf.status = Follower
		rf.currentTerm = term
		rf.votedFor = -1
		return true
	}
	return false
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	Debug(dPersist, "S%d saving Raft's persistent state to stable storage at T%d.", rf.me, rf.currentTerm)
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.currentTerm\". err: %v, data: %v", err, rf.currentTerm)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.votedFor\". err: %v, data: %v", err, rf.votedFor)
	}
	if err := e.Encode(rf.logs); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.logs\". err: %v, data: %v", err, rf.logs)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// 字节缓冲区
	r := bytes.NewBuffer(data)
	// Gob 解码器
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.currentTerm\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&votedFor); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.votedFor\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&logs); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.logs\". err: %v, data: %s", err, data)
	}
	rf.currentTerm = currentTerm
	rf.logs = logs
	rf.votedFor = votedFor
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
		lastLogTerm := 0
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}

		// 如果不能满足 args.LastLogIndex < currentLogIndex  args.LastLogTerm < currentLogTerm 任一条件，都不能投票
		// 请求投票（RequestVote） RPC 实现了这样的限制：RPC 中包含了候选人的日志信息，然后投票人会拒绝掉那些日志没有自己新的投票请求。
		// Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。\
		// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		if args.LastLogTerm < lastLogTerm || (len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)) {

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
		rf.persist()
		reply.Replystatus = Normal
		reply.Term = rf.currentTerm
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

// 处理心跳请求、同步日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		Debug(dLog2, "S%d <- S%d Received heartbeat at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dLog2, "S%d <- S%d Received append entries at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	}
	reply.Success = false
	// 返回假 如果领导人的任期小于接收者的当前任期
	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is lower, rejecting append request. (%d < %d)",
			rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	if rf.status == Candidate && rf.currentTerm == args.Term {
		rf.status = Follower
		Debug(dLog2, "S%d Convert from candidate to follower at T%d.", rf.me, rf.currentTerm)
	}

	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.timer.Reset(rf.electionTimeout)

	// 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.logs.getEntry(args.PrevLogIndex).Term {
		Debug(dLog2, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
		reply.XLen = len(rf.logs)
		reply.XTerm = rf.logs.getEntry(args.PrevLogIndex).Term
		reply.XIndex, _ = rf.logs.getBoundsWithTerm(reply.XTerm)
		return
	}
	// 如果一个已经存在的条目和新条目发生了冲突（因为索引相同，任期不同），
	// 那么就删除这个已经存在的条目以及它之后的所有条目
	for i, entry := range args.Entries {
		if rf.logs.getEntry(i+1+args.PrevLogIndex).Term != entry.Term {
			Debug(dLog2, "S%d Running into conflict with existing entries at T%d. conflictLogs: %v, startIndex: %d.",
				rf.me, rf.currentTerm, args.Entries[i:], i+1+args.PrevLogIndex)
			rf.logs = append(rf.logs.getSlice(1, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}
	Debug(dLog2, "S%d <- S%d Append entries success. Saved logs: %v.", rf.me, args.LeaderId, args.Entries)
	if len(args.Entries) > 0 {
		rf.persist()
	}
	// 如果"领导人的已知已提交的最高日志条目的索引"大于"接收者的已知已提交最高日志条目的索引"(leaderCommit > commitIndex)，
	// 则把接收者的已知"已经提交的最高的日志条目的索引commitIndex"重置为
	// "领导人的已知已经提交的最高的日志条目的索引 leaderCommit"或者是"上一个新条目的索引",取两者的最小值
	if args.LeaderCommit > rf.commitIndex {
		Debug(dCommit, "S%d Get higher LC at T%d, updating commitIndex. (%d < %d)",
			rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(args.PrevLogIndex+len(args.Entries))))
		Debug(dCommit, "S%d Updated commitIndex at T%d. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
	}
	reply.Success = true
}

type AppendEntriesArgs struct {
	Term         int        // leader 任期
	LeaderId     int        // 领导人id
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term         int  // 当前任期，对于领导人而言 它会更新自己的任期
	Success      bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	XTerm        int // term in the conflicting entry (if any)
	XIndex       int // index of first entry with that term (if any)
	XLen         int // log length
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
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.logs) + 1
				}
				rf.timer.Reset(HeartBeatTimeout)
				Debug(dTerm, "S%d Reaching the majority and becoming the leader\n", rf.me)
			}
		}
	}
	// 如果没有获得选票
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) {
	if rf.killed() {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dLog, "S%d <- S%d Received send entry reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid send entry reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the append request, send entry reply discarded."+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		if rf.checkTerm(reply.Term) {
			return
		}
		if reply.Success {
			// 如果成功：更新相应跟随者的 nextIndex 和 matchIndex
			Debug(dLog, "S%d <- S%d Log entries in sync at T%d.", rf.me, server, rf.currentTerm)
			newNext := args.PrevLogIndex + 1 + len(args.Entries)
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = int(math.Max(float64(newNext), float64(rf.nextIndex[server])))
			rf.matchIndex[server] = int(math.Max(float64(newMatch), float64(rf.matchIndex[server])))
			// 假设存在 N 满足N > commitIndex，
			// 使得大多数的 matchIndex[i] ≥ N以及log[N].term == currentTerm 成立，
			// 则令 commitIndex = N
			for N := len(rf.logs); N > rf.commitIndex && rf.logs.getEntry(N).Term == rf.currentTerm; N-- {
				count := 1
				// 遍历每个节点，检查其 matchIndex 是否大于等于 N
				for peer, matchIndex := range rf.matchIndex {
					if peer == rf.me {
						continue
					}
					if matchIndex >= N {
						count++
					}
				}
				// 如果有大多数的节点的 matchIndex 大于等于 N，更新 commitIndex
				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					Debug(dCommit, "S%d Updated commitIndex at T%d for majority consensus. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
					break
				}
			}
		} else {
			// 如果因为日志不一致而失败，则 nextIndex 递减并重试
			if reply.XTerm == -1 {
				// follower's log is too short
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				_, maxIndex := rf.logs.getBoundsWithTerm(reply.XTerm)
				if maxIndex != -1 {
					// leader has XTerm
					rf.nextIndex[server] = maxIndex
				} else {
					// leader doesn't have XTerm
					rf.nextIndex[server] = reply.XIndex
				}
			}
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return index, term, false
	}
	isLeader = true
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	Debug(dLog, "S%d leader append log, command:%v\n", rf.me, command)
	rf.logs = append(rf.logs, appendLog)
	rf.persist()
	index = len(rf.logs)
	term = rf.currentTerm
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		<-rf.timer.C
		// 定时器被触发
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		switch rf.status {
		// 当前为跟随者，触发定时器意味着已经等待了electionTimeout，状态变化为竞选者
		case Follower:
			rf.status = Candidate
			fallthrough // 继续执行下一个分支
		// 当前为候选人状态，把选票投给自己，并行地向集群中的其他服务器节点发送请求投票的 RPCs 来给自己投票
		case Candidate:
			// 任期 +1
			rf.currentTerm++
			// 投票给自己
			rf.votedFor = rf.me
			// 收到的选票数
			votedNums := 1
			// 开启新的选举任期
			rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
			rf.persist()
			rf.timer.Reset(rf.electionTimeout)
			// 并行地向集群中的其他服务器节点发送请求投票的 RPCs 来给自己投票
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				voteArgs := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.logs),
					LastLogTerm:  0,
				}
				if len(rf.logs) > 0 {
					voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
				}
				voteReply := RequestVoteReply{}
				go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
			}
		// 当前为领导者，进行心跳/日志同步
		case Leader:
			// 重置心跳
			Debug(dTimer, "S%d leader reset HeartBeatTimeout\n", rf.me)
			rf.timer.Reset(HeartBeatTimeout)
			appendNums := 1 // 对于正确返回的节点数量
			// 构造心跳请求
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				appendEntriesArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: 0, // 单纯的心跳不会产生日志
					PrevLogTerm:  0,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				appendEntriesReply := AppendEntriesReply{}
				// 是否有日志需要同步，跟着心跳一起发送
				appendEntriesArgs.Entries = rf.logs[rf.nextIndex[i]-1:]
				if rf.nextIndex[i] > 0 {
					appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
				}
				if appendEntriesArgs.PrevLogIndex > 0 {
					appendEntriesArgs.PrevLogTerm = rf.logs.getEntry(rf.nextIndex[i] - 1).Term
				}
				Debug(dCommit, "S%d send a append entries to S%d, append log count: %v\n", rf.me, i, len(appendEntriesArgs.Entries))
				go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, &appendNums)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyLogsLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		// Apply logs periodically until the last committed index.
		rf.mu.Lock()
		// To avoid the apply operation getting blocked with the lock held,
		// use a slice to store all committed msgs to apply, and apply them only after unlocked
		appliedMsgs := []ApplyMsg{}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.logs.getEntry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			})
			Debug(dLog2, "S%d Applying log at T%d. LA: %d, CI: %d.", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		}
		rf.mu.Unlock()
		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
		time.Sleep(time.Duration(30) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	// The election timeout is randomized to be between 150ms and 300ms.
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.timer = time.NewTicker(rf.electionTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// PrintfLog("[             Make-func-rf(%v)           ] init server, electionTimeout: %v\n", rf.me, rf.electionTimeout)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogsLoop(applyCh)

	return rf
}
