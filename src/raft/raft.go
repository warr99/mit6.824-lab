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

// 定义一个全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

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

	lastIncludedIndex int    // 快照中包含的最后日志条目的索引值
	lastIncludedTerm  int    // 快照中包含的最后日志条目的任期号
	snapshot          []byte // 存储在内存中的快照信息
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
		rf.persist()
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
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedIndex\". err: %v, data: %v", err, rf.lastIncludedIndex)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedTerm\". err: %v, data: %v", err, rf.lastIncludedTerm)
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	if err := d.Decode(&currentTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.currentTerm\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&votedFor); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.votedFor\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&logs); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.logs\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.lastIncludedIndex\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.lastIncludedTerm\". err: %v, data: %s", err, data)
	}
	rf.currentTerm = currentTerm
	rf.logs = logs
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	Debug(dSnap, "S%d Snapshotting through index %d.", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, _ := rf.lastLogInfo()
	if rf.lastIncludedIndex >= index {
		Debug(dSnap, "S%d Snapshot already applied to persistent storage. (%d >= %d)", rf.me, rf.lastIncludedIndex, index)
		return
	}
	if rf.commitIndex < index {
		Debug(dWarn, "S%d Cannot snapshot uncommitted log entries, discard the call. (%d < %d)", rf.me, rf.commitIndex, index)
		return
	}
	newLog := rf.getSlice(index+1, lastLogIndex+1)
	newLastIncludeTerm := rf.getEntry(index).Term

	rf.lastIncludedTerm = newLastIncludeTerm
	rf.logs = newLog
	rf.lastIncludedIndex = index
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		Debug(dSnap, "S%d set lastApplied=%d at Snapshot().", rf.me, index)
		rf.lastApplied = index
	}
	rf.snapshot = snapshot
	rf.persistAndSnapshot(snapshot)
}

func (rf *Raft) persistAndSnapshot(snapshot []byte) {
	Debug(dSnap, "S%d Saving persistent state and service snapshot to stable storage at T%d.", rf.me, rf.currentTerm)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.currentTerm\". err: %v, data: %v", err, rf.currentTerm)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.votedFor\". err: %v, data: %v", err, rf.votedFor)
	}
	if err := e.Encode(rf.logs); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.log\". err: %v, data: %v", err, rf.logs)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedIndex\". err: %v, data: %v", err, rf.lastIncludedIndex)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedTerm\". err: %v, data: %v", err, rf.lastIncludedTerm)
	}
	data := w.Bytes()
	rf.persister.Save(data, snapshot)
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
	index = len(rf.logs) + rf.lastIncludedIndex
	term = rf.currentTerm
	rf.persist()
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
	Debug(dClient, "S%d Current client is exiting.", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	Debug(dInfo, "S%d init set the lastApplied=0", rf.me)
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.status = Follower
	// The election timeout is randomized to be between 150ms and 300ms.
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.timer = time.NewTicker(rf.electionTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
		Debug(dInfo, "S%d readPersist() set the lastApplied=%d", rf.me, rf.lastApplied)
	}
	go rf.ticker()
	go rf.applyLogsLoop(applyCh)

	return rf
}
