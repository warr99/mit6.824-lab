package raft

import (
	"math/rand"
	"time"
)

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
				lastLogIndex, lastLogTerm := rf.lastLogInfo()
				voteArgs := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  0,
				}
				if len(rf.logs) > 0 || rf.lastIncludedIndex != 0 {
					voteArgs.LastLogTerm = lastLogTerm
				}
				voteReply := RequestVoteReply{}
				go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
			}
		// 当前为领导者，进行心跳/日志同步
		case Leader:
			Debug(dTimer, "S%d HBT elapsed. Broadcast heartbeats.", rf.me)
			rf.sendEntries(true)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyLogsLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		// Apply logs periodically until the last committed index.
		rf.mu.Lock()
		// Debug(dInfo, "S%d set the lastApplied=%d", rf.me, rf.lastApplied)
		// To avoid the apply operation getting blocked with the lock held,
		// use a slice to store all committed msgs to apply, and apply them only after unlocked
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		appliedMsgs := []ApplyMsg{}
		// rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex()
		lastIndex, _ := rf.lastLogInfo()
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < lastIndex {
			rf.lastApplied++
			Debug(dSnap, "S%d applyLogsLoop, set lastApplied++ =%d", rf.me, rf.lastApplied)
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.getEntry(rf.lastApplied).Command,
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
