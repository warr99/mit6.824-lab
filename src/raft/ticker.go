package raft

import (
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

// 选举定时器
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		nowTime := time.Now()

		// 根据当前节点的标识生成一个超时时间，并通过 Sleep 函数等待该超时时间。
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		// 上锁，以确保在操作 Raft 实例状态时不会被其他协程干扰。
		rf.mu.Lock()

		// 如果当前时间超过了上一次投票的时间，并且当前节点不是 Leader 状态，则触发选举流程。
		if rf.votedTimer.Before(nowTime) && rf.status != Leader {
			// 将节点状态转变为 Candidate。
			rf.status = Candidate
			// 为自己投票。
			rf.votedFor = rf.me
			// 初始化投票计数为 1，表示自己已经投给自己一票。
			rf.voteNum = 1
			// 更新当前任期号。
			rf.currentTerm += 1
			// 持久化 Raft 实例的状态。
			rf.persist()

			// 发送选举请求给其他节点。
			rf.sendElection()

			// 更新投票定时器的时间，以防止在短时间内多次触发选举。
			rf.votedTimer = time.Now()
		}
		// 解锁，释放对 Raft 实例的锁。
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the status machine
	for !rf.killed() {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.getLogEntry(rf.lastApplied).Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}

}
