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
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		rf.mu.Lock()

		// 时间过期发起选举
		// 如果 sleep 之后，votedTimer 的时间没有被重置，还是在 nowTime 之前 -> 发起新一轮的选举
		if rf.votedTimer.Before(nowTime) && rf.status != Leader {
			// 转变状态
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.persist()
			Debug(dTerm, "S%d Follower becomes Candidate and initiates elections, currentTerm:%d", rf.me, rf.currentTerm)
			rf.sendElection()
			rf.votedTimer = time.Now()

		}
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
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() && !rf.killed() {
			rf.lastApplied += 1
			log, _ := rf.restoreLog(rf.lastApplied)
			Debug(dCommit, "S%d put the committed entry to apply on the status machine, lastApplied:%d, Command:%d",
				rf.me, rf.lastApplied, log.Command)
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       log.Command,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}

}
