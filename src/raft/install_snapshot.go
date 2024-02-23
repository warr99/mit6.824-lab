package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term             int    // leader's term
	LeaderId         int    // so follower can redirect clients
	LastIncludeIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludeTerm  int    // term of lastIncludedIndex
	Data             []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	Debug(dSnap, "S%d -> S%d Sending installing snapshot request at T%d.", rf.me, server, rf.currentTerm)
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendInstallSnapshot(server, &args, &reply)

	if res {
		rf.mu.Lock()
		if rf.status != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			Debug(dSnap, "S%d Current Term lower, change to Follower (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			rf.status = Follower
			rf.votedFor = -1
			rf.voteNum = 0
			rf.persist()
			rf.votedTimer = time.Now()
			rf.mu.Unlock()
			return
		}
		// reset matchIndex nextIndex
		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	Debug(dSnap, "S%d <- S%d Received install snapshot request at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	if rf.currentTerm > args.Term {
		Debug(dSnap, "S%d Term is lower, rejecting install snapshot request. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.status = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.votedTimer = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		Debug(dSnap, "S%d A newer snapshot already exists, rejecting install snapshot request. (%d <= %d)",
			rf.me, args.LastIncludeIndex, rf.lastIncludeIndex)
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		restoredLogEntry, _ := rf.restoreLog(i)
		tempLog = append(tempLog, restoredLogEntry)
	}
	rf.logs = tempLog
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex
	Debug(dSnap, "S%d reset lastIncludeTerm:%d, lastIncludeIndex:%d", rf.me, rf.lastIncludeTerm, args.LastIncludeIndex)
	if index > rf.commitIndex {
		Debug(dSnap, "S%d reset commitIndex:%d", rf.me, index)
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		Debug(dSnap, "S%d reset lastApplied:%d", rf.me, index)
		rf.lastApplied = index
	}
	rf.persister.Save(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	Debug(dSnap, "S%d send snap msg to applyChan")
	rf.mu.Unlock()

	rf.applyChan <- msg

}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d Snapshotting through index %d.", rf.me, index)
	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	if rf.lastIncludeIndex >= index {
		Debug(dSnap, "S%d Snapshot already applied to persistent storage. (%d >= %d)", rf.me, rf.lastIncludeIndex, index)
		return
	}
	if rf.commitIndex < index {
		Debug(dWarn, "S%d Cannot snapshot uncommitted log entries, discard the call. (%d < %d)", rf.me, rf.commitIndex, index)
		return
	}
	// 更新快照日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		restoredLogEntry, _ := rf.restoreLog(i)
		sLogs = append(sLogs, restoredLogEntry)
	}

	// 更新快照下标/任期
	if index == rf.getLastIndex()+1 {
		rf.lastIncludeTerm = rf.getLastTerm()
	} else {
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	Debug(dSnap, "S%d reset lastIncludeTerm:%d, lastIncludeIndex:%d", rf.me, rf.lastIncludeTerm, index)
	rf.logs = sLogs

	// reset commitIndex lastApplied
	if index > rf.commitIndex {
		Debug(dSnap, "S%d reset commitIndex:%d", rf.me, index)
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		Debug(dSnap, "S%d reset lastApplied:%d", rf.me, index)
		rf.lastApplied = index
	}

	// 持久化快照信息
	rf.persister.Save(rf.persistData(), snapshot)
}
