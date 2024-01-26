package raft

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	Debug(dSnap, "S%d <- S%d Received install snapshot request at T%d.", rf.me, args.LeaderId, rf.currentTerm)

	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d Term is lower, rejecting install snapshot request. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.timer.Reset(HeartBeatTimeout)
	// 将快照后的logs切割，快照前的直接applied
	argsLastIncludeIndex := args.LastIncludedIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})
	lastLogIndex, _ := rf.lastLogInfo()
	for i := argsLastIncludeIndex + 1; i <= lastLogIndex; i++ {
		tempLog = append(tempLog, *rf.getEntry(i))
	}
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.logs = tempLog
	if argsLastIncludeIndex > rf.commitIndex {
		rf.commitIndex = argsLastIncludeIndex
	}
	if argsLastIncludeIndex > rf.lastApplied {
		rf.lastApplied = argsLastIncludeIndex
		Debug(dSnap, "S%d instal Snapshot, set lastApplied=%d", rf.me, rf.lastApplied)
	}
	rf.persistAndSnapshot(args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyChan <- msg
}

func (rf *Raft) sendSnapshot(server int) {
	Debug(dSnap, "S%d -> S%d Sending installing snapshot request at T%d.", rf.me, server, rf.currentTerm)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	go rf.leaderSendSnapshot(args, server)
}

func (rf *Raft) leaderSendSnapshot(args *InstallSnapshotArgs, server int) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dSnap, "S%d <- S%d Received install snapshot reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid install snapshot reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the install snapshot request, install snapshot reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		rf.checkTerm(reply.Term)
		newNext := args.LastIncludedIndex + 1
		newMatch := args.LastIncludedIndex
		rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
		rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])
	}
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
