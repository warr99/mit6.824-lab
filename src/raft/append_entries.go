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
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
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
			rf.nextIndex[server] = max((newNext), rf.nextIndex[server])
			Debug(dLog, "S%d <- S%d set the new nextIndex[%d] = %d.", rf.me, server, server, rf.nextIndex[server])
			rf.matchIndex[server] = max((newMatch), rf.matchIndex[server])
			// 假设存在 N 满足N > commitIndex，
			// 使得大多数的 matchIndex[i] ≥ N以及log[N].term == currentTerm 成立，
			// 则令 commitIndex = N
			for N := rf.lastIncludedIndex + len(rf.logs); N > rf.commitIndex && rf.getEntry(N).Term == rf.currentTerm; N-- {
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
				Debug(dLog, "S%d reset the rf.nextIndex[%d]  = %d.", rf.me, server, rf.nextIndex[server])
			} else {
				_, maxIndex := rf.getBoundsWithTerm(reply.XTerm)
				if maxIndex != -1 {
					// leader has XTerm
					rf.nextIndex[server] = maxIndex
				} else {
					// leader doesn't have XTerm
					rf.nextIndex[server] = reply.XIndex
				}
			}
			lastLogIndex, _ := rf.lastLogInfo()
			nextIndex := rf.nextIndex[server]
			// 当前leader的logs中已经没有follower需要的日志，因此需要发送 InstallSnapshot RPC
			if nextIndex <= rf.lastIncludedIndex {
				rf.sendSnapshot(server)
			} else if lastLogIndex >= nextIndex {
				Debug(dLog, "S%d <- S%d Inconsistent logs, retrying.", rf.me, server)
				newArg := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
					Entries:      rf.getSlice(nextIndex, lastLogIndex+1),
				}
				go rf.leaderSendEntries(newArg, server)
			}
		}
	}
}

func (rf *Raft) sendEntries(isHeartbeat bool) {
	Debug(dTimer, "S%d Resetting HBT, wait for next heartbeat broadcast.", rf.me)
	rf.timer.Reset(HeartBeatTimeout)
	lastLogIndex, _ := rf.lastLogInfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[peer]
		if nextIndex <= rf.lastIncludedIndex {
			// current leader does not have enough log to sync the outdated peer,
			// because logs were cleared after the snapshot, then send an InstallSnapshot RPC instead
			rf.sendSnapshot(peer)
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		if lastLogIndex >= nextIndex {
			args.Entries = rf.getSlice(nextIndex, lastLogIndex+1)
			Debug(dLog, "S%d -> S%d Sending append entries at T%d. PLI: %d, PLT: %d, LC: %d. Entries: %v.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex,
				args.PrevLogTerm, args.LeaderCommit, args.Entries,
			)
			go rf.leaderSendEntries(args, peer)
		} else if isHeartbeat {
			args.Entries = make([]LogEntry, 0)
			Debug(dLog, "S%d -> S%d Sending heartbeat at T%d. PLI: %d, PLT: %d, LC: %d.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			go rf.leaderSendEntries(args, peer)
		}
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
	if args.PrevLogIndex < rf.lastIncludedIndex {
		alreadySanpshotLogLen := rf.lastIncludedIndex - args.PrevLogIndex
		if alreadySanpshotLogLen <= len(args.Entries) {
			newArgs := &AppendEntriesArgs{
				Term:         args.Term,
				LeaderId:     args.LeaderId,
				PrevLogTerm:  rf.lastIncludedTerm,
				PrevLogIndex: rf.lastIncludedIndex,
				Entries:      args.Entries[alreadySanpshotLogLen:],
				LeaderCommit: args.LeaderCommit,
			}
			args = newArgs
			Debug(dWarn, "S%d Log entry at PLI already discarded by snapshot, readjusting. PLI: %d, PLT:%d, Entries: %v.",
				rf.me, args.PrevLogIndex, args.Entries)
		} else {
			Debug(dWarn, "S%d Log entry at PLI already discarded by snapshot, assume as a match. PLI: %d.", rf.me, args.PrevLogIndex)
			reply.Success = true
			return
		}
	}
	// 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.getEntry(args.PrevLogIndex).Term {
		Debug(dLog2, "S%d Prev log entries do not match. args.PrevLogTerm=%d, rf.getEntry(%d).Term=%d.Ask leader to retry.",
			rf.me,
			args.PrevLogTerm,
			args.PrevLogIndex,
			rf.getEntry(args.PrevLogIndex).Term)
		// 当前节点的日志长度
		reply.XLen = len(rf.logs) + rf.lastIncludedIndex
		// 为前一个日志条目的任期
		reply.XTerm = rf.getEntry(args.PrevLogIndex).Term
		// 具有相同任期的日志条目的最小索引(若找不到相同任期,为-1)
		reply.XIndex, _ = rf.getBoundsWithTerm(reply.XTerm)
		return
	}
	// 如果一个已经存在的条目和新条目发生了冲突（因为索引相同，任期不同），
	// 那么就删除这个已经存在的条目以及它之后的所有条目
	// 遍历领导者发送的新的日志条目
	for i, entry := range args.Entries {
		// 检查新的日志条目与已有日志条目在相应索引上的任期是否一致
		if rf.getEntry(i+1+args.PrevLogIndex).Term != entry.Term {
			// 发生了冲突
			Debug(dLog2, "S%d Running into conflict with existing entries at T%d. conflictLogs: %v, startIndex: %d.",
				rf.me, rf.currentTerm, args.Entries[i:], i+1+args.PrevLogIndex)
			// 当前节点已有的与新的日志条目冲突的部分之前的日志切片 + 新的日志条目中产生冲突之后的部分
			// 重新赋值给当前节点的日志
			rf.logs = append(rf.getSlice(1+rf.lastIncludedIndex, i+1+args.PrevLogIndex), args.Entries[i:]...)
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
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		Debug(dCommit, "S%d Updated commitIndex at T%d. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
	}
	reply.Success = true
}
