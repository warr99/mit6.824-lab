package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int        // leader 任期
	LeaderId     int        // 领导人id
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term        int  // 当前任期，对于领导人而言 它会更新自己的任期
	Success     bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	UpNextIndex int  // 第一个与 XTerm 相同的日志条目的索引。领导者可以使用这个信息来找到从哪里开始重新发送日志。
}

func (rf *Raft) leaderAppendEntries() {

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// 开启协程并发的进行日志增量
		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}

			// rf.nextIndex[i]-1 <= lastIncludeIndex -> Follower 的日志小于 Leader 的快照状态，将自己的快照发过去
			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				Debug(dLog, "S%d old logs were cleared after the snapshot, send an InstallSnapshot RPC instead", rf.me)
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			re := rf.sendAppendEntries(server, &args, &reply)

			if re {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				Debug(dLog, "S%d <- S%d Received send entries reply at T%d.", rf.me, server, rf.currentTerm)
				if rf.status != Leader {
					return
				}
				// 返回的任期已经落后当前任期 -> reply 无效
				if reply.Term < rf.currentTerm {
					Debug(dLog, "S%d Reply Term lower, invalid send entry reply. (%d < %d)",
						rf.me, reply.Term, rf.currentTerm)
					return
				}
				// 返回的任期大于当前任期 -> 当前节点落后 change to Follower
				if reply.Term > rf.currentTerm {
					Debug(dLog, "S%d Current Term lower, change to Follower (%d < %d)",
						rf.me, reply.Term, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.votedTimer = time.Now()
					return
				}
				// 当前任期不等于发送请求时的任期(发送之后任期发生改变) -> 放弃该回复
				if rf.currentTerm != args.Term {
					Debug(dWarn, "S%d Term has changed after the append request, send entry reply discarded. "+
						"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
					return
				}

				if reply.Success {
					// 更新 Follower 节点的 commitIndex 和 matchIndex
					rf.commitIndex = rf.lastIncludeIndex
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 外层遍历下标是否满足,从最大的索引开始反向遍历
					for index := rf.getLastIndex(); index >= rf.lastIncludeIndex+1; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						// 如果超过半数以上节点的匹配索引满足条件，且当前索引对应的任期与当前节点的任期相同
						if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							Debug(dCommit, "S%d Updated commitIndex at T%d for majority consensus. commitIndex: %d.", rf.me, rf.currentTerm, rf.commitIndex)
							break
						}

					}
				} else {
					// 追加日志发生冲突 -> 进行更新
					Debug(dLog, "S%d <- S%d Inconsistent logs, need retry. reset nextIndex=%d", rf.me, server, reply.UpNextIndex)
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
					}
				}
			}

		}(index)

	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// gid := getGID()
	// beforeLock(gid, "AppendEntries", rf.me)
	// startGetLockTime := time.Now()
	rf.mu.Lock()
	// afterLock(gid, "AppendEntries", startGetLockTime, rf.me)
	// startHoldLockTime := time.Now()
	defer func() {
		// afterUnlock(gid, "AppendEntries", startHoldLockTime, rf.me)
		rf.mu.Unlock()
	}()
	if len(args.Entries) == 0 {
		Debug(dLog2, "S%d <- S%d Received heartbeat at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dLog2, "S%d <- S%d Received append entries at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	}
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is lower, rejecting append request. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}
	// 重置 Follower 节点的状态
	reply.Success = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.status = Follower
	rf.currentTerm = args.Term
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	// 如果自身的快照索引比请求中的 PrevLogIndex 还大，说明存在冲突，返回冲突的下一个索引（用于更新 Leader 的 nextIndex）
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}

	// 如果自身的最后日志索引小于请求中的 PrevLogIndex，说明 Follower 缺失日志，返回自身的最后索引
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex()
		return
	} else {
		// 如果在 prevLogIndex 处的日志条目的 term 与 prevLogTerm 不匹配，那么回复 false (§5.3)
		argsPrevLogIndexTerm := rf.restoreLogTerm(args.PrevLogIndex)
		if argsPrevLogIndexTerm != args.PrevLogTerm {
			reply.Success = false
			// the follower can include the term of the conflicting entry and the first index it stores for that term.
			// With this information, the leader can decrement nextIndex to bypass all of the conflicting entries in that term;
			// one AppendEntries RPC will be required for each term with conflicting entries, rather than one RPC per entry.
			reply.UpNextIndex = rf.getMinIndexInOneTerm(argsPrevLogIndexTerm, args.PrevLogIndex)
			Debug(dLog2, "S%d The term of the log entry at prevLogIndex does not match prevLogTerm(%d != %d), reset UpNextIndex: %d",
				rf.me, argsPrevLogIndexTerm, args.PrevLogTerm, reply.UpNextIndex)
			return
		}
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	// 进行日志的截取
	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
	rf.persist()

	//update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(args.Entries) == 0 {
		Debug(dLog, "S%d -> S%d Leader send heartbeat", rf.me, server)
	} else {
		Debug(dLog, "S%d -> S%d Leader send entries, PrevLogIndex:%d, PrevLogTerm:%d",
			rf.me, server, args.PrevLogIndex, args.PrevLogTerm)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
