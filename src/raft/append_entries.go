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
	Term    int  // 当前任期，对于领导人而言 它会更新自己的任期
	Success bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	XIndex  int  // 第一个与 XTerm 相同的日志条目的索引。领导者可以使用这个信息来找到从哪里开始重新发送日志。
}

func (rf *Raft) leaderAppendEntries() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[server]-1 < rf.lastIncludedIndex {
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
			// 如果有日志需要同步
			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludedIndex:]...)
				args.Entries = entries
			}
			// 如果没有 发送心跳，日志留空
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			Debug(dLog, "S%d->S%d send AppendEntries", rf.me, server)
			ok := rf.sendAppendEntries(server, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				Debug(dLog, "S%d<-S%d Received send entry reply at T%d.", rf.me, server, rf.currentTerm)
				if rf.status != Leader {
					return
				}
				if reply.Term > rf.currentTerm {
					Debug(dLog, "S%d Term lower, invalid send entry reply. (%d < %d)", rf.me, reply.Term, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.votedTimer = time.Now()
					return
				}
				if reply.Success {
					Debug(dLog, "S%d<-S%d Log entries in sync at T%d.", rf.me, server, rf.currentTerm)
					rf.commitIndex = rf.lastIncludedIndex
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					for index := rf.getLastIndex(); index >= rf.lastIncludedIndex+1; index-- {
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
						if sum >= len(rf.peers)/2+1 && rf.getLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							Debug(dCommit, "S%d Updated commitIndex at T%d for majority consensus. : %d.", rf.me, rf.currentTerm, rf.commitIndex)
							break
						}
					}

				} else {
					if reply.XIndex != -1 {
						rf.nextIndex[server] = reply.XIndex
					}
				}
			}

		}(index)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 处理心跳请求、同步日志RP
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dLog2, "S%d<-S%d Received appendEntries request", rf.me, args.LeaderId)
	// 返回假 如果领导人的任期小于接收者的当前任期（5.1 节）
	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Leader's term is less than the current term, %d<%d", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XIndex = -1
		return
	}
	reply.Success = true
	reply.Term = args.Term
	reply.XIndex = -1

	rf.status = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	// 返回假 如果接收者日志中没有包含这样一个条目
	// 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上

	// 当前节点的快照已经包含了发送过来的日志
	if rf.lastIncludedIndex > args.PrevLogIndex {
		Debug(dLog2, "S%d follower snapshot includes the logs sent over, lastIncludedIndex=%d, PrevLogIndex=%d",
			rf.me,
			rf.lastIncludedIndex,
			args.PrevLogIndex,
		)
		reply.Success = false
		reply.XIndex = rf.getLastIndex() + 1
		return
	}
	// 当前节点最大的日志比发送过来的日志小 -> 当前节点日志缺失
	if rf.getLastIndex() < args.PrevLogIndex {
		Debug(dLog2, "S%d follower missing logs", rf.me)
		reply.Success = false
		reply.XIndex = rf.getLastIndex()
		return
	} else {
		// prevLogIndex处的任期与args.PrevLogTerm不匹配 -> 发生冲突
		argIndexTerm := rf.getLogTerm(args.PrevLogIndex)
		if argIndexTerm != args.PrevLogTerm {
			reply.Success = false
			for i := args.PrevLogIndex; i >= rf.lastIncludedIndex; i-- {
				// 也就是往后跳过任期为 argIndexTerm 的所有日志
				if rf.getLogTerm(i) != argIndexTerm {
					reply.XIndex = i + 1
					break
				}
			}
			Debug(dDrop, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
			return
		}
	}
	// 如果一个已经存在的条目和新条目发生了冲突（因为索引相同，任期不同），
	// 那么就删除这个已经存在的条目以及它之后的所有条目
	Debug(dLog2, "S%d follower append log", rf.me)
	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.Entries...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
}
