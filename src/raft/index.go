package raft

import ()

// 日志条目
type LogEntry struct {
	Term    int         // 领导人接收到该条目时的任期（初始索引为1）
	Command interface{} // 用于状态机的命令（空接口->可以存储任何类型的数据）
}

// 通过快照偏移还原真实日志条目
func (rf *Raft) getLogEntry(curIndex int) LogEntry {
	return rf.logs[curIndex-rf.lastIncludedIndex]
}

// 通过快照偏移还原真实日志任期
func (rf *Raft) getLogTerm(curIndex int) int {
	// 如果当前index与快照一致/日志为空，直接返回快照/快照初始化信息，否则根据快照计算
	if curIndex-rf.lastIncludedIndex == 0 {
		return rf.lastIncludedTerm
	}
	//fmt.Printf("[GET] curIndex:%v,rf.lastIncludedIndex:%v\n", curIndex, rf.lastIncludedIndex)
	return rf.logs[curIndex-rf.lastIncludedIndex].Term
}

// 获取最后的快照日志下标(代表已存储）
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludedIndex
}

// 获取最后的任期(快照版本
func (rf *Raft) getLastTerm() int {
	// 因为初始有填充一个，否则最直接len == 0
	if len(rf.logs)-1 == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

// 通过快照偏移还原真实PrevLogInfo
func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.nextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.getLogTerm(newEntryBeginIndex)
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}
