package raft

import (
	"log"
	"math"
)

// 日志条目
type LogEntry struct {
	Term    int         // 领导人接收到该条目时的任期（初始索引为1）
	Command interface{} // 用于状态机的命令（空接口->可以存储任何类型的数据）
}

func (rf *Raft) lastLogInfo() (index, term int) {
	logEntries := rf.logs
	index = len(logEntries) + rf.lastIncludedIndex
	logEntry := rf.getEntry(index)
	return index, logEntry.Term
}

func (rf *Raft) getEntry(index int) *LogEntry {
	logEntries := rf.logs
	logIndex := index - rf.lastIncludedIndex
	if logIndex < 0 {
		Debug(dError, "S%d LogEntries.getEntry: index too small. (%d < %d)", rf.me, index, rf.lastIncludedIndex)
		log.Panicf("LogEntries.getEntry: index too small. (%d < %d)", index, rf.lastIncludedIndex)
	}
	if logIndex == 0 {
		return &LogEntry{
			Command: nil,
			Term:    rf.lastIncludedTerm,
		}
	}
	if logIndex > len(logEntries) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logEntries[logIndex-1]
}

func (rf *Raft) getSlice(startIndex, endIndex int) []LogEntry {
	logEntries := rf.logs
	logStartIndex := startIndex - rf.lastIncludedIndex
	logEndIndex := endIndex - rf.lastIncludedIndex
	if logStartIndex <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			startIndex, len(logEntries))
		log.Panicf("LogEntries.getSlice: startIndex out of range. (%d < %d)", startIndex, rf.lastIncludedIndex)
	}
	if logEndIndex > len(logEntries)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			endIndex, len(logEntries))
		log.Panicf("LogEntries.getSlice: endIndex out of range. (%d > %d)", endIndex, len(logEntries)+1+rf.lastIncludedIndex)
	}
	if logStartIndex > logEndIndex {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
		log.Panicf("LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
	}
	return append([]LogEntry(nil), logEntries[logStartIndex-1:logEndIndex-1]...)
}

// Get the index of first entry and last entry with the given term.
// Return (-1,-1) if no such term is found
func (rf *Raft) getBoundsWithTerm(term int) (minIndex int, maxIndex int) {
	if term == 0 {
		return 0, 0
	}
	logEntries := rf.logs
	minIndex, maxIndex = math.MaxInt, -1
	for i := rf.lastIncludedIndex + 1; i <= rf.lastIncludedIndex+len(logEntries); i++ {
		if rf.getEntry(i).Term == term {
			minIndex = int(math.Min(float64(minIndex), float64(i)))
			maxIndex = int(math.Max(float64(maxIndex), float64(i)))
		}
	}
	if maxIndex == -1 {
		return -1, -1
	}
	return
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
