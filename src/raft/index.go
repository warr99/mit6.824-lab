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