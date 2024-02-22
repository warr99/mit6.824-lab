package raft

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Debugging
const isDebug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if isDebug {
		log.Printf(format, a...)
	}
	return
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func beforeLock(id int64, funName string, rf int) {
	fmt.Printf("[%d] [%v] S%d Try to get lock\n", id, funName, rf)
}

func afterLock(id int64, funName string, startGetLockTime time.Time, rf int) {
	fmt.Printf("[%d] [%v] S%d Time to acquire lock: %v\n", id, funName, rf, time.Since(startGetLockTime))
}

func afterUnlock(id int64, funName string, startHoldLockTime time.Time, rf int) {
	fmt.Printf("[%d] [%v] S%d Time to hold lock: %v\n", id, funName, rf, time.Since(startHoldLockTime))
}

// 获取goroutine的ID
func getGID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	id := parseGID(buf[:n])
	return id
}

// 解析goroutine的ID
func parseGID(stack []byte) int64 {
	const prefix = "goroutine "
	stack = stack[len(prefix):]
	for i, b := range stack {
		if b < '0' || b > '9' {
			stack = stack[:i]
			break
		}
	}
	id, _ := strconv.ParseInt(string(stack), 10, 64)
	return id
}
