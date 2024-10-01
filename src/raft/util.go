package raft

import "log"

// Debugging
const Debug = false
const LEADER = 0
const FOLLOWER = 1
const CANDIDATE = 2

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
