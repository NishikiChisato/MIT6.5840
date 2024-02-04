package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
		os.Stdout.Sync()
	}
	return
}
