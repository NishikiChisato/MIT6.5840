package raft

import (
	"log"
	"os"
)

// Debugging
var Debug bool = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
		os.Stdout.Sync()
	}
	return
}

var TDebug bool = false

func TPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if TDebug {
		log.Printf(colorYellow+format+colorReset, a...)
		os.Stdout.Sync()
	}
	return
}

func TRaftPrintAllLogs(cfg *config) {
	for _, v := range cfg.rafts {
		DPrintf("%v\n", v)
	}
}
