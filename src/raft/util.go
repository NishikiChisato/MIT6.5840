package raft

import (
	"log"
	"os"
)

// Debugging
var Debug bool = false
var TDebug bool = false
var All_Log bool = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
		os.Stdout.Sync()
	}
	return
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if TDebug {
		log.Printf(colorYellow+format+colorReset, a...)
		os.Stdout.Sync()
	}
	return
}

func TRaftPrintAllLogs(cfg *config) {
	log.SetFlags(log.Lmicroseconds)
	for _, v := range cfg.rafts {
		if All_Log {
			log.Printf("%v\n", v)
			os.Stdout.Sync()
		}
	}
}
