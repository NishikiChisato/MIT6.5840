package raft

import (
	"fmt"
	"log"
	"os"
)

// Debugging
var Debug bool = false
var TDebug bool = false
var EDebug bool = true
var All_Log bool = false

func DPrintf(id int, format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug {
		format = fmt.Sprintf("[server]: %v, ", id) + format + "\n"
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

func EPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if EDebug {
		log.Printf(colorGreen+format+colorReset, a...)
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
