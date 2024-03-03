package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient logTopic = "CLNT"
	dCommit logTopic = "CMIT"
	dDrop   logTopic = "DROP"
	dError  logTopic = "ERRO"
	dInfo   logTopic = "INFO"
	dLog    logTopic = "LOG1"
	dLog2   logTopic = "LOG2"
	dTest   logTopic = "TEST"
	dTrace  logTopic = "TRCE"
	dWarn   logTopic = "WARN"
)

// Debugging
var debug int = 1
var DDebug bool = true
var TDebug bool = false
var CDebug bool = true
var All_Log bool = false

var debugStart time.Time
var debugVerbosity int

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

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugPrintf(topic logTopic, format string, a ...interface{}) {
	if debug >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
