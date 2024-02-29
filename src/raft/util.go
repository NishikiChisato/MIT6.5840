package raft

import (
	"fmt"
	"log"
	"os"
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
var debug int = 0
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

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func DPrintf(id int, format string, a ...interface{}) (n int, err error) {
	if DDebug {
		format = fmt.Sprintf("[server]: %v, ", id) + format + "\n"
		log.Printf(format, a...)
		os.Stdout.Sync()
	}
	return
}

func TPrintf(format string, a ...interface{}) (n int, err error) {
	if TDebug {
		log.Printf(colorYellow+format+colorReset, a...)
		os.Stdout.Sync()
	}
	return
}

func CPrintf(format string, a ...interface{}) (n int, err error) {
	if CDebug {
		log.Printf(colorGreen+format+colorReset, a...)
		os.Stdout.Sync()
	}
	return
}

func PrintAllLogsFromCFG(cfg *config) {
	for i, v := range cfg.rafts {
		filename := fmt.Sprintf("S%v.log", i)
		file, err := os.Create(filename)
		if err != nil {
			panic("file cannot create")
		}
		logs := v.getAllLog()
		file.WriteString(logs)
	}
}
