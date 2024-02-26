package raft

import (
	"fmt"
	"os"
)

// auxiliary function

func (rf *Raft) String() string {
	var ret string
	ret += fmt.Sprintf("[server]: %v, [term]: %v, [vote for]: %v, [dead]: %v, [state]: %v\n", rf.me, rf.currentTerm, rf.voteFor, rf.dead, rf.state.String())
	ret += fmt.Sprintf("[lastApplied]: %v, [commitIndex]: %v\n", rf.lastApplied, rf.commitIndex)
	ret += fmt.Sprintf("[nextIndex]: %v, [matchIndex]: %v\n", rf.nextIndex, rf.matchIndex)
	for i, l := range rf.logs {
		ret += fmt.Sprintf("[%v]: %v\n", i, l)
	}
	return ret
}

func (rf *Raft) getAllLog() string {
	var ret string
	rf.mu.Lock()
	for _, log := range rf.logs {
		ret += fmt.Sprintf("%+v\n", log)
	}
	rf.mu.Unlock()
	return ret
}

func (rf *Raft) WriteLog() {
	filename := fmt.Sprintf("server_%v.log", rf.me)
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("create error")
	}
	defer file.Close()
	for log := range rf.logDebuger {
		str := fmt.Sprintf("[commit] [valid]: %v, [cmd]: %v, [idx]: %v, [msg]: %v\n", log.apply.CommandValid, log.apply.Command, log.apply.CommandIndex, log.msg)
		str += fmt.Sprintf("[snapshot] [valid]: %v, [idx]: %v, [term]: %v\n", log.apply.SnapshotValid, log.apply.SnapshotIndex, log.apply.SnapshotTerm)
		file.WriteString(str)
	}

}
