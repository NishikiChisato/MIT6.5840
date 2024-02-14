package raft

import "fmt"

// auxiliary function

func (rf *Raft) String() string {
	var ret string
	ret += fmt.Sprintf("[server]: %v, [term]: %v, [vote for]: %v, [dead]: %v\n", rf.me, rf.currentTerm, rf.voteFor, rf.dead)
	return ret
}
