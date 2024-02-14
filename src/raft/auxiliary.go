package raft

import "fmt"

// auxiliary function

func (rf *Raft) String() string {
	var ret string
	ret += fmt.Sprintf("[server]: %v, [term]: %v, [vote for]: %v, [dead]: %v\n", rf.me, rf.current_term, rf.vote_for, rf.dead)
	return ret
}
