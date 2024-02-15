package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerState int

const (
	Leader ServerState = iota
	Candidate
	Follower
)

func (s ServerState) String() string {
	switch s {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return "Unknown"
	}
}

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
)

type LogType struct {
	Command interface{}
	Term    int
}

type PersistType struct {
	CurrentTerm int
	VoteFor     int
	Logs        []LogType
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

type DebugMsg struct {
	apply ApplyMsg
	msg   string
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// leader election
	cnt               int         // number of servers
	state             ServerState // state of server
	currentTerm       int         // server's current term, in each RPC, it should be updated
	voteFor           int         // server id of this follower vote for
	electionTimestamp time.Time   // timestamp for leader election

	// log replicated
	logs           []LogType
	lastApplied    int
	commitIndex    int
	commitCond     *sync.Cond
	nextIndex      map[int]int
	matchIndex     map[int]int
	tester         chan ApplyMsg
	sendTrigger    chan struct{}
	batchTimestamp time.Time

	// persist
	logDebuger chan DebugMsg

	// log compaction
	snapshot         []byte
	lastIncludeIndex int
	lastIncludeTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	// return term, isleader
	rf.mu.Lock()
	x := rf.currentTerm
	y := rf.state
	rf.mu.Unlock()
	return x, y == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	DPrintf(rf.me, "persist state")
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	var persist_storage PersistType
	persist_storage.CurrentTerm = rf.currentTerm
	persist_storage.VoteFor = rf.voteFor
	persist_storage.Logs = append(persist_storage.Logs, rf.logs[1:]...)

	encoder.Encode(&persist_storage)
	raft_state := buf.Bytes()
	rf.persister.Save(raft_state, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	DPrintf(rf.me, "recover from crash")
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	var content PersistType
	if decoder.Decode(&content) == nil {
		rf.currentTerm = content.CurrentTerm
		rf.voteFor = content.VoteFor
		rf.logs = append(rf.logs, content.Logs...)
	} else {
		DPrintf(rf.me, "restore from persist error")
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int // candidate term
	CandidateId   int // candidate id
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Reply_Term int  // server's term
	Vote_Grant bool // whether vote or not, true is vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(rf.me, "[RV] [args]: %+v, [term, vote for]: (%v, %v), [logs]: %v", args, rf.currentTerm, rf.voteFor, rf.logs)
	if rf.currentTerm < args.CandidateTerm {
		rf.convertFollower(args.CandidateTerm)
		rf.persist()
	}
	// last_index should be len(rf.logs) - 1 instead of lastApplied
	// suppose a scenario, a disconnected leader consistently receives request, its own log will grow simultaneously
	// but the term in its log would not change!! the object of our comparision is term!!
	last_index := len(rf.logs) - 1
	last_term := rf.logs[last_index].Term
	// if split vote occurs, followers hove no way to update its voteFor
	// in case of this scenarios, follower eithor vote original candidate or wait for election timeout
	if args.CandidateTerm == rf.currentTerm && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(args.LastLogTerm > last_term || (args.LastLogTerm == last_term && args.LastLogIndex >= last_index)) {
		rf.voteFor, rf.electionTimestamp = args.CandidateId, time.Now()
		reply.Vote_Grant = true
	} else {
		reply.Vote_Grant = false
	}
	reply.Reply_Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	LeaderTerm   int       // leader's term
	LeaderId     int       // leader id
	Entries      []LogType // log struct, index 0 is the previous log entry, the remaining is new log entries
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	ReplyTerm int  // server's term, used for leader to update its own term(but it will revert to follower if condition satified)
	Success   bool // indicate success of current RPC(true is success)
	// to mark whether current leader is outdated
	// the leader is outdated: term is lower or log is outdated
	XTerm  int // for fast recovery
	XIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(rf.me, "[AE] [args]: %+v, [term]: %v", args, rf.currentTerm)
	DPrintf(rf.me, "number of go routine: %v", runtime.NumGoroutine())
	// if leader own higher term, no matter what state the server is, should convert to follower
	if rf.currentTerm < args.LeaderTerm {
		rf.convertFollower(args.LeaderTerm)
		rf.persist()
	}
	reply.Success = false
	if rf.currentTerm == args.LeaderTerm {
		// state is candidate but term is the same as leader
		if rf.state != Follower {
			rf.convertFollower(args.LeaderTerm)
			rf.persist()
		}
		rf.electionTimestamp = time.Now()

		if args.PrevLogIndex >= len(rf.logs) {
			reply.XTerm, reply.XIndex, reply.ReplyTerm = -1, len(rf.logs), rf.currentTerm
			return
		}

		if (args.PrevLogIndex == 0 || args.PrevLogIndex < len(rf.logs)) && (rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.XTerm, reply.XIndex, reply.ReplyTerm = rf.logs[args.PrevLogIndex].Term, -1, rf.currentTerm
			for i := 1; i < len(rf.logs); i++ {
				if rf.logs[i].Term == rf.logs[args.PrevLogIndex].Term {
					reply.XIndex = i
					break
				}
			}
			return
		}

		conflict_index, append_index := args.PrevLogIndex+1, 0
		for {
			if conflict_index == len(rf.logs) || append_index == len(args.Entries) {
				break
			}
			if rf.logs[conflict_index].Term != args.Entries[append_index].Term && rf.logs[conflict_index].Command != args.Entries[append_index].Command {
				break
			}
			conflict_index++
			append_index++
		}
		// conflict_index either is the end of logs or is the index where log entry is not matched with leader
		// append_index either is the end of args.Entries or is the index where following entries will be appended into follower
		DPrintf(rf.me, "[AE] [before log]: %v, [args]: %+v", rf.logs, args)
		rf.logs = append(rf.logs[:conflict_index], args.Entries[append_index:]...)
		DPrintf(rf.me, "[AE] [after log]: %v", rf.logs)

		rf.persist()

		if rf.commitIndex < args.LeaderCommit {
			// we can simply set rf.commitIndex = args.LeaderCommit instead of min(args.LeaderCommit, len(rf.logs) - 1)
			// the reason is leader always send logs from nextIndex to THE END to follower, and LeaderCommit must lower than and equal to leader's log len
			// after follower replicates args.Entries, follower can own all logs from leader, so args.LeaderCommit must lower than and equal to len(rf.logs) - 1
			// rf.commitIndex = int(math.Min(float64(len(rf.logs)), float64(args.LeaderCommit)))
			rf.commitIndex = args.LeaderCommit
			rf.commitCond.Signal()
		}
		reply.Success = true
	}
	reply.ReplyTerm = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	term, isLeader := rf.GetState()
	if !isLeader {
		return 0, term, false
	}
	DPrintf(rf.me, "[Start] state: %v", rf.state.String())

	rf.mu.Lock()
	rf.logs = append(rf.logs, LogType{Command: command, Term: term})
	rf.persist()
	index := len(rf.logs) - 1
	DPrintf(rf.me, "[Start] append log: %+v", rf.logs[len(rf.logs)-1])
	save_batch_timestamp := rf.batchTimestamp
	rf.mu.Unlock()

	// once leader receives a new log entry, send AE to followers
	// we can't hold lock when sending message to channel, dead lock will occur!!
	if time.Since(save_batch_timestamp) > time.Millisecond*time.Duration(50) {
		rf.mu.Lock()
		rf.batchTimestamp = time.Now()
		rf.mu.Unlock()
		rf.sendTrigger <- struct{}{}
	}
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	election_timeout := 150 + (rand.Int31() % 200)
	start_term := rf.currentTerm
	for !rf.killed() {

		// Your code here (2A)

		rf.mu.Lock()
		// ticker only allow follower or candidate to run
		if rf.state != Follower && rf.state != Candidate {
			rf.mu.Unlock()
			return
		}

		if start_term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		if time.Since(rf.electionTimestamp) > time.Millisecond*time.Duration(election_timeout) {
			DPrintf(rf.me, "[ticker] start election in term: %v", rf.currentTerm)
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.currentTerm++
	rf.electionTimestamp = time.Now()

	// during the execution of this function, the term of current server may be changed (multiple instance executed in background)
	// if one instance change this server's state, its term will grow at the same time.
	// we should use the initial term when starts this function to compare with reply term instead of current term
	start_term := rf.currentTerm
	vote := int32(1)
	for i := 0; i < rf.cnt; i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			rf.mu.Lock()
			last_index := len(rf.logs) - 1
			last_term := rf.logs[last_index].Term
			rf.mu.Unlock()
			args := RequestVoteArgs{
				CandidateTerm: start_term,
				CandidateId:   rf.me,
				LastLogIndex:  last_index,
				LastLogTerm:   last_term}

			reply := RequestVoteReply{}
			// the reason for timeout is discussed below
			save_time := time.Now()
			if ok := rf.sendRequestVote(idx, &args, &reply); ok && time.Since(save_time) <= time.Millisecond*time.Duration(100) {
				rf.mu.Lock()
				// state may have changed by other startElection instance
				// if we omit this judgement, the term may be decreased (start_term is 2, rf.currentTerm is 5 and reply.Reply_Term is 4)
				if rf.state != Candidate {
					rf.mu.Unlock()
					return
				}
				if reply.Reply_Term > start_term {
					rf.convertFollower(reply.Reply_Term)
					rf.persist()
					rf.mu.Unlock()
					return
				} else if reply.Reply_Term == start_term && reply.Vote_Grant {
					// only reply term match with start term, the following code can be executed
					// because if current candidate becomes leader, all follower should have the same term
					atomic.AddInt32(&vote, 1)
					if val := atomic.LoadInt32(&vote); int(val) >= (rf.cnt+1)/2 {
						DPrintf(rf.me, "[startElection] become leader with votes %v", val)
						rf.startLeader()
						rf.mu.Unlock()
						return
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
	// if election failed, we should restart ticker to trigger new election
	go rf.ticker()
}

func (rf *Raft) convertFollower(new_term int) {
	rf.state = Follower
	rf.currentTerm = new_term
	rf.voteFor = -1
	rf.electionTimestamp = time.Now()
	go rf.ticker()
}

func (rf *Raft) startLeader() {
	rf.state = Leader
	go func() {
		rf.heartbeatMessage()
		heartbeat_timeout := 100
		// timer represents one of two events
		// - wait for 50ms
		// - message from sendTrigger channel
		send_timer := time.NewTimer(time.Millisecond * time.Duration(heartbeat_timeout))
		defer send_timer.Stop()
		for !rf.killed() {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			// we would wait some time, so before we send AE, we should check current server still is leader
			do_send := false
			select {
			case <-send_timer.C:
				send_timer.Stop()
				send_timer.Reset(time.Millisecond * time.Duration(heartbeat_timeout))
				do_send = true
			case _, ok := <-rf.sendTrigger:
				if !ok {
					return
				}
				if !send_timer.Stop() {
					<-send_timer.C
				}
				send_timer.Reset(time.Millisecond * time.Duration(heartbeat_timeout))
				do_send = true
			}
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if do_send {
				rf.heartbeatMessage()
			}
		}
	}()

}

func (rf *Raft) heartbeatMessage() {
	DPrintf(rf.me, "send AE to followers")
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	start_term := rf.currentTerm
	rf.mu.Unlock()
	for i := 0; i < rf.cnt; i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			rf.mu.Lock()
			nxt_idx := rf.nextIndex[idx]
			entries := rf.logs[nxt_idx:]
			save_commit_index := rf.commitIndex
			prev_index := nxt_idx - 1
			prev_term := rf.logs[prev_index].Term
			rf.mu.Unlock()
			args := AppendEntriesArgs{
				LeaderTerm:   start_term,
				LeaderId:     rf.me,
				Entries:      entries,
				PrevLogIndex: prev_index,
				PrevLogTerm:  prev_term,
				LeaderCommit: save_commit_index,
			}
			reply := AppendEntriesReply{}
			// because of network partition, rf.sendAppendEntries may not return for a long time
			// in the case of scenario, go routine would never return, and leader would consistently create new go routine to send AE
			// we stipulate thsi timeout cannot be over heartbeat timeout
			save_time := time.Now()
			if ok := rf.sendAppendEntries(idx, &args, &reply); ok && time.Since(save_time) <= time.Millisecond*time.Duration(100) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if reply.ReplyTerm > start_term {
					rf.convertFollower(reply.ReplyTerm)
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if reply.ReplyTerm == start_term {
					if reply.Success {
						rf.nextIndex[idx] = nxt_idx + len(entries)
						rf.matchIndex[idx] = rf.nextIndex[idx] - 1
						for i := rf.commitIndex + 1; i < len(rf.logs); i++ {
							replica_cnt := 1
							for _, val := range rf.matchIndex {
								if val >= i {
									replica_cnt++
								}
								if replica_cnt >= (rf.cnt+1)/2 {
									rf.commitIndex = i
								}
							}
						}
						if rf.commitIndex > save_commit_index {
							rf.commitCond.Signal()
							// once leader commit this log entry, we send AE to followers
							// we can't hold lock when sending message to channel, dead lock will occur!!
							rf.mu.Unlock()
							rf.sendTrigger <- struct{}{}
							rf.mu.Lock()
						}
					} else {
						// not optimized code
						// rf.nextIndex[idx]--
						// XTerm is the term of conflict log, XIndex is the index of conflict log
						//
						// 		|<--       XTerm       -->|
						// 		^
						// 		XIndex is the start index of log with XTerm
						//
						// if follower don't have XTerm, representing that follower's log is too short, leader should reduce its nextIndex in a unit of term
						// if leader don't have XTerm, representing that the follower's log will be overwrite, leader should reduce its nextIndex in a unit of term
						// if leader have XTerm, leader should find the first index of XTerm, representing that the follower have correct log term but log entry is incorrect
						// in the case of this scenario, leader should send all logs with XTerm to follower
						DPrintf(rf.me, "[XTerm, XIndex]: (%v, %v)", reply.XTerm, reply.XIndex)
						if reply.XTerm != -1 {
							nxt_idx = -1
							for i := 1; i < len(rf.logs); i++ {
								if rf.logs[i].Term == reply.XTerm {
									nxt_idx = i
									break
								}
								nxt_idx++
							}
							if nxt_idx != -1 {
								rf.nextIndex[idx] = nxt_idx
							} else {
								rf.nextIndex[idx] = reply.XIndex
							}
						} else {
							rf.nextIndex[idx] = reply.XIndex
						}
						DPrintf(rf.me, "nextIndex[%v]: %v", idx, rf.nextIndex[idx])
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.lastApplied < rf.commitIndex) {
			rf.commitCond.Wait()
		}
		DPrintf(rf.me, "[apply log] [lastApplied]: %v, [commitIndex]: %v", rf.lastApplied, rf.commitIndex)
		for i := rf.lastApplied + 1; i <= rf.commitIndex && i < len(rf.logs); i++ {
			rf.tester <- ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
			// the increase in lastApplied should simultaneously be acted
			rf.lastApplied++
			DPrintf(rf.me, "[apply log] [log]: %+v has applied, [apply idx]: %v", rf.logs[i], i)
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	// Your initialization code here (2A, 2B, 2C).

	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		cnt:               len(peers),
		currentTerm:       0,
		state:             Follower,
		electionTimestamp: time.Now(),
		voteFor:           -1,
		logs:              make([]LogType, 1),
		lastApplied:       0,
		commitIndex:       0,
		nextIndex:         make(map[int]int),
		matchIndex:        make(map[int]int),
		tester:            applyCh,
		sendTrigger:       make(chan struct{}, 64),
		batchTimestamp:    time.Now(),
		logDebuger:        make(chan DebugMsg, 64),
	}

	for i := 0; i < rf.cnt; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()
	// go rf.WriteLog()

	return rf
}
