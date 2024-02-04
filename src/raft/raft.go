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

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	// peers[me] is the name for Raft
	// field 'peers' can be ignore, just a fuild used for RPC

	tester chan ApplyMsg

	// atomic variable
	cnt             int         // number of servers
	server_id       int         // identifier of server
	state           ServerState // state of server
	current_term    int         // server's current term, in each RPC, it should be updated
	current_index   int         // index of latest log entry
	vote_for        int         // server id of this follower vote for
	committed_index int         // index of last committed log entry
	last_applied    int         // index of last applied to state machine
	election_rounds int         // the rounds of elcetion, used by follower to distinct different election, ensuring each election only vote once

	logs []*LogType // store log, first index is 1

	// heartbeat 10 times per second (1s = 1000ms) -> every 100ms emit heartbeat
	// should elect a new leader in five seconds (5000ms) -> suppose 10 times of split vote -> election must finish in 500ms
	// election ranges from 300ms to 500ms
	leader_election_timestamp time.Time // timestamp for leader election
	heart_beat_timestamp      time.Time // used for leader, timestamp for heart beat

	next_index  map[int]int // used for leader, index of next log entry to send to follower(key is server id, val is index of next lof entry to be send in leader)
	match_index map[int]int // used for leader, index of log entry which is replicated in that server(key is server id, val is index of log entry in leader)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	// return term, isleader
	rf.mu.Lock()
	x := rf.current_term
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
	Candidate_term            int // candidate term
	Candidate_Id              int // candidate id
	Last_Log_Index            int // index of last log entry in candidate
	Last_Log_Term             int // term of log entry pointed by Last_Log_Index
	Candidate_Election_Rounds int // the rounds of candidate elcetion
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Reply_Term int  // server's term
	Vote_Grant bool // whether vote or not, true is vote
	Debug_Info string
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.isFollower() {
		if args.Candidate_term < rf.current_term {
			rf.mu.Unlock()
			reply.Reply_Term = rf.current_term
			reply.Vote_Grant = false
			reply.Debug_Info = "candidate's term is lower than server's"
			return
		}
		if args.Candidate_Election_Rounds == rf.election_rounds {
			reply.Reply_Term = rf.current_term
			reply.Vote_Grant = false
			//reply.Debug_Info = "current server has vote yet"
			reply.Debug_Info = fmt.Sprintf("current server has vote yet, [server rounds]: %v", rf.election_rounds)
			rf.mu.Unlock()
			return
		} else {
			rf.election_rounds = args.Candidate_Election_Rounds
			rf.vote_for = -1
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		// DPrintf(colorCyan+"[vote]: %v, [last term]: %v, [current term]: %v, [last index]: %v, [current index]: %v\n", rf.vote_for, args.Last_Log_Term, rf.logs[rf.current_index].Term, args.Last_Log_Index, rf.current_index)
		// we should use last log term and last log index to compare
		if rf.vote_for == -1 && (args.Last_Log_Term > rf.logs[rf.last_applied].Term || (args.Last_Log_Term == rf.logs[rf.last_applied].Term && args.Last_Log_Index >= rf.last_applied)) {
			rf.leader_election_timestamp = time.Now()
			rf.vote_for = args.Candidate_Id
			rf.current_term = args.Candidate_term
			rf.mu.Unlock()

			reply.Reply_Term = rf.current_term
			reply.Vote_Grant = true
			reply.Debug_Info = "vote success"
		} else {
			rf.current_term = args.Candidate_term
			rf.mu.Unlock()
			reply.Reply_Term = rf.current_term
			reply.Vote_Grant = false
			reply.Debug_Info = "candidate's log lags behind server's"
		}
	} else if rf.isCandidate() {
		if args.Candidate_term > rf.current_term {
			rf.leader_election_timestamp = time.Now()
			rf.current_term = args.Candidate_term
			rf.state = Follower
		}
		rf.mu.Unlock()
		reply.Reply_Term = rf.current_term
		reply.Vote_Grant = false
		reply.Debug_Info = "server is candidate, not vote"
	} else if rf.isLeader() {
		// leader
		if args.Candidate_term > rf.current_term {
			rf.leader_election_timestamp = time.Now()
			rf.current_term = args.Candidate_term
			rf.state = Follower
		}
		rf.mu.Unlock()
		reply.Reply_Term = rf.current_term
		reply.Vote_Grant = false
		reply.Debug_Info = "server is leader, not vote"
	}
}

type AppendEntriesArgs struct {
	Leader_term            int        // leader's term
	Leader_id              int        // leader id
	Prev_Log_Index         int        // index of log entry immediately preceding the new log entry
	Prev_Log_Term          int        // term of log entry pointed by Prev_Log_Index
	Entries                []*LogType // log struct, index 0 is the previous log entry, the remaining is new log entries
	Leader_committed_index int        // leader's committed index
}

type AppendEntriesReply struct {
	Reply_term        int         // server's term, used for leader to update its own term(but it will revert to follower if condition satified)
	Success           bool        // indicate success of current RPC(true is success)
	Server_Identifier ServerState // the identification of server
	Debug_Info        string
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.isFollower() {
		if args.Leader_term < rf.current_term {
			rf.mu.Unlock()
			reply.Reply_term = rf.current_term
			reply.Success = false
			reply.Server_Identifier = Follower
			reply.Debug_Info = "[server]: follower, leader's term is lower than follower's"
			return
		}

		if args.Leader_committed_index < rf.last_applied {
			rf.mu.Unlock()
			// we can not reset election_timeout
			reply.Reply_term = rf.current_term
			reply.Success = false
			reply.Server_Identifier = Follower
			reply.Debug_Info = "[server]: follower, leader inconsistency"
			return
		}

		// reset election timeout
		rf.leader_election_timestamp = time.Now()

		if len(args.Entries) == 0 {
			// update server's term
			rf.current_term = args.Leader_term
			// reset server's vote_for
			rf.vote_for = -1
			rf.committed_index = min(rf.current_index, args.Leader_committed_index)
			DPrintf(colorPurple+"[server id]: %v, [last applied]: %v, [committed index]: %v, [log len]: %v\n"+colorReset, rf.server_id, rf.last_applied, rf.committed_index, len(rf.logs))
			for i := rf.last_applied; i < rf.committed_index; i++ {
				rf.last_applied++
				rf.tester <- ApplyMsg{CommandValid: true, Command: rf.logs[rf.last_applied].Command, CommandIndex: rf.last_applied}
			}
			rf.mu.Unlock()
			reply.Reply_term = rf.current_term
			reply.Success = true
			reply.Server_Identifier = Follower
			reply.Debug_Info = "[server]: follower, log entry is empty, success"
			return
		}

		// if the term of log entry pointed by Prev_Log_Index in follower not matchs Prev_Log_Term
		if len(rf.logs) != 0 && (args.Prev_Log_Index < len(rf.logs) && args.Prev_Log_Index >= 0 && rf.logs[args.Prev_Log_Index].Term != args.Prev_Log_Term) {
			rf.mu.Unlock()
			reply.Reply_term = rf.current_term
			reply.Success = false
			reply.Server_Identifier = Follower
			reply.Debug_Info = fmt.Sprintf("[server]: follower, leader's log entry is not up-to-date, [logs len]: %v, [local prev term]: %v\n", len(rf.logs), rf.logs[args.Prev_Log_Index].Term)
			return
		}
		// replicate log entries in AppendEntries to follower
		// if inconsistency occurs, delete the following log entries at that point in follower
		// if control flow can reach here, it means that log entry pointed by Prev_Log_Index in rf.logs is matched with Prev_Log_Term
		// the start_index is assigned when inconsistency occurs
		// Deduplication is needed!!!!!
		for idx, log := range rf.logs {
			DPrintf(colorPurple+"[before] [server id]: %v, [idx]: %v, [log]: %v\n"+colorReset, rf.server_id, idx, *log)
		}
		start_index := 0
		for i, j := args.Prev_Log_Index+1, 0; i < len(rf.logs) && j < len(args.Entries); i++ {
			if rf.logs[i].Term != args.Entries[j].Term || rf.logs[i].Command != args.Entries[j].Command {
				// delete the following element
				rf.logs = rf.logs[:i]
				rf.current_index = len(rf.logs) - 1
				break
			} else {
				start_index++
			}
			j++
		}
		DPrintf(colorPurple+"[server id]: %v, [start index]: %v\n"+colorReset, rf.server_id, start_index)
		// append all log entries to rf.logs
		for i := start_index; i < len(args.Entries); i++ {
			rf.logs = append(rf.logs, args.Entries[i])
		}
		rf.current_index = len(rf.logs) - 1
		for idx, log := range rf.logs {
			DPrintf(colorPurple+"[after] [server id]: %v, [idx]: %v, [log]: %v\n"+colorReset, rf.server_id, idx, *log)
		}
		rf.current_term = args.Leader_term
		rf.committed_index = min(rf.current_index, args.Leader_committed_index)
		DPrintf(colorPurple+"[server id]: %v, [commit idx]: %v, [last applied]: %v, [log len]: %v, [leader commit idx]: %v\n"+colorReset, rf.server_id, rf.committed_index, rf.last_applied, len(rf.logs), args.Leader_committed_index)
		for i := rf.last_applied; i < rf.committed_index && i < len(rf.logs); i++ {
			rf.last_applied++
			rf.tester <- ApplyMsg{CommandValid: true, Command: rf.logs[rf.last_applied].Command, CommandIndex: rf.last_applied}
		}
		rf.leader_election_timestamp = time.Now()

		rf.mu.Unlock()
		reply.Reply_term = rf.current_term
		reply.Success = true
		reply.Server_Identifier = Follower
		reply.Debug_Info = fmt.Sprintf("[server]: follower: %v, append success, [current_idx]: %v, [log len]: %v\n", rf.server_id, rf.current_index, len(rf.logs))
	} else if rf.isCandidate() {
		rf.leader_election_timestamp = time.Now()
		rf.current_term = args.Leader_term
		rf.state = Follower
		rf.vote_for = -1
		rf.mu.Unlock()
		reply.Reply_term = rf.current_term
		reply.Success = false
		reply.Server_Identifier = Candidate
		reply.Debug_Info = fmt.Sprintf("[server]: candidate: %v, fault\n", rf.server_id)
	} else {
		// leader
		if args.Leader_term >= rf.current_term && args.Leader_committed_index >= rf.committed_index {
			rf.leader_election_timestamp = time.Now()
			rf.current_term = args.Leader_term
			rf.state = Follower
			rf.vote_for = -1
		}
		reply.Reply_term = rf.current_term
		rf.mu.Unlock()
		reply.Success = false
		reply.Server_Identifier = Leader
		reply.Debug_Info = fmt.Sprintf("[server]: leader: %v, fault\n", rf.server_id)
	}
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
	// current server is leader
	log_entry := LogType{Command: command, Term: term}
	rf.mu.Lock()
	rf.logs = append(rf.logs, &log_entry)
	rf.current_index++
	rf.mu.Unlock()

	DPrintf(colorBlue+"[start] [server id]: %v, [current_idx]: %v, [term]: %v, [is_leader]: %v\n"+colorReset, rf.server_id, rf.current_index, term, isLeader)

	return rf.current_index, term, isLeader
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

// auxiliary function
// leader is 0, candidate is 1, follower is 2
// use required with lock
func (rf *Raft) isLeader() bool {
	z := rf.state
	return z == Leader
}

func (rf *Raft) isCandidate() bool {
	z := rf.state
	return z == Candidate
}

func (rf *Raft) isFollower() bool {
	z := rf.state
	return z == Follower
}

func (rf *Raft) heartbeat_message_empty() {
	rf.mu.Lock()
	// reset heartbeat timestamp
	rf.heart_beat_timestamp = time.Now()
	rf_server_id := rf.server_id
	rf_current_term := rf.current_term
	rf_committed_index := rf.committed_index
	rf.mu.Unlock()
	for i := 0; i < rf.cnt; i++ {
		go func(idx int) {
			if idx == rf_server_id {
				return
			}
			var ae_args AppendEntriesArgs
			var ae_reply AppendEntriesReply
			ae_args = AppendEntriesArgs{Leader_term: rf_current_term, Leader_id: rf_server_id, Leader_committed_index: rf_committed_index}
			ae_reply = AppendEntriesReply{}
			ok := rf.sendAppendEntries(idx, &ae_args, &ae_reply)
			if !ok {
				return
			}
		}(i)
	}
}

func (rf *Raft) heartbeat_message_with_log() {
	rf.mu.Lock()
	rf_server_id := rf.server_id
	rf_current_term := rf.current_term
	rf_committed_index := rf.committed_index
	rf_logs := rf.logs
	rf.mu.Unlock()

	// heartbeat messages
	// reset heartbeat timeout
	rf.heart_beat_timestamp = time.Now()

	// a new log entry appended to leader

	rf.mu.Lock()
	for i := 0; i < rf.cnt; i++ {
		go func(idx int) {
			if idx == rf_server_id {
				return
			}
			var ae_args AppendEntriesArgs
			var ae_reply AppendEntriesReply
			// we can directly use rf_next_index[idx], not even with lock
			// we should duplicate it and use the copy
			rf.mu.Lock()
			nxt_idx := rf.next_index[idx]
			rf.mu.Unlock()
			if nxt_idx > len(rf_logs) {
				return
			}
			// fmt.Printf("[leader]: %v, [idx]: %v, [next idx]: %v\n", rf.server_id, idx, rf_next_index[idx])
			log_entries := rf_logs[nxt_idx:]
			replicated_len := len(log_entries)
			ae_args = AppendEntriesArgs{Leader_term: rf_current_term, Leader_id: rf_server_id,
				Prev_Log_Index: nxt_idx - 1, Prev_Log_Term: rf_logs[nxt_idx-1].Term,
				Entries: log_entries, Leader_committed_index: rf_committed_index}
			ae_reply = AppendEntriesReply{}
			ok := rf.sendAppendEntries(idx, &ae_args, &ae_reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			if ae_reply.Reply_term > rf.current_term {
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
			DPrintf(colorYellow+"[from]: %v, [to]: %v, [prev_index]: %v, [prev_term]: %v, [entries len]: %v\n"+colorReset, rf.server_id, idx, rf.next_index[idx]-1, rf.logs[rf.next_index[idx]-1].Term, len(log_entries))
			DPrintf(colorYellow+"[ok]: %v, [success]: %v, [debug]: %v\n"+colorReset, ok, ae_reply.Success, ae_reply.Debug_Info)
			DPrintf(colorYellow+"[next index]: %v, [match index]: %v\n"+colorReset, rf.next_index, rf.match_index)
			DPrintf(colorYellow+"[debug]: %v\n"+colorReset, ae_reply.Debug_Info)
			for idx, log := range rf.logs {
				DPrintf(colorCyan+"[leader]: %v, [idx]: %v, [log]: %v\n"+colorReset, rf.server_id, idx, log)
			}
			if ae_reply.Server_Identifier != Follower {
				rf.mu.Unlock()
				return
			}
			if !ae_reply.Success {
				if rf.next_index[idx] > 1 {
					rf.next_index[idx]--
				}
			} else {
				rf.next_index[idx] += replicated_len
				rf.match_index[idx] = rf.next_index[idx] - 1
			}
			rf.mu.Unlock()
		}(i)
	}
	rf.mu.Unlock()
}

func (rf *Raft) check_commit() {
	time.Sleep(time.Millisecond * time.Duration(50))
	rf.mu.Lock()
	cnt, try_committed_index := 1, rf.committed_index+1
	// try_committed_index must lower than len(rf.logs)
	if try_committed_index >= len(rf.logs) {
		rf.mu.Unlock()
		return
	}
	for _, val := range rf.next_index {
		if val-1 >= try_committed_index {
			cnt++
		}
	}
	DPrintf(colorYellow+"[cnt]: %v\n"+colorReset, cnt)
	if cnt >= (rf.cnt+1)/2 {
		rf.committed_index++
		DPrintf(colorYellow+"[leader]: %v, [committed_index]: %v\n"+colorReset, rf.server_id, rf.committed_index)
	}
	for i := rf.last_applied; i < rf.committed_index; i++ {
		rf.last_applied++
		rf.tester <- ApplyMsg{CommandValid: true, Command: rf.logs[rf.last_applied].Command, CommandIndex: rf.last_applied}
	}
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// atomic variable, there is no race condition
		//fmt.Printf("[server id]: %v, [state]: %v, [term]: %v, [dead]: %v\n", rf.server_id, rf.state, rf.current_term, rf.dead)

		// Your code here (2A)
		// Check if a leader election should be started.

		// election timeout range from 400ms to 500ms
		election_timeout := 300 + (rand.Int31() % 200)
		heartbeat_timeout := 100
		rf.mu.Lock()
		rf_state := rf.state
		rf.mu.Unlock()
		if rf_state == Follower {
			rf.mu.Lock()
			if rf_state != rf.state {
				rf.mu.Unlock()
				continue
			}

			rf.mu.Unlock()
			// when thread sleeps, cannot hold lock!!!
			time.Sleep(time.Millisecond * time.Duration(election_timeout))
			rf_election_timestamp := rf.leader_election_timestamp
			// fmt.Printf(colorBlue+"[follower] [server id]: %v, current follower\n"+colorReset, rf.server_id)
			rf.mu.Lock()
			if rf.vote_for == -1 && time.Since(rf_election_timestamp) >= time.Millisecond*time.Duration(election_timeout) {
				// initialize a new election
				rf.state = Candidate
			}
			rf.mu.Unlock()
		} else if rf_state == Candidate {
			rf.mu.Lock()
			if rf_state != rf.state {
				rf.mu.Unlock()
				continue
			}

			// fmt.Printf(colorPurple+"[candidate] [server id]: %v become candidate\n"+colorReset, rf.server_id)

			// initialize a new election, reset property
			rf.leader_election_timestamp = time.Now()
			rf.election_rounds++
			rf.current_term++
			rf.vote_for = rf.server_id

			// avoid concurrency
			rf_server_id := rf.server_id
			rf_current_term := rf.current_term
			rf_election_rounds := rf.election_rounds
			rf_last_applied := rf.last_applied
			rf_logs := rf.logs

			rf.mu.Unlock()
			votes := 1 // vote for itself
			request_vote_state := true
			var lk sync.Mutex

			rf.mu.Lock()
			for i := 0; i < rf.cnt; i++ {
				go func(idx int) {
					if idx == rf.server_id {
						return
					}
					rv_args := RequestVoteArgs{Candidate_term: rf.current_term,
						Candidate_Id:              rf_server_id,
						Last_Log_Index:            rf_last_applied,
						Last_Log_Term:             rf_logs[rf_last_applied].Term,
						Candidate_Election_Rounds: rf_election_rounds}
					rv_reply := RequestVoteReply{}
					for request_vote_state && !rf.sendRequestVote(idx, &rv_args, &rv_reply) {
						time.Sleep(time.Millisecond * time.Duration(10))
					}
					//DPrintf("[candidate] [server id]: %v, [election_state]: %v, [request id]: %v, [reply_term]: %v, [grant]: %v\n", rf_server_id, request_vote_state, idx, rv_reply.Reply_Term, rv_reply.Vote_Grant)
					DPrintf(colorCyan+"[Debug]: %v, [server id]: %v, [object id]: %v\n"+colorReset, rv_reply.Debug_Info, rf.server_id, idx)
					if rv_reply.Reply_Term > rf_current_term {
						rf.mu.Lock()
						rf.current_term = rv_reply.Reply_Term
						rf.state = Follower
						rf.vote_for = -1
						rf.leader_election_timestamp = time.Now()
						rf.mu.Unlock()
						return
					}
					if rv_reply.Vote_Grant {
						lk.Lock()
						votes++
						lk.Unlock()
					}
				}(i)
			}
			rf.mu.Unlock()

			time.Sleep(time.Millisecond * time.Duration(election_timeout))
			request_vote_state = false
			rf.mu.Lock()
			rf_current_state := rf.state
			rf.mu.Unlock()
			if rf_current_state == Follower {
				continue
			}

			// DPrintf("[candidate] [server id]: %v, [votes]: %v\n", rf_server_id, votes)
			if votes >= (rf.cnt+1)/2 {
				rf.mu.Lock()
				rf.state = Leader
				rf.mu.Unlock()
				// we cannot use gorountine
				rf.heartbeat_message_with_log()
			}
			// if failed to become leader, retain candidate state
		} else if rf_state == Leader {
			rf.mu.Lock()
			if rf_state != rf.state {
				rf.mu.Unlock()
				continue
			}

			// rf_committed_index := rf.committed_index
			// rf_logs := rf.logs
			rf_heartbeat_timestamp := rf.heart_beat_timestamp
			rf.mu.Unlock()

			if time.Since(rf_heartbeat_timestamp) >= time.Millisecond*time.Duration(heartbeat_timeout) {
				// heartbeat messages
				// reset heartbeat timeout
				rf.heart_beat_timestamp = time.Now()

				// a new log entry appended to leader
				// if rf_committed_index+1 < len(rf_logs) {
				// 	go rf.heartbeat_message_with_log()
				// } else {
				// 	go rf.heartbeat_message_empty()
				// }
				go rf.heartbeat_message_with_log()
				go rf.check_commit()
			}

		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.tester = applyCh
	rf.cnt = len(peers)
	rf.state = Follower
	rf.server_id = me
	rf.vote_for = -1 // -1 means there hasn't vote yet
	rf.current_term = 0
	rf.current_index = 0 // there are always one log entry in each server
	rf.committed_index = 0
	rf.last_applied = 0
	rf.leader_election_timestamp = time.Now()

	// every server initially has one log entry
	rf.logs = append(rf.logs, &LogType{Term: 0})

	rf.next_index = make(map[int]int, rf.cnt)
	rf.match_index = make(map[int]int, rf.cnt)
	for i := 0; i < rf.cnt; i++ {
		rf.next_index[i] = 1
		rf.match_index[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
