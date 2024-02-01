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
		if rf.vote_for == -1 && (args.Last_Log_Term > rf.current_term || (args.Last_Log_Term == rf.current_term && args.Last_Log_Index >= rf.current_index)) {
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
	Reply_term int  // server's term, used for leader to update its own term(but it will revert to follower if condition satified)
	Success    bool // indicate success of current RPC(true is success)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.isFollower() {
		if args.Leader_term < rf.current_term {
			rf.mu.Unlock()
			reply.Reply_term = rf.current_term
			reply.Success = false
			return
		}
		// reset election timeout
		rf.leader_election_timestamp = time.Now()
		if len(args.Entries) == 0 {
			// update server's term
			rf.current_term = args.Leader_term
			// reset server's vote_for
			rf.vote_for = -1
			rf.mu.Unlock()
			reply.Reply_term = rf.current_term
			reply.Success = true
			return
		}
		// if args.Prev_Log_Index > rf.current_index || (len(rf.logs) > 0 && args.Prev_Log_Term != rf.logs[args.Prev_Log_Index].Term) {
		// 	reply.Reply_term = rf.current_term
		// 	reply.Success = false
		// 	rf.mu.Unlock()
		// 	return
		// }
		// if rf.logs[args.Prev_Log_Index+1].Term != args.Entries[0].Term || rf.logs[args.Prev_Log_Index+1].Command != args.Entries[0].Command {
		// 	rf.leader_election_timestamp = time.Now()
		// 	tmp_log := []*LogType{}
		// 	for i := 0; i <= args.Prev_Log_Index; i++ {
		// 		tmp_log = append(tmp_log, rf.logs[i])
		// 	}
		// 	rf.logs = tmp_log
		// }
		// for i := 1; i < len(args.Entries); i++ {
		// 	rf.logs = append(rf.logs, args.Entries[i])
		// }
		// if args.Leader_committed_index > rf.committed_index {
		// 	rf.committed_index = min(args.Leader_committed_index, len(rf.logs)-1)
		// }
		rf.mu.Unlock()
		reply.Reply_term = rf.current_term
		reply.Success = true
	} else if rf.isCandidate() {
		if args.Leader_term >= rf.current_term {
			rf.leader_election_timestamp = time.Now()
			rf.current_term = args.Leader_term
			rf.state = Follower
			rf.vote_for = -1
		}
		rf.mu.Unlock()
		reply.Reply_term = rf.current_term
		reply.Success = true
	} else {
		// leader
		if args.Leader_term >= rf.current_term {
			rf.leader_election_timestamp = time.Now()
			rf.current_term = args.Leader_term
			rf.state = Follower
			rf.vote_for = -1
		}
		rf.mu.Unlock()
		reply.Reply_term = rf.current_term
		reply.Success = true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		// atomic variable, there is no race condition
		//fmt.Printf("[server id]: %v, [state]: %v, [term]: %v, [dead]: %v\n", rf.server_id, rf.state, rf.current_term, rf.dead)

		// Your code here (2A)
		// Check if a leader election should be started.

		// election timeout range from 400ms to 500ms
		election_timeout := 300 + (rand.Int31() % 200)
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
			rf_current_index := rf.current_index
			rf_current_term := rf.current_term
			rf_election_rounds := rf.election_rounds
			rf_committed_index := rf.committed_index

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
						Last_Log_Index:            rf_current_index,
						Last_Log_Term:             rf_current_term,
						Candidate_Election_Rounds: rf_election_rounds}
					rv_reply := RequestVoteReply{}
					for request_vote_state && !rf.sendRequestVote(idx, &rv_args, &rv_reply) {

					}
					// fmt.Printf("[candidate] [server id]: %v, [election_state]: %v, [request id]: %v, [reply_term]: %v, [grant]: %v\n", rf_server_id, request_vote_state, idx, rv_reply.Reply_Term, rv_reply.Vote_Grant)
					// fmt.Printf("[Debug]: %v\n", rv_reply.Debug_Info)
					if rv_reply.Reply_Term > rf_current_term {
						rf.mu.Lock()
						rf.current_term = rv_reply.Reply_Term
						rf.state = Follower
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

			// fmt.Printf("[candidate] [server id]: %v, [votes]: %v\n", rf_server_id, votes)
			if votes >= (rf.cnt+1)/2 {
				rf.mu.Lock()
				rf.state = Leader

				rf_server_id = rf.server_id
				rf_current_index = rf.current_index
				rf_current_term = rf.current_term
				rf_election_rounds = rf.election_rounds
				rf_committed_index = rf.committed_index
				rf.mu.Unlock()

				// fmt.Printf("[candidate] [server id]: %v becomes leader\n", rf_server_id)
				// candidate become leader should send empty AppendEntries RPC immediately
				for i := 0; i < rf.cnt; i++ {
					go func(i int) {
						if i == rf_server_id {
							return
						}
						ae_args := AppendEntriesArgs{Leader_term: rf.current_term,
							Leader_id:              rf_server_id,
							Prev_Log_Index:         rf_current_index,
							Prev_Log_Term:          rf_current_term,
							Leader_committed_index: rf_committed_index}
						ae_reply := AppendEntriesReply{}
						// only send once
						ok := rf.sendAppendEntries(i, &ae_args, &ae_reply)
						if !ok {
							//fmt.Printf("[candidate] candidate send failed: from [server id]: %v to [server id]: %v\n", rf.server_id, i)
							return
						}
					}(i)
				}
			}
			// if failed to become leader, retain candidate state
		} else if rf_state == Leader {
			rf.mu.Lock()
			if rf_state != rf.state {
				rf.mu.Unlock()
				continue
			}
			// fmt.Printf(colorYellow+"[leader] [server id]: %v, current leader\n"+colorReset, rf.server_id)

			heartbeat_timeout := time.Millisecond * time.Duration(100)
			rf.mu.Unlock()
			rf_heartbeat_timestamp := rf.heart_beat_timestamp

			// heartbeat messages
			if time.Since(rf_heartbeat_timestamp) >= heartbeat_timeout {
				// reset heartbeat timeout
				rf.heart_beat_timestamp = time.Now()

				for i := 0; i < rf.cnt; i++ {
					go func(idx int) {
						if idx == rf.server_id {
							return
						}
						ae_args := AppendEntriesArgs{Leader_term: rf.current_term,
							Leader_id:              rf.server_id,
							Prev_Log_Index:         rf.current_index,
							Prev_Log_Term:          rf.current_term,
							Leader_committed_index: rf.committed_index}
						ae_reply := AppendEntriesReply{}
						ok := rf.sendAppendEntries(idx, &ae_args, &ae_reply)
						// fmt.Printf(colorYellow+"[leader] [server id]: %v, [sendAE]: [to server id]: %v, [ok]: %v, [reply term]: %v, [reply success]: %v\n"+colorReset, rf.server_id, idx, ok, ae_reply.Reply_term, ae_reply.Success)
						if !ok {
							//fmt.Printf("[leader] candidate send failed: from [server id]: %v to [object id]: %v\n", rf.server_id, idx)
							return
						}
					}(i)
				}
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
	rf.current_index = 0
	rf.committed_index = 0
	rf.last_applied = 0
	rf.logs = append(rf.logs, &LogType{})
	rf.leader_election_timestamp = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
