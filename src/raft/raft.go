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

// auxiliary function
// leader is 0, candidate is 1, follower is 2
// use required with lock
func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	z := rf.state
	rf.mu.Unlock()
	return z == Leader
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	z := rf.state
	rf.mu.Unlock()
	return z == Candidate
}

func (rf *Raft) isFollower() bool {
	rf.mu.Lock()
	z := rf.state
	rf.mu.Unlock()
	return z == Follower
}

func (rf *Raft) getCnt() int {
	rf.mu.Lock()
	z := rf.cnt
	rf.mu.Unlock()
	return z
}

func (rf *Raft) getServerId() int {
	rf.mu.Lock()
	z := rf.server_id
	rf.mu.Unlock()
	return z
}

func (rf *Raft) setState(new_state ServerState) {
	rf.mu.Lock()
	rf.state = new_state
	rf.mu.Unlock()
}

func (rf *Raft) getCurrentIndex() int {
	rf.mu.Lock()
	z := rf.current_index
	rf.mu.Unlock()
	return z
}

func (rf *Raft) setCurrentIndex(idx int) {
	rf.mu.Lock()
	rf.current_index = idx
	rf.mu.Unlock()
}

func (rf *Raft) addCurrentIndex() {
	rf.mu.Lock()
	rf.current_index++
	rf.mu.Unlock()
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	z := rf.current_term
	rf.mu.Unlock()
	return z
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	rf.current_term = term
	rf.mu.Unlock()
}

func (rf *Raft) addCurrentTerm() {
	rf.mu.Lock()
	rf.current_term++
	rf.mu.Unlock()
}

func (rf *Raft) getCommittedIndex() int {
	rf.mu.Lock()
	z := rf.committed_index
	rf.mu.Unlock()
	return z
}

func (rf *Raft) setCommittedIndex(idx int) {
	rf.mu.Lock()
	rf.committed_index = idx
	rf.mu.Unlock()
}

func (rf *Raft) addCommittedIndex() {
	rf.mu.Lock()
	rf.committed_index++
	rf.mu.Unlock()
}

func (rf *Raft) getLastApplied() int {
	rf.mu.Lock()
	z := rf.last_applied
	rf.mu.Unlock()
	return z
}

func (rf *Raft) setLastApplied(idx int) {
	rf.mu.Lock()
	rf.last_applied = idx
	rf.mu.Unlock()
}

func (rf *Raft) addLastApplied() {
	rf.mu.Lock()
	rf.last_applied++
	rf.mu.Unlock()
}

func (rf *Raft) getElectionRounds() int {
	rf.mu.Lock()
	z := rf.election_rounds
	rf.mu.Unlock()
	return z
}

func (rf *Raft) addElectionRounds() {
	rf.mu.Lock()
	rf.election_rounds++
	rf.mu.Unlock()
}

func (rf *Raft) setElectionRounds(val int) {
	rf.mu.Lock()
	rf.election_rounds = val
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimeout() {
	rf.mu.Lock()
	rf.leader_election_timestamp = time.Now()
	rf.mu.Unlock()
}

func (rf *Raft) resetHeartbeatTimeout() {
	rf.mu.Lock()
	rf.heart_beat_timestamp = time.Now()
	rf.mu.Unlock()
}

func (rf *Raft) isVoteFor() bool {
	rf.mu.Lock()
	z := rf.vote_for
	rf.mu.Unlock()
	return z == -1
}

func (rf *Raft) setVoteFor(id int) {
	rf.mu.Lock()
	rf.vote_for = id
	rf.mu.Unlock()
}

func (rf *Raft) applyLogEntry(index int) {
	rf.mu.Lock()
	rf.tester <- ApplyMsg{CommandValid: true, Command: rf.logs[index].Command, CommandIndex: index}
	rf.mu.Unlock()
}

func (rf *Raft) getLogs() []*LogType {
	rf.mu.Lock()
	logs := rf.logs
	rf.mu.Unlock()
	return logs
}

func (rf *Raft) getLogLen() int {
	rf.mu.Lock()
	z := len(rf.logs)
	rf.mu.Unlock()
	return z
}

func (rf *Raft) getLogEntry(idx int) LogType {
	rf.mu.Lock()
	z := rf.logs[idx]
	rf.mu.Unlock()
	return *z
}

func (rf *Raft) appendLogEntry(val LogType) {
	rf.mu.Lock()
	rf.logs = append(rf.logs, &val)
	rf.mu.Unlock()
}

func (rf *Raft) setLogs(val []*LogType) {
	rf.mu.Lock()
	rf.logs = val
	rf.mu.Unlock()
}

func (rf *Raft) getNextIndex() map[int]int {
	rf.mu.Lock()
	z := rf.next_index
	rf.mu.Unlock()
	return z
}

func (rf *Raft) getMatchIndex() map[int]int {
	rf.mu.Lock()
	z := rf.match_index
	rf.mu.Unlock()
	return z
}

func (rf *Raft) addNextIndex(idx, val int) {
	rf.mu.Lock()
	rf.next_index[idx] += val
	rf.mu.Unlock()
}

func (rf *Raft) setNextIndex(idx, val int) {
	rf.mu.Lock()
	rf.next_index[idx] = val
	rf.mu.Unlock()
}

func (rf *Raft) addMatchIndex(idx, val int) {
	rf.mu.Lock()
	rf.match_index[idx] += val
	rf.mu.Unlock()
}

func (rf *Raft) setMatchIndex(idx, val int) {
	rf.mu.Lock()
	rf.match_index[idx] = val
	rf.mu.Unlock()
}

func (rf *Raft) getLeaderElectionTimestamp() time.Time {
	rf.mu.Lock()
	z := rf.leader_election_timestamp
	rf.mu.Unlock()
	return z
}

func (rf *Raft) getHeartbeatTimestamp() time.Time {
	rf.mu.Lock()
	z := rf.heart_beat_timestamp
	rf.mu.Unlock()
	return z
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

func (rf *Raft) RequestVoteCondition(args *RequestVoteArgs) bool {
	rf.mu.Lock()
	z := args.Last_Log_Term > rf.logs[rf.last_applied].Term || (args.Last_Log_Term == rf.logs[rf.last_applied].Term && args.Last_Log_Index >= rf.last_applied)
	rf.mu.Unlock()
	return z
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.isFollower() {
		if args.Candidate_term < rf.current_term {
			reply.Reply_Term = rf.getCurrentTerm()
			reply.Vote_Grant = false
			//reply.Debug_Info = "candidate's term is lower than server's"
			return
		}
		if args.Candidate_Election_Rounds == rf.getElectionRounds() {
			reply.Reply_Term = rf.getCurrentTerm()
			reply.Vote_Grant = false
			//reply.Debug_Info = "current server has vote yet"
			//reply.Debug_Info = fmt.Sprintf("current server has vote yet, [server rounds]: %v", rf.election_rounds)
			return
		} else {
			rf.setElectionRounds(args.Candidate_Election_Rounds)
			rf.setVoteFor(-1)
		}
		// DPrintf(colorCyan+"[vote]: %v, [last term]: %v, [current term]: %v, [last index]: %v, [current index]: %v\n", rf.vote_for, args.Last_Log_Term, rf.logs[rf.current_index].Term, args.Last_Log_Index, rf.current_index)
		// we should use last log term and last log index to compare
		if rf.isVoteFor() && rf.RequestVoteCondition(args) {
			rf.resetElectionTimeout()
			rf.setVoteFor(args.Candidate_Id)
			rf.setCurrentTerm(args.Candidate_term)

			reply.Reply_Term = rf.current_term
			reply.Vote_Grant = true
			// reply.Debug_Info = "vote success"
		} else {
			rf.setCurrentTerm(args.Candidate_term)

			reply.Reply_Term = rf.getCurrentTerm()
			reply.Vote_Grant = false
			//reply.Debug_Info = "candidate's log lags behind server's"
		}
	} else if rf.isCandidate() {
		if args.Candidate_term > rf.getCurrentTerm() {
			rf.resetElectionTimeout()
			rf.setCurrentTerm(args.Candidate_term)
			rf.setState(Follower)
		}
		reply.Reply_Term = rf.getCurrentTerm()
		reply.Vote_Grant = false
		//reply.Debug_Info = "server is candidate, not vote"
	} else if rf.isLeader() {
		// leader
		if args.Candidate_term > rf.getCurrentTerm() {
			rf.resetElectionTimeout()
			rf.setCurrentTerm(args.Candidate_term)
			rf.setState(Follower)
		}
		reply.Reply_Term = rf.getCurrentTerm()
		reply.Vote_Grant = false
		//reply.Debug_Info = "server is leader, not vote"
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
	Reply_term     int  // server's term, used for leader to update its own term(but it will revert to follower if condition satified)
	Success        bool // indicate success of current RPC(true is success)
	Identification ServerState
	Xterm          int // for fast recovery
	XIndex         int
	Debug_Info     string
}

func (rf *Raft) AppendEntriesCondition(args *AppendEntriesArgs) bool {
	rf.mu.Lock()
	z := (args.Prev_Log_Index < len(rf.logs) && args.Prev_Log_Index >= 0 && rf.logs[args.Prev_Log_Index].Term != args.Prev_Log_Term)
	rf.mu.Unlock()
	return z
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.isFollower() {
		if args.Leader_term < rf.getCurrentTerm() {
			reply.Reply_term = rf.getCurrentTerm()
			reply.Success = false
			reply.Xterm = -1
			reply.XIndex = rf.getLogLen()
			reply.Identification = Follower
			//reply.Debug_Info = "[server]: follower, leader's term is lower than follower's"
			return
		}

		if args.Leader_committed_index < rf.getLastApplied() {
			// we can not reset election_timeout
			reply.Reply_term = rf.getCurrentTerm()
			reply.Success = false
			reply.Xterm = -1
			reply.XIndex = rf.getLogLen()
			reply.Identification = Follower
			//reply.Debug_Info = "[server]: follower, leader inconsistency"
			return
		}

		// reset election timeout
		rf.resetElectionTimeout()

		if len(args.Entries) == 0 {
			// update server's term
			rf.setCurrentTerm(args.Leader_term)
			// reset server's vote_for
			rf.setVoteFor(-1)
			rf.setCommittedIndex(min(rf.getCurrentIndex(), args.Leader_committed_index))
			for i := rf.getLastApplied(); i < rf.getCommittedIndex(); i++ {
				rf.addLastApplied()
				rf.applyLogEntry(rf.getLastApplied())
			}

			reply.Reply_term = rf.getCurrentTerm()
			reply.Success = true
			reply.Identification = Follower
			//reply.Debug_Info = "[server]: follower, log entry is empty, success"
			return
		}

		// follower don't have log entry in Prev_Log_Index
		if args.Prev_Log_Index >= rf.getLogLen() {
			reply.Reply_term = rf.getCurrentTerm()
			reply.Xterm = -1
			reply.XIndex = rf.getLogLen()
			reply.Success = false
			reply.Identification = Follower
			return
		}

		// if the term of log entry pointed by Prev_Log_Index in follower not matchs Prev_Log_Term
		if len(rf.getLogs()) != 0 && rf.AppendEntriesCondition(args) {
			XTerm, XIndex := rf.getLogEntry(args.Prev_Log_Index).Term, -1
			for i := 1; i < rf.getLogLen(); i++ {
				if log := rf.getLogEntry(i); log.Term == XTerm {
					XIndex = i
					break
				}
			}
			reply.Xterm = XTerm
			reply.XIndex = XIndex
			reply.Identification = Follower
			reply.Reply_term = rf.getCurrentTerm()
			reply.Success = false
			return
		}
		// replicate log entries in AppendEntries to follower
		// if inconsistency occurs, delete the following log entries at that point in follower
		// if control flow can reach here, it means that log entry pointed by Prev_Log_Index in rf.logs is matched with Prev_Log_Term
		// the start_index is assigned when inconsistency occurs
		// Deduplication is needed!!!!!
		start_index := 0
		for i, j := args.Prev_Log_Index+1, 0; i < rf.getLogLen() && j < len(args.Entries); i++ {
			log := rf.getLogEntry(i)
			term, command := log.Term, log.Command
			if term != args.Entries[j].Term || command != args.Entries[j].Command {
				// delete the following element
				// rf.logs = rf.logs[:i]
				logs := rf.getLogs()
				rf.setLogs(logs[:i])
				rf.setCurrentIndex(rf.getLogLen() - 1)
				break
			} else {
				start_index++
			}
			j++
		}
		// append all log entries to rf.logs
		for i := start_index; i < len(args.Entries); i++ {
			// rf.logs = append(rf.logs, args.Entries[i])
			rf.appendLogEntry(*args.Entries[i])
		}
		rf.setCurrentIndex(rf.getLogLen() - 1)
		rf.setCurrentTerm(args.Leader_term)
		rf.setCommittedIndex(min(rf.getCurrentIndex(), args.Leader_committed_index))
		for i := rf.getLastApplied(); i < rf.getCommittedIndex() && i < rf.getLogLen(); i++ {
			rf.addLastApplied()
			rf.applyLogEntry(rf.getLastApplied())
		}
		rf.resetElectionTimeout()

		reply.Reply_term = rf.getCurrentTerm()
		reply.Success = true
		reply.Identification = Follower
		//reply.Debug_Info = fmt.Sprintf("[server]: follower: %v, append success, [current_idx]: %v, [log len]: %v\n", rf.server_id, rf.current_index, len(rf.logs))
	} else if rf.isCandidate() {
		rf.resetElectionTimeout()
		rf.setCurrentTerm(args.Leader_term)
		rf.setState(Follower)
		rf.setVoteFor(-1)

		reply.Reply_term = rf.getCurrentTerm()
		reply.Success = false
		reply.Identification = Candidate
		//reply.Debug_Info = fmt.Sprintf("[server]: candidate: %v, fault\n", rf.server_id)
	} else {
		// leader
		if args.Leader_term >= rf.getCurrentTerm() && args.Leader_committed_index >= rf.getCommittedIndex() {
			rf.resetElectionTimeout()
			rf.setCurrentTerm(args.Leader_term)
			rf.setState(Follower)
			rf.setVoteFor(-1)
		}

		reply.Reply_term = rf.getCurrentTerm()
		reply.Success = false
		reply.Identification = Leader
		//reply.Debug_Info = fmt.Sprintf("[server]: leader: %v, fault\n", rf.server_id)
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

func (rf *Raft) heartbeat_message_empty() {
	// reset heartbeat timestamp
	rf.resetHeartbeatTimeout()
	for i := 0; i < rf.cnt; i++ {
		go func(idx int) {
			if idx == rf.getServerId() {
				return
			}
			if rf.getNextIndex()[idx] > rf.getLogLen() {
				return
			}
			var ae_args AppendEntriesArgs
			var ae_reply AppendEntriesReply
			ae_args = AppendEntriesArgs{Leader_term: rf.getCurrentTerm(), Leader_id: rf.getServerId(),
				Prev_Log_Index: rf.getNextIndex()[idx] - 1, Prev_Log_Term: rf.getLogs()[rf.getNextIndex()[idx]-1].Term,
				Leader_committed_index: rf.getCommittedIndex()}
			ae_reply = AppendEntriesReply{}
			ok := rf.sendAppendEntries(idx, &ae_args, &ae_reply)
			if !ok {
				return
			}
		}(i)
	}
}

func (rf *Raft) heartbeat_message_with_log() {

	// heartbeat messages
	// reset heartbeat timeout
	rf.resetHeartbeatTimeout()

	// a new log entry appended to leader

	for i := 0; i < rf.cnt; i++ {
		go func(idx int) {
			if idx == rf.getServerId() {
				return
			}
			var ae_args AppendEntriesArgs
			var ae_reply AppendEntriesReply
			// we can directly use rf_next_index[idx], not even with lock
			// we should duplicate it and use the copy
			// nxt_idx := rf.next_index[idx]
			nxt_idx := rf.getNextIndex()[idx]
			if nxt_idx > rf.getLogLen() {
				return
			}
			// fmt.Printf("[leader]: %v, [idx]: %v, [next idx]: %v\n", rf.server_id, idx, rf_next_index[idx])
			// log_entries := rf_logs[nxt_idx:]
			log_entries := rf.getLogs()[nxt_idx:]
			replicated_len := len(log_entries)
			ae_args = AppendEntriesArgs{Leader_term: rf.getCurrentTerm(), Leader_id: rf.getServerId(),
				Prev_Log_Index: nxt_idx - 1, Prev_Log_Term: rf.getLogs()[nxt_idx-1].Term,
				Entries: log_entries, Leader_committed_index: rf.getCommittedIndex()}
			ae_reply = AppendEntriesReply{}
			ok := rf.sendAppendEntries(idx, &ae_args, &ae_reply)
			if !ok {
				return
			}
			if ae_reply.Reply_term > rf.getCurrentTerm() {
				rf.setState(Follower)
				return
			}
			if ae_reply.Identification != Follower {
				return
			}
			if !ae_reply.Success {
				// if rf.getNextIndex()[idx] > 1 {
				// 	rf.addNextIndex(idx, -1)
				// }
				has_xterm := false
				idx_xterm := -1
				for i := 1; i < rf.getLogLen(); i++ {
					log := rf.getLogEntry(i)
					if ae_reply.Xterm == log.Term {
						has_xterm = true
						idx_xterm = i
					}
				}
				if has_xterm {
					rf.setNextIndex(idx, idx_xterm+1)
				} else {
					rf.setNextIndex(idx, ae_reply.XIndex)
				}
			} else {
				// rf.next_index[idx] += replicated_len
				// rf.match_index[idx] = rf.next_index[idx] - 1
				rf.addNextIndex(idx, replicated_len)
				rf.setMatchIndex(idx, rf.getNextIndex()[idx]-1)
			}
		}(i)
	}
}

func (rf *Raft) check_commit() {
	try_committed_index := rf.getCommittedIndex() + 1
	cnt := 1
	// try_committed_index must lower than len(rf.logs)
	if try_committed_index >= rf.getLogLen() {
		return
	}
	nxt_idx := rf.getNextIndex()
	for _, val := range nxt_idx {
		if val-1 >= try_committed_index {
			cnt++
		}
	}
	if cnt >= (rf.getCnt()+1)/2 {
		// rf.committed_index++
		rf.addCommittedIndex()
	}
	for i := rf.getLastApplied(); i < rf.getCommittedIndex(); i++ {
		// rf.last_applied++
		rf.addLastApplied()
		rf.applyLogEntry(rf.getLastApplied())
	}
}

func (rf *Raft) CandidateRequestVotes(election_timeout int) int {
	// initialize a new election, reset property
	rf.resetElectionTimeout()
	rf.addElectionRounds()
	rf.addCurrentTerm()
	rf.setVoteFor(rf.getServerId())

	votes := 1 // vote for itself
	request_vote_state := true
	var lk sync.Mutex

	for i := 0; i < rf.cnt; i++ {
		go func(idx int) {
			if idx == rf.server_id {
				return
			}
			rv_args := RequestVoteArgs{Candidate_term: rf.current_term,
				Candidate_Id:              rf.getServerId(),
				Last_Log_Index:            rf.getLastApplied(),
				Last_Log_Term:             rf.getLogs()[rf.getLastApplied()].Term,
				Candidate_Election_Rounds: rf.getElectionRounds()}
			rv_reply := RequestVoteReply{}
			for request_vote_state && !rf.sendRequestVote(idx, &rv_args, &rv_reply) {
				time.Sleep(time.Millisecond * time.Duration(10))
			}
			//DPrintf("[candidate] [server id]: %v, [election_state]: %v, [request id]: %v, [reply_term]: %v, [grant]: %v\n", rf_server_id, request_vote_state, idx, rv_reply.Reply_Term, rv_reply.Vote_Grant)
			if rv_reply.Reply_Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(rv_reply.Reply_Term)
				rf.setState(Follower)
				rf.setVoteFor(-1)
				rf.resetElectionTimeout()
				return
			}
			if rv_reply.Vote_Grant {
				lk.Lock()
				votes++
				lk.Unlock()
			}
		}(i)
	}

	time.Sleep(time.Millisecond * time.Duration(election_timeout))
	request_vote_state = false
	return votes
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// atomic variable, there is no race condition
		//fmt.Printf("[server id]: %v, [state]: %v, [term]: %v, [dead]: %v\n", rf.server_id, rf.state, rf.current_term, rf.dead)

		// Your code here (2A)
		// Check if a leader election should be started.

		// election timeout range from 400ms to 500ms
		election_timeout := 200 + (rand.Int31() % 150)
		heartbeat_timeout := 100
		if rf.isFollower() {
			// when thread sleeps, cannot hold lock!!!
			time.Sleep(time.Millisecond * time.Duration(election_timeout))
			// fmt.Printf(colorBlue+"[follower] [server id]: %v, current follower\n"+colorReset, rf.server_id)
			if rf.isVoteFor() && time.Since(rf.getLeaderElectionTimestamp()) >= time.Millisecond*time.Duration(election_timeout) {
				// initialize a new election
				rf.state = Candidate
			}
		} else if rf.isCandidate() {
			votes := rf.CandidateRequestVotes(int(election_timeout))

			if rf.isFollower() {
				continue
			}

			// DPrintf("[candidate] [server id]: %v, [votes]: %v\n", rf_server_id, votes)
			if votes >= (rf.cnt+1)/2 {
				rf.setState(Leader)
				// we cannot use gorountine
				rf.heartbeat_message_with_log()
			}
			// if failed to become leader, retain candidate state
		} else if rf.isLeader() {

			if time.Since(rf.getHeartbeatTimestamp()) >= time.Millisecond*time.Duration(heartbeat_timeout) {
				// heartbeat messages
				// reset heartbeat timeout
				rf.resetHeartbeatTimeout()

				// a new log entry appended to leader
				// if rf_committed_index+1 < len(rf_logs) {
				// 	rf.heartbeat_message_with_log()
				// } else {
				// 	rf.heartbeat_message_empty()
				// }
				rf.heartbeat_message_with_log()
				rf.check_commit()
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
