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
	"fmt"
	"math/rand"
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
	Current_Term    int
	Current_Index   int
	Vote_For        int
	Logs            []LogType
	Committed_Index int
	Last_Applied    int
	Election_Rounds int
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

	cnt             atomic.Value // number of servers
	server_id       atomic.Value // identifier of server
	state           ServerState  // state of server
	current_term    atomic.Value // server's current term, in each RPC, it should be updated
	current_index   atomic.Value // index of latest log entry
	vote_for        atomic.Value // server id of this follower vote for
	committed_index atomic.Value // index of last committed log entry
	last_applied    atomic.Value // index of last applied to state machine

	logs []*LogType // store log, first index is 1

	// heartbeat 10 times per second (1s = 1000ms) -> every 100ms emit heartbeat
	// should elect a new leader in five seconds (5000ms) -> suppose 10 times of split vote -> election must finish in 500ms
	// election ranges from 300ms to 500ms
	leader_election_timestamp time.Time // timestamp for leader election
	heart_beat_timestamp      time.Time // used for leader, timestamp for heart beat

	next_index  map[int]int // used for leader, index of next log entry to send to follower(key is server id, val is index of next lof entry to be send in leader)
	match_index map[int]int // used for leader, index of log entry which is replicated in that server(key is server id, val is index of log entry in leader)
	index_latch sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	// return term, isleader
	rf.mu.Lock()
	x := rf.current_term.Load()
	y := rf.state
	rf.mu.Unlock()
	return x.(int), y == Leader
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

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	var rf_persist PersistType
	rf_persist.Current_Index = rf.getCurrentIndex()
	rf_persist.Current_Term = rf.getCurrentTerm()
	rf_persist.Vote_For = rf.getVoteFor()
	rf_persist.Committed_Index = rf.getCommittedIndex()
	rf_persist.Last_Applied = rf.getLastApplied()
	for i := 1; i < rf.getLogLen(); i++ {
		rf_persist.Logs = append(rf_persist.Logs, rf.getLogEntry(i))
	}
	encoder.Encode(&rf_persist)
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
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	var content PersistType
	if decoder.Decode(&content) == nil {
		rf.setCurrentIndex(content.Current_Index)
		rf.setCurrentTerm(content.Current_Term)
		rf.setVoteFor(content.Vote_For)
		rf.setCommittedIndex(content.Committed_Index)
		rf.setLastApplied(content.Last_Applied)
		for _, log := range content.Logs {
			rf.appendLogEntry(log)
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// auxiliary function

func (rf *Raft) String() string {
	var res string
	res += fmt.Sprintf("[id]: %v, [cnt]: %v, [state]: %v, [killed]: %v\n", rf.getServerId(), rf.getCnt(), rf.getState(), rf.killed())
	res += fmt.Sprintf("[current]: [term]: %v, [idx]: %v\n", rf.getCurrentTerm(), rf.getCurrentIndex())
	res += fmt.Sprintf("[committed_index]: %v, [last_applied]: %v\n", rf.getCommittedIndex(), rf.getLastApplied())
	res += fmt.Sprintf("[vote for]: %v\n", rf.getVoteFor())
	logs := rf.getLogs()
	for i, v := range logs {
		res += fmt.Sprintf(colorCyan+"[log]: [idx]: %v, [val]: %v\n"+colorReset, i, v)
	}
	return res
}

func (rf *Raft) getPersist() Persister {
	z := rf.persister
	return *z
}

// leader is 0, candidate is 1, follower is 2
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

func (rf *Raft) getMe() int {
	rf.mu.Lock()
	z := rf.me
	rf.mu.Unlock()
	return z
}

func (rf *Raft) setMe(val int) {
	rf.mu.Lock()
	rf.me = val
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) getDead() int32 {
	z := atomic.LoadInt32(&rf.dead)
	return z
}

func (rf *Raft) setDead(val int32) {
	atomic.StoreInt32(&rf.dead, val)
	rf.persist()
}

func (rf *Raft) getCnt() int {
	z := rf.cnt.Load()
	return z.(int)
}

func (rf *Raft) setCnt(val int) {
	rf.cnt.Store(val)
	rf.persist()
}

func (rf *Raft) getServerId() int {
	z := rf.server_id.Load()
	return z.(int)
}

func (rf *Raft) setServerId(val int) {
	rf.server_id.Store(val)
	rf.persist()
}

func (rf *Raft) setState(new_state ServerState) {
	rf.mu.Lock()
	rf.state = new_state
	rf.mu.Unlock()
}

func (rf *Raft) getState() ServerState {
	rf.mu.Lock()
	z := rf.state
	rf.mu.Unlock()
	return z
}

func (rf *Raft) getCurrentIndex() int {
	z := rf.current_index.Load()
	return z.(int)
}

func (rf *Raft) setCurrentIndex(idx int) {
	rf.current_index.Store(idx)
	rf.persist()
}

func (rf *Raft) addCurrentIndex() {
	z := rf.current_index.Load()
	rf.current_index.Store(z.(int) + 1)
	rf.persist()
}

func (rf *Raft) getCurrentTerm() int {
	z := rf.current_term.Load()
	return z.(int)
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.current_term.Store(term)
	rf.persist()
}

func (rf *Raft) addCurrentTerm() {
	z := rf.current_term.Load()
	rf.current_term.Store(z.(int) + 1)
	rf.persist()
}

func (rf *Raft) getCommittedIndex() int {
	z := rf.committed_index.Load()
	return z.(int)
}

func (rf *Raft) setCommittedIndex(idx int) {
	rf.committed_index.Store(idx)
	rf.persist()
}

func (rf *Raft) addCommittedIndex() {
	z := rf.committed_index.Load()
	rf.committed_index.Store(z.(int) + 1)
	rf.persist()
}

func (rf *Raft) getLastApplied() int {
	z := rf.last_applied.Load()
	return z.(int)
}

func (rf *Raft) setLastApplied(idx int) {
	rf.last_applied.Store(idx)
	rf.persist()
}

func (rf *Raft) addLastApplied() {
	z := rf.last_applied.Load()
	rf.last_applied.Store(z.(int) + 1)
	rf.persist()
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
	z := rf.vote_for.Load()
	return z.(int) == -1
}

func (rf *Raft) setVoteFor(id int) {
	rf.vote_for.Store(id)
	rf.persist()
}

func (rf *Raft) getVoteFor() int {
	z := rf.vote_for.Load()
	return z.(int)
}

func (rf *Raft) applyLogEntry(index int) {
	rf.mu.Lock()
	rf.tester <- ApplyMsg{CommandValid: true, Command: rf.logs[index].Command, CommandIndex: index}
	rf.mu.Unlock()
}

func (rf *Raft) getLogs() []*LogType {
	rf.mu.Lock()
	logs := make([]*LogType, len(rf.logs))
	copy(logs, rf.logs)
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
	rf.persist()
}

func (rf *Raft) setLogs(val []*LogType) {
	rf.mu.Lock()
	rf.logs = val
	rf.mu.Unlock()
	rf.persist()
}

func (rf *Raft) getNextIndex() map[int]int {
	rf.mu.Lock()
	z := make(map[int]int, len(rf.next_index))
	for k, v := range rf.next_index {
		z[k] = v
	}
	rf.mu.Unlock()
	return z
}

func (rf *Raft) getMatchIndex() map[int]int {
	rf.mu.Lock()
	z := make(map[int]int, len(rf.match_index))
	for k, v := range rf.match_index {
		z[k] = v
	}
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

func (rf *Raft) setLeaderElectionTimestamp(val time.Time) {
	rf.mu.Lock()
	rf.leader_election_timestamp = val
	rf.mu.Unlock()
}

func (rf *Raft) getHeartbeatTimestamp() time.Time {
	rf.mu.Lock()
	z := rf.heart_beat_timestamp
	rf.mu.Unlock()
	return z
}

func (rf *Raft) setHeartbeatTimestamp(val time.Time) {
	rf.mu.Lock()
	rf.heart_beat_timestamp = val
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Candidate_term int // candidate term
	Candidate_Id   int // candidate id
	Last_Log_Index int // index of last log entry in candidate
	Last_Log_Term  int // term of log entry pointed by Last_Log_Index
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Reply_Term int  // server's term
	Vote_Grant bool // whether vote or not, true is vote
	Debug_Info string
}

func (rf *Raft) RequestVoteVoteforCondition(args *RequestVoteArgs) bool {
	rf_logs := rf.getLogs()
	DPrintf("[server]: %v\n", rf.getServerId())
	for i, l := range rf_logs {
		DPrintf("[idx]: %v, [log]: %v\n", i, l)
	}
	DPrintf("[args] [last term, last index]: (%v, %v)\n", args.Last_Log_Term, args.Last_Log_Index)
	return (args.Last_Log_Term > rf_logs[rf.getCommittedIndex()].Term || (args.Last_Log_Term == rf_logs[rf.getCommittedIndex()].Term && args.Last_Log_Index >= rf.getCommittedIndex()))
}

func (rf *Raft) RequestVoteFollower(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Candidate_term < rf.getCurrentTerm() {
		reply.Reply_Term = rf.getCurrentTerm()
		reply.Vote_Grant = false
		reply.Debug_Info = "candidate's term is lower than follower"
		return
	}

	// must greater than, equal is not allowed
	if args.Candidate_term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Candidate_term)
		if rf.RequestVoteVoteforCondition(args) {
			rf.resetElectionTimeout()
			rf.setVoteFor(args.Candidate_Id)
			reply.Reply_Term = rf.getCurrentTerm()
			reply.Vote_Grant = true
			reply.Debug_Info = "follower vote"
			return
		}
	}
	reply.Reply_Term = rf.getCurrentTerm()
	reply.Vote_Grant = false
}

func (rf *Raft) RequestVoteCandidate(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Candidate_term < rf.getCurrentTerm() {
		reply.Reply_Term = rf.getCurrentTerm()
		reply.Vote_Grant = false
		reply.Debug_Info = "candidate's term is lower than candidate"
		return
	}

	// must greater than, equal is not allowed
	// there is a scenario that two followers simultaneously become candidate
	// if the condition is greater than and equal to, as a consequence, two candidate both will become leader
	if args.Candidate_term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Candidate_term)
		if rf.RequestVoteVoteforCondition(args) {
			rf.resetElectionTimeout()
			rf.setState(Follower)
			rf.setVoteFor(args.Candidate_Id)
			reply.Reply_Term = rf.getCurrentTerm()
			reply.Vote_Grant = true
			return
		}
	}
	reply.Reply_Term = rf.getCurrentTerm()
	reply.Vote_Grant = false
}

func (rf *Raft) RequestVoteLeader(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Candidate_term < rf.getCurrentTerm() {
		reply.Reply_Term = rf.getCurrentTerm()
		reply.Vote_Grant = false
		reply.Debug_Info = "candidate's term is lower than leader"
		return
	}

	// must greater than, equal is not allowed
	if args.Candidate_term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Candidate_term)
		if rf.RequestVoteVoteforCondition(args) {
			rf.resetElectionTimeout()
			rf.setState(Follower)
			rf.setVoteFor(args.Candidate_Id)
			reply.Reply_Term = rf.getCurrentTerm()
			reply.Vote_Grant = true
			reply.Debug_Info = "leader vote"
			return
		}
	}
	reply.Reply_Term = rf.getCurrentTerm()
	reply.Vote_Grant = false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	switch rf.getState() {
	case Follower:
		rf.RequestVoteFollower(args, reply)
	case Candidate:
		rf.RequestVoteCandidate(args, reply)
	case Leader:
		rf.RequestVoteLeader(args, reply)
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
	// to mark whether current leader is outdated
	// the leader is outdated: term is lower or log is outdated
	Xterm      int // for fast recovery
	XIndex     int
	Debug_Info string
}

func (rf *Raft) AppendEntriesCondition(args *AppendEntriesArgs) bool {
	rf_logs := rf.getLogs()
	return (args.Prev_Log_Index < rf.getLogLen() && args.Prev_Log_Index >= 0 && rf_logs[args.Prev_Log_Index].Term == args.Prev_Log_Term)
}

func (rf *Raft) AppendEntriesCommittedOutdated(args *AppendEntriesArgs) bool {
	return args.Leader_committed_index < rf.getLastApplied()
}

func (rf *Raft) AppendEntriesFollower(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Leader_term < rf.getCurrentTerm() || rf.AppendEntriesCommittedOutdated(args) {
		reply.Reply_term = rf.getCurrentTerm()
		reply.Success = false
		return
	}

	if len(args.Entries) == 0 {
		rf.setCurrentTerm(args.Leader_term)
		rf.setVoteFor(-1)
		rf.setCommittedIndex(min(rf.getCurrentIndex(), args.Leader_committed_index))
		for i := rf.getLastApplied(); i < rf.getCommittedIndex(); i++ {
			rf.addLastApplied()
			rf.applyLogEntry(rf.getLastApplied())
		}
		rf.resetElectionTimeout()

		reply.Reply_term = rf.getCurrentTerm()
		reply.Success = true
		return
	}

	// follower don't have log entry in Prev_Log_Index
	if args.Prev_Log_Index >= rf.getLogLen() {
		rf.resetElectionTimeout()
		rf.setCurrentTerm(args.Leader_term)
		rf.setVoteFor(-1)
		reply.Reply_term = rf.getCurrentTerm()
		reply.Xterm = -1
		reply.XIndex = rf.getLogLen()
		reply.Success = false
		return
	}

	// if the term of log entry pointed by Prev_Log_Index in follower not matchs Prev_Log_Term
	if !rf.AppendEntriesCondition(args) {
		XTerm, XIndex := rf.getLogEntry(args.Prev_Log_Index).Term, -1
		for i := 1; i < rf.getLogLen(); i++ {
			if log := rf.getLogEntry(i); log.Term == XTerm {
				XIndex = i
				break
			}
		}
		rf.resetElectionTimeout()
		rf.setCurrentTerm(args.Leader_term)
		rf.setVoteFor(-1)
		reply.Xterm = XTerm
		reply.XIndex = XIndex
		reply.Reply_term = rf.getCurrentTerm()
		reply.Success = false
		return
	}

	// replicate log entries in AppendEntries to follower
	// if inconsistency occurs, delete the following log entries at that point in follower
	// if control flow can reach here, it means that log entry pointed by Prev_Log_Index in rf.logs is matched with Prev_Log_Term
	// the start_index is assigned when inconsistency occurs
	// Deduplication is needed!!!!!

	i, j := args.Prev_Log_Index+1, 0
	for i < rf.getLogLen() && j < len(args.Entries) {
		log := rf.getLogEntry(i)
		term, command := log.Term, log.Command
		if term != args.Entries[j].Term && command != args.Entries[j] {
			logs := rf.getLogs()
			rf.setLogs(logs[:i])
			// conflict_idx = j
			break
		}
		i++
		j++
	}
	if i < rf.getLogLen() {
		logs := rf.getLogs()
		rf.setLogs(logs[:i])
	}
	if j < len(args.Entries) {
		for k := j; k < len(args.Entries); k++ {
			rf.appendLogEntry(*args.Entries[k])
		}
	}

	rf.setCurrentIndex(rf.getLogLen() - 1)
	rf.setCurrentTerm(args.Leader_term)
	rf.setVoteFor(-1)
	rf.setCommittedIndex(min(rf.getCurrentIndex(), args.Leader_committed_index))
	rf.resetElectionTimeout()

	reply.Reply_term = rf.getCurrentTerm()
	reply.Success = true
}

func (rf *Raft) AppendEntriesCandidate(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// must greater than and equal to
	if args.Leader_term >= rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Leader_term)
		if rf.AppendEntriesCondition(args) {
			rf.setState(Follower)
			rf.resetElectionTimeout()
			rf.setVoteFor(-1)
		}
	}
	reply.Reply_term = rf.getCurrentTerm()
	reply.Success = false
}

func (rf *Raft) AppendEntriesLeader(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// must greater than and equal to
	if args.Leader_term >= rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Leader_term)
		if rf.AppendEntriesCondition(args) {
			rf.setState(Follower)
			rf.resetElectionTimeout()
			rf.setVoteFor(-1)
		}
	}
	reply.Reply_term = rf.getCurrentTerm()
	reply.Success = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	switch rf.getState() {
	case Follower:
		rf.AppendEntriesFollower(args, reply)
	case Candidate:
		rf.AppendEntriesCandidate(args, reply)
	case Leader:
		rf.AppendEntriesLeader(args, reply)
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
	rf.index_latch.Lock()
	log_entry := LogType{Command: command, Term: term}
	rf.appendLogEntry(log_entry)
	rf.addCurrentIndex()
	z := rf.getCurrentIndex()
	rf.index_latch.Unlock()

	return z, term, isLeader
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
	for i := 0; i < rf.getCnt(); i++ {
		go func(idx int) {
			if idx == rf.getServerId() {
				return
			}
			rf.index_latch.Lock()
			next_index := rf.getNextIndex()
			logs := rf.getLogs()
			rf.index_latch.Unlock()
			if next_index[idx] > rf.getLogLen() {
				return
			}
			var ae_args AppendEntriesArgs
			var ae_reply AppendEntriesReply
			ae_args = AppendEntriesArgs{Leader_term: rf.getCurrentTerm(), Leader_id: rf.getServerId(),
				Prev_Log_Index: next_index[idx] - 1, Prev_Log_Term: logs[next_index[idx]-1].Term,
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
	rf_logs := rf.getLogs()

	for i := 0; i < rf.getCnt(); i++ {
		go func(idx int) {
			if idx == rf.getServerId() {
				return
			}
			var ae_args AppendEntriesArgs
			var ae_reply AppendEntriesReply
			// we can directly use rf_next_index[idx], not even with lock
			// we should duplicate it and use the copy
			// nxt_idx := rf.next_index[idx]
			rf.index_latch.Lock()
			nxt_idx := rf.getNextIndex()[idx]
			logs := rf.getLogs()
			rf.index_latch.Unlock()
			if nxt_idx > rf.getLogLen() {
				return
			}
			log_entries := rf_logs[nxt_idx:]
			replicated_len := len(log_entries)
			ae_args = AppendEntriesArgs{Leader_term: rf.getCurrentTerm(), Leader_id: rf.getServerId(),
				Prev_Log_Index: nxt_idx - 1, Prev_Log_Term: logs[nxt_idx-1].Term,
				Entries: log_entries, Leader_committed_index: rf.getCommittedIndex()}
			ae_reply = AppendEntriesReply{}
			ok := rf.sendAppendEntries(idx, &ae_args, &ae_reply)
			if !ok {
				return
			}
			DPrintf(colorCyan+"[server]: %v, [idx]: %v\n"+colorReset, rf.getServerId(), idx)
			DPrintf("[reply term]: %v\n", ae_reply.Reply_term)
			// if outdated is true, irrespective of server type, current leader should step down
			if ae_reply.Reply_term > rf.getCurrentTerm() {
				rf.setCurrentTerm(ae_reply.Reply_term)
				rf.resetElectionTimeout()
				rf.setState(Follower)
				return
			}
			if !ae_reply.Success {
				has_xterm := false
				idx_xterm := -1
				for i := 1; i < rf.getLogLen(); i++ {
					log := rf.getLogEntry(i)
					if ae_reply.Xterm == log.Term {
						has_xterm = true
						idx_xterm = i
					}
				}
				rf.index_latch.Lock()
				if has_xterm {
					if idx_xterm >= 0 {
						rf.setNextIndex(idx, idx_xterm+1)
					}
				} else {
					if ae_reply.XIndex >= 1 {
						rf.setNextIndex(idx, ae_reply.XIndex)
					}
				}
				rf.index_latch.Unlock()
			} else {
				// rf.next_index[idx] += replicated_len
				// rf.match_index[idx] = rf.next_index[idx] - 1
				rf.index_latch.Lock()
				rf.addNextIndex(idx, replicated_len)
				rf.setMatchIndex(idx, rf.getNextIndex()[idx]-1)
				rf.index_latch.Unlock()
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
	// the problem in figure 8 is that a leader create a log entry and it crash before it commit this log entry
	// this log entry may not replicated or may replicated in a majority of follower
	// in the latter case, this log entry will be considered committed from leader's standpoint
	// in order for eliminating the issue of figure 8, the commit action in different log entry are different
	// in case of log entries with previous term, only log entries with current term have committed, those log entries can apply
	// in case of log entries with current term, when it replicated in a majority of follower, can treat it committed
	rf.index_latch.Lock()
	nxt_idx := rf.getNextIndex()
	for _, val := range nxt_idx {
		if val-1 >= try_committed_index {
			cnt++
		}
	}
	rf.index_latch.Unlock()
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
	rf.addCurrentTerm()
	rf.setVoteFor(rf.getServerId())

	votes := int32(1) // vote for itself
	var request_vote_state atomic.Value
	request_vote_state.Store(true)
	var lk sync.Mutex

	for i := 0; i < rf.getCnt(); i++ {
		go func(idx int) {
			if idx == rf.getServerId() {
				return
			}
			rv_args := RequestVoteArgs{Candidate_term: rf.getCurrentTerm(),
				Candidate_Id:   rf.getServerId(),
				Last_Log_Index: rf.getCommittedIndex(),
				Last_Log_Term:  rf.getLogs()[rf.getCommittedIndex()].Term}

			rv_reply := RequestVoteReply{}
			for request_vote_state.Load() == true && !rf.sendRequestVote(idx, &rv_args, &rv_reply) {
				time.Sleep(time.Millisecond * time.Duration(20))
			}
			DPrintf("[request vote]  [server]: %v, [term]: %v, [obj]: %v, [reply term, vote grant]: (%v, %v)\n", rf.getServerId(), rf.getCurrentTerm(), idx, rv_reply.Reply_Term, rv_reply.Vote_Grant)
			DPrintf("[args]: %v\n", rv_args)
			DPrintf("[debug]: %v\n", rv_reply.Debug_Info)
			// candidate discover a higher term or the existence of leader, should convert to follower
			if rf.getCurrentTerm() < rv_reply.Reply_Term {
				rf.setCurrentTerm(rv_reply.Reply_Term)
				rf.setState(Follower)
				rf.setVoteFor(-1)
				rf.resetElectionTimeout()
				return
			}
			if rv_reply.Vote_Grant {
				lk.Lock()
				atomic.AddInt32(&votes, 1)
				lk.Unlock()
			}
		}(i)
	}

	time.Sleep(time.Millisecond * time.Duration(election_timeout))
	request_vote_state.Store(false)
	return int(atomic.LoadInt32(&votes))
}

func (rf *Raft) ticker() {
	election_timeout := 200 + (rand.Int31() % 200)
	heartbeat_timeout := 100
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		// election timeout range from 400ms to 500ms
		if rf.isFollower() {
			// when thread sleeps, cannot hold lock!!!
			time.Sleep(time.Millisecond * time.Duration(election_timeout))
			if time.Since(rf.getLeaderElectionTimestamp()) >= time.Millisecond*time.Duration(election_timeout) {
				// initialize a new election
				rf.setState(Candidate)
			}
		} else if rf.isCandidate() {
			votes := rf.CandidateRequestVotes(int(election_timeout))
			if rf.isFollower() {
				continue
			}
			if votes >= (rf.getCnt()+1)/2 {
				DPrintf(colorRed+"[become leader]: %v\n"+colorReset, rf.getServerId())
				rf.setState(Leader)
				// we cannot use gorountine
				rf.heartbeat_message_with_log()
			}
			// if failed to become leader, retain candidate state
		} else if rf.isLeader() {

			if time.Since(rf.getHeartbeatTimestamp()) >= time.Millisecond*time.Duration(heartbeat_timeout) {
				// reset heartbeat timeout
				rf.resetHeartbeatTimeout()

				rf.heartbeat_message_with_log()
				time.Sleep(time.Millisecond * time.Duration(heartbeat_timeout))
				// judgement must be made here
				if !rf.isLeader() {
					continue
				}
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
	rf.cnt.Store(len(peers))
	rf.state = Follower
	rf.server_id.Store(me)
	rf.vote_for.Store(-1) // -1 means there hasn't vote yet
	rf.current_term.Store(0)
	rf.current_index.Store(0) // there are always one log entry in each server
	rf.committed_index.Store(0)
	rf.last_applied.Store(0)
	atomic.StoreInt32(&rf.dead, 0)
	rf.leader_election_timestamp = time.Now()
	rf.heart_beat_timestamp = time.Now()

	// every server initially has one log entry
	rf.logs = append(rf.logs, &LogType{Term: 0})

	rf.next_index = make(map[int]int, rf.getCnt())
	rf.match_index = make(map[int]int, rf.getCnt())
	for i := 0; i < rf.getCnt(); i++ {
		rf.next_index[i] = 1
		rf.match_index[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
