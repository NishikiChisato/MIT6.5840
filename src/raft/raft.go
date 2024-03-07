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
	Index   int
}

type PersistType struct {
	CurrentTerm int
	VoteFor     int
	Logs        []LogType
	//snapshot should also be persisted
	LastIncludedIndex int
	LastIncludedTerm  int
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
	// index 0 either be empty log entry or log entry with lastIncludeIndex and lastIncludeTerm
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
	// the object of persist not only currentTerm/voteFor/logs, but snapshot
	// the former consists of rf.snapshot/rf.lastIncludeIndex/rf.lastIncludeTerm
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

func (rf *Raft) AsyncGetState() <-chan bool {
	ch := make(chan bool)
	go func() {
		rf.mu.Lock()
		z := rf.state == Leader
		rf.mu.Unlock()
		ch <- z
	}()
	return ch
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

	Debug(dPersist, "S%d %v persist state", rf.me, rf.state.String())
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	var persist_storage PersistType
	persist_storage.CurrentTerm = rf.currentTerm
	persist_storage.VoteFor = rf.voteFor
	persist_storage.Logs = append(persist_storage.Logs, rf.logs[0:]...)

	persist_storage.LastIncludedIndex = rf.lastIncludeIndex
	persist_storage.LastIncludedTerm = rf.lastIncludeTerm

	encoder.Encode(&persist_storage)
	raft_state := buf.Bytes()
	rf.persister.Save(raft_state, rf.snapshot)
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
	Debug(dPersist, "S%d %v recovery from persist", rf.me, rf.state.String())
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	var content PersistType
	if decoder.Decode(&content) == nil {
		rf.currentTerm = content.CurrentTerm
		rf.voteFor = content.VoteFor
		// rf.logs = append(rf.logs, content.Logs...)
		rf.logs = make([]LogType, len(content.Logs))
		copy(rf.logs, content.Logs)

		rf.lastIncludeIndex = content.LastIncludedIndex
		rf.lastIncludeTerm = content.LastIncludedTerm

		Debug(dPersist, "S%d %v after persist, log is: %+v", rf.me, rf.state.String(), rf.logs)
	} else {
		Debug(dError, "S%d %v recovery raft state from persist Fatal", rf.me, rf.state.String())
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.snapshot = data
}

func (rf *Raft) globalIndex2LocalIndex(idx, lastIncludedIndex int) int {
	return idx - lastIncludedIndex
}

func (rf *Raft) localIndex2GlobalIndex(idx, lastIncludedIndex int) int {
	return idx + lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go func() {
		// Snapshot must asynchronous return due to deadlock, caused by synchronous return
		rf.mu.Lock()
		if index <= rf.lastIncludeIndex || index > rf.commitIndex || index > rf.lastApplied {
			// no matter current server is leader or follower, the interval of snapshot ranges from rf.lastIncludeIndex + 1(included) to rf.lastApplied(included)
			rf.mu.Unlock()
			return
		}
		Debug(dClient, "S%d %s receive snapshot request at %v", rf.me, rf.state.String(), index)

		rf.logs[0] = rf.logs[rf.globalIndex2LocalIndex(index, rf.lastIncludeIndex)]
		rf.logs = append(rf.logs[:1], rf.logs[rf.globalIndex2LocalIndex(index, rf.lastIncludeIndex)+1:]...)
		rf.lastIncludeIndex = index
		rf.lastIncludeTerm = rf.logs[0].Term

		rf.snapshot = snapshot
		rf.persist()
		rf.mu.Unlock()
	}()
}

// we provide synchronous version of snapshot for upper layer
func (rf *Raft) SyncSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	if index <= rf.lastIncludeIndex || index > rf.commitIndex || index > rf.lastApplied {
		// no matter current server is leader or follower, the interval of snapshot ranges from rf.lastIncludeIndex + 1(included) to rf.lastApplied(included)
		rf.mu.Unlock()
		return
	}
	Debug(dClient, "S%d %s receive snapshot request at %v", rf.me, rf.state.String(), index)

	rf.logs[0] = rf.logs[rf.globalIndex2LocalIndex(index, rf.lastIncludeIndex)]
	rf.logs = append(rf.logs[:1], rf.logs[rf.globalIndex2LocalIndex(index, rf.lastIncludeIndex)+1:]...)
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.logs[0].Term

	rf.snapshot = snapshot
	rf.persist()
	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	LeaderTerm       int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	ReplyTerm int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	Debug(dLog, "S%d %v(term: %v, LII: %v, LIT: %v, LA: %v) receive IS from %v, LII: %v, LIT: %v",
		rf.me, rf.state.String(), rf.currentTerm, rf.lastIncludeIndex, rf.lastIncludeTerm, rf.lastApplied, args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
	if args.LeaderTerm < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.LeaderTerm > rf.currentTerm {
		rf.convertFollower(args.LeaderTerm)
		rf.persist()
	}
	rf.electionTimestamp = time.Now()
	reply.ReplyTerm = rf.currentTerm
	if args.LeaderTerm == rf.currentTerm && args.LastIncludeIndex > rf.lastIncludeIndex {
		truncate_index := 0
		for {
			if truncate_index >= len(rf.logs) || (rf.logs[truncate_index].Index == args.LastIncludeIndex && rf.logs[truncate_index].Term == args.LastIncludeTerm) {
				break
			}
			truncate_index++
		}

		rf.logs = rf.logs[truncate_index:]
		rf.snapshot = args.Data
		rf.lastIncludeIndex = args.LastIncludeIndex
		rf.lastIncludeTerm = args.LastIncludeTerm
		rf.lastApplied = args.LastIncludeIndex
		rf.commitIndex = min(max(rf.commitIndex, args.LastIncludeIndex), rf.localIndex2GlobalIndex(len(rf.logs), rf.lastIncludeIndex))

		if len(rf.logs) == 0 {
			rf.logs = append(rf.logs, LogType{Command: nil, Term: rf.lastIncludeTerm, Index: rf.lastIncludeIndex})
		}

		rf.persist()

		Debug(dCommit, "S%d %v apply snapshot at index: %v, LII: %v, LIT: %v, LA: %v, CI: %v",
			rf.me, rf.state.String(), rf.lastIncludeIndex, rf.lastIncludeIndex, rf.lastIncludeTerm, rf.lastApplied, rf.commitIndex)
		rf.tester <- ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotIndex: rf.lastIncludeIndex, SnapshotTerm: rf.lastIncludeTerm}
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
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
	ReplyTerm int  // server's term
	VoteGrant bool // whether vote or not, true is vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "S%d %v (term: %v, voteFor: %v, lastIndex: %v, lastTerm: %v) receive RV from %v, args is %+v",
		rf.me, rf.state.String(), rf.currentTerm, rf.voteFor, rf.localIndex2GlobalIndex(len(rf.logs), rf.lastIncludeIndex), rf.logs[len(rf.logs)-1].Term, args.CandidateId, args)
	if rf.currentTerm < args.CandidateTerm {
		rf.convertFollower(args.CandidateTerm)
		rf.persist()
	}
	// last_index should be len(rf.logs) - 1 instead of lastApplied
	// suppose a scenario, a disconnected leader consistently receives request, its own log will grow simultaneously
	// but the term in its log would not change!! the object of our comparision is term!!
	last_index := len(rf.logs)
	last_term := rf.logs[last_index-1].Term
	// if split vote occurs, followers hove no way to update its voteFor
	// in case of this scenarios, follower eithor vote original candidate or wait for election timeout
	if args.CandidateTerm == rf.currentTerm && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(args.LastLogTerm > last_term || (args.LastLogTerm == last_term && args.LastLogIndex >= rf.localIndex2GlobalIndex(last_index, rf.lastIncludeIndex))) {
		rf.voteFor, rf.electionTimestamp = args.CandidateId, time.Now()
		reply.VoteGrant = true
		Debug(dVote, "S%d -> S%d with since: %v", rf.me, args.CandidateId, time.Since(rf.electionTimestamp))
	} else {
		reply.VoteGrant = false
	}
	reply.ReplyTerm = rf.currentTerm
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
	Debug(dLog, "S%d %v (T: %v, LII: %v, LIT: %v, log: %+v) receive AE from %v(T: %v, PLI: %v, PLT: %v, LC: %v)",
		rf.me, rf.state.String(), rf.currentTerm, rf.lastIncludeIndex, rf.lastIncludeTerm, rf.logs,
		args.LeaderId, args.LeaderTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
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

		if args.PrevLogIndex < rf.lastIncludeIndex {
			reply.Success = true
			reply.ReplyTerm = rf.currentTerm
			return
		}

		if args.PrevLogIndex >= rf.localIndex2GlobalIndex(len(rf.logs), rf.lastIncludeIndex) {
			reply.XTerm, reply.XIndex, reply.ReplyTerm = -1, rf.localIndex2GlobalIndex(len(rf.logs), rf.lastIncludeIndex), rf.currentTerm
			return
		}

		if (args.PrevLogIndex >= rf.localIndex2GlobalIndex(0, rf.lastIncludeIndex) || args.PrevLogIndex < rf.localIndex2GlobalIndex(len(rf.logs), rf.lastIncludeIndex)) &&
			(rf.logs[rf.globalIndex2LocalIndex(args.PrevLogIndex, rf.lastIncludeIndex)].Term != args.PrevLogTerm) {
			reply.XTerm, reply.XIndex, reply.ReplyTerm = rf.logs[rf.globalIndex2LocalIndex(args.PrevLogIndex, rf.lastIncludeIndex)].Term, -1, rf.currentTerm
			for i := 1; i < len(rf.logs); i++ {
				if rf.logs[i].Term == rf.logs[rf.globalIndex2LocalIndex(args.PrevLogIndex, rf.lastIncludeIndex)].Term {
					reply.XIndex = rf.localIndex2GlobalIndex(i, rf.lastIncludeIndex)
					break
				}
			}
			return
		}

		conflict_index, append_index := rf.globalIndex2LocalIndex(args.PrevLogIndex+1, rf.lastIncludeIndex), 0
		// conflict_index either is the end of logs or is the index where log entry is not matched with leader
		// append_index either is the end of args.Entries or is the index where following entries will be appended into follower
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
		if append_index < len(args.Entries) {
			rf.logs = append(rf.logs[:conflict_index], args.Entries[append_index:]...)
		}
		Debug(dLog, "S%d %v log len is: %v, entries len is: %v, CI: %v, LA: %v",
			rf.me, rf.state.String(), len(rf.logs), len(args.Entries), rf.commitIndex, rf.lastApplied)

		rf.persist()

		if rf.commitIndex < args.LeaderCommit {
			// we can simply set rf.commitIndex = args.LeaderCommit instead of min(args.LeaderCommit, len(rf.logs) - 1)
			// the reason is leader always send logs from nextIndex to THE END to follower, and LeaderCommit must lower than and equal to leader's log len
			// after follower replicates args.Entries, follower can own all logs from leader, so args.LeaderCommit must lower than and equal to len(rf.logs) - 1
			// rf.commitIndex = int(math.Min(float64(len(rf.logs)), float64(args.LeaderCommit)))
			rf.commitIndex = min(args.LeaderCommit, rf.localIndex2GlobalIndex(len(rf.logs), rf.lastIncludeIndex))
			rf.commitCond.Broadcast()
		}
		reply.Success = true
		rf.electionTimestamp = time.Now()
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	rf.mu.Lock()
	new_log := LogType{Command: command, Term: term, Index: rf.localIndex2GlobalIndex(len(rf.logs), rf.lastIncludeIndex)}
	rf.logs = append(rf.logs, new_log)
	rf.persist()
	index := rf.localIndex2GlobalIndex(len(rf.logs)-1, rf.lastIncludeIndex)
	Debug(dClient, "S%d %v receive a new log: %+v", rf.me, rf.state.String(), new_log)
	rf.mu.Unlock()

	rf.sendTrigger <- struct{}{}
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
	election_timeout := 250 + (rand.Int31() % 400)
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
			Debug(dVote, "S%d %v start a new election in term %v with since: %v, election_timeout: %v",
				rf.me, rf.state.String(), rf.currentTerm, time.Since(rf.electionTimestamp), time.Millisecond*time.Duration(election_timeout))
			rf.startElection(election_timeout)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 150
		// milliseconds.
		// ticker must lower than election timeout
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) retryRequestVote(obj, retry_cnt, retry_interval int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ch := make([]chan RequestVoteReply, retry_cnt)
	fun := func(i int) {
		save_reply := RequestVoteReply{}
		Debug(dVote, "S%d %v send RV(retry) to %v", rf.me, rf.state.String(), obj)
		if ok := rf.sendRequestVote(obj, args, &save_reply); ok {
			ch[i] <- save_reply
		}
	}
	for i := 0; i < retry_cnt; i++ {
		go fun(i)
		select {
		case ret := <-ch[i]:
			Debug(dVote, "S%d %v retry to %v success", rf.me, rf.state.String(), obj)
			reply.ReplyTerm = ret.ReplyTerm
			reply.VoteGrant = ret.VoteGrant
			return true
		case <-time.After(time.Millisecond * time.Duration(retry_interval)):
			continue
		}
	}
	return false
}

func (rf *Raft) startElection(election_timeout int32) {
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
			last_index := len(rf.logs)
			last_term := rf.logs[last_index-1].Term
			save_last_include_index := rf.lastIncludeIndex
			rf.mu.Unlock()
			args := RequestVoteArgs{
				CandidateTerm: start_term,
				CandidateId:   rf.me,
				LastLogIndex:  rf.localIndex2GlobalIndex(last_index, save_last_include_index),
				LastLogTerm:   last_term}

			reply := RequestVoteReply{}

			retry_cnt := 5
			retry_interval := 50
			Debug(dVote, "S%d %v send RV(first) to %v", rf.me, rf.state.String(), idx)
			success := rf.sendRequestVote(idx, &args, &reply)
			if !success {
				success = rf.retryRequestVote(idx, retry_cnt, retry_interval, &args, &reply)
			}
			rf.mu.Lock()
			if success {
				// state may have changed by other startElection instance
				// if we omit this judgement, the term may be decreased (start_term is 2, rf.currentTerm is 5 and reply.ReplyTerm is 4)
				if rf.state != Candidate || time.Since(rf.electionTimestamp) >= time.Millisecond*time.Duration(election_timeout) {
					rf.mu.Unlock()
					return
				}
				if reply.ReplyTerm > start_term {
					rf.convertFollower(reply.ReplyTerm)
					rf.persist()
					rf.mu.Unlock()
					return
				} else if reply.ReplyTerm == start_term && reply.VoteGrant {
					// only reply term match with start term, the following code can be executed
					// because if current candidate becomes leader, all follower should have the same term
					atomic.AddInt32(&vote, 1)
					if val := atomic.LoadInt32(&vote); int(val) >= (rf.cnt+1)/2 {
						if rf.state == Leader {
							rf.mu.Unlock()
							return
						}
						Debug(dVote, "S%d %v become leader in term %v", rf.me, rf.state.String(), rf.currentTerm)
						rf.startLeader()
						rf.mu.Unlock()
						return
					}
				}
			}
			rf.mu.Unlock()
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
		Debug(dWarn, "S%d %v startleader with log len: %v", rf.me, rf.state, len(rf.logs))
		rf.heartbeatMessage()
		heartbeat_timeout := 150
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

func (rf *Raft) retryEmitInstallSnapshot(obj, retry_cnt, retry_interval int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ch := make([]chan InstallSnapshotReply, retry_cnt)
	fun := func(i int) {
		save_reply := InstallSnapshotReply{}
		Debug(dLeader, "S%d %v(term: %v) send IS(retry) to %v with lastIncludedIndex: %v, lastIncludeTerm: %v", rf.me, rf.state.String(), rf.currentTerm, obj, rf.lastIncludeIndex, rf.lastIncludeTerm)
		if ok := rf.sendInstallSnapshot(obj, args, &save_reply); ok {
			ch[i] <- save_reply
		}
	}
	for i := 0; i < retry_cnt; i++ {
		go fun(i)
		select {
		case ret := <-ch[i]:
			reply.ReplyTerm = ret.ReplyTerm
			return true
		case <-time.After(time.Millisecond * time.Duration(retry_interval)):
			continue
		}
	}
	return false
}

func (rf *Raft) leaderEmitInstallSnapshot(obj int) {
	rf.mu.Lock()
	start_term := rf.currentTerm
	start_last_include_index := rf.lastIncludeIndex
	start_last_include_term := rf.lastIncludeTerm
	start_data := make([]byte, len(rf.snapshot))
	copy(start_data, rf.snapshot)
	rf.mu.Unlock()

	args := InstallSnapshotArgs{
		LeaderTerm:       start_term,
		LeaderId:         rf.me,
		LastIncludeIndex: start_last_include_index,
		LastIncludeTerm:  start_last_include_term,
		Data:             start_data,
	}
	reply := InstallSnapshotReply{}
	retry_cnt := 5
	retry_interval := 30
	Debug(dLeader, "S%d %v(T: %v) send IS(first) to %v with LII: %v, LIT: %v", rf.me, rf.state.String(), rf.currentTerm, obj, rf.lastIncludeIndex, rf.lastIncludeTerm)
	success := rf.sendInstallSnapshot(obj, &args, &reply)
	if !success {
		success = rf.retryEmitInstallSnapshot(obj, retry_cnt, retry_interval, &args, &reply)
	}
	if success {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if reply.ReplyTerm > rf.currentTerm {
			rf.convertFollower(reply.ReplyTerm)
			rf.persist()
		} else if reply.ReplyTerm == rf.currentTerm {
			Debug(dLeader, "S%d %v(T: %v) NI[%v] from %v to %v", rf.me, rf.state.String(), rf.currentTerm, obj, rf.nextIndex[obj], rf.lastIncludeIndex+1)
			rf.nextIndex[obj] = start_last_include_index + 1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) retryHeartbeatMessage(obj, retry_cnt, retry_interval int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ch := make([]chan AppendEntriesReply, retry_cnt)
	fun := func(i int) {
		save_reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(obj, args, &save_reply); ok {
			ch[i] <- save_reply
		}
	}
	for i := 0; i < retry_cnt; i++ {
		go fun(i)
		select {
		case ret := <-ch[i]:
			reply.ReplyTerm = ret.ReplyTerm
			reply.Success = ret.Success
			reply.XIndex = ret.XIndex
			reply.XTerm = ret.XTerm
			return true
		case <-time.After(time.Millisecond * time.Duration(retry_interval)):
			continue
		}
	}
	return false
}

func (rf *Raft) heartbeatMessage() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	start_term := rf.currentTerm
	// once heartbeatMessage was called, rf.cnt - 1 goroutine will be created, and will send rf.cnt - 1 times into rf.sendTrigger
	// as long as rf.sendTrigger has content, heartbeatMessage will be called, and more and more goroutine will be create
	// one heartbeatMessage should match once rf.sendTrigger
	rf.mu.Unlock()
	for i := 0; i < rf.cnt; i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			rf.mu.Lock()
			save_last_include_index := rf.lastIncludeIndex
			nxt_idx := rf.globalIndex2LocalIndex(rf.nextIndex[idx], save_last_include_index)
			Debug(dLeader, "S%d %v has nxt_idx: %v, nextIndex[%v]: %v, LII: %v", rf.me, rf.state.String(), nxt_idx, idx, rf.nextIndex[idx], rf.lastIncludeIndex)
			if rf.localIndex2GlobalIndex(nxt_idx, save_last_include_index) <= rf.lastIncludeIndex {
				Debug(dLeader, "S%v %s prepare to send IS to %v with nxt_idx: %v, LII: %v", rf.me, rf.state.String(), idx, nxt_idx, rf.lastIncludeIndex)
				rf.mu.Unlock()
				rf.leaderEmitInstallSnapshot(idx)
				return
			}
			// equal is not allowed!! we should send heartbeat message with empty entries
			if nxt_idx > len(rf.logs) {
				Debug(dLeader, "S%d %v prepare to send AE to %v but nxt_idx: %v beyond log len: %v", rf.me, rf.state.String(), idx, nxt_idx, len(rf.logs))
				// during normal execution, this scenario wasn't occurred
				// suppose rf.nextIndex[1] = 15, len(rf.logs) = 20, and log len of server 1 is 15
				// we also suppose current leader's logs are not applied)
				// at some time, current leader step down(due to a higher term), and after a while, it's elected a leader again
				// during this interval, current leader's log may be truncated by another leader, so its log len is lower than rf.nextIndex[1]
				rf.mu.Unlock()
				return
			}
			entries := make([]LogType, len(rf.logs[nxt_idx:]))
			copy(entries, rf.logs[nxt_idx:])
			save_commit_index := rf.commitIndex
			prev_index := nxt_idx - 1
			prev_term := rf.logs[prev_index].Term
			rf.mu.Unlock()
			args := AppendEntriesArgs{
				LeaderTerm:   start_term,
				LeaderId:     rf.me,
				Entries:      entries,
				PrevLogIndex: rf.localIndex2GlobalIndex(prev_index, save_last_include_index),
				PrevLogTerm:  prev_term,
				LeaderCommit: save_commit_index,
			}
			reply := AppendEntriesReply{}
			retry_cnt := 5
			retry_interval := 30
			Debug(dLeader, "S%d %v(T: %v) send AE(first) to %v, log len: %v, PLI: %v, PLT: %v, LC: %v",
				rf.me, rf.state.String(), rf.currentTerm, idx, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			success := rf.sendAppendEntries(idx, &args, &reply)
			if !success {
				success = rf.retryHeartbeatMessage(idx, retry_cnt, retry_interval, &args, &reply)
			}
			rf.mu.Lock()
			if success {
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				Debug(dLeader, "S%d %v receives AE reply from %v with success: %v", rf.me, rf.state.String(), idx, reply.Success)
				if reply.ReplyTerm > start_term {
					rf.convertFollower(reply.ReplyTerm)
					rf.persist()
					rf.mu.Unlock()
					return
				} else if reply.ReplyTerm == start_term {
					if reply.Success {
						// we unlock when sending RPC, so, during this time, rf.matchIndex may be updated
						// we should record rf.commitIndex when we come back, and than we judge whether rf.commitIndex can be updated
						save_commit_index = rf.commitIndex
						rf.nextIndex[idx] = rf.localIndex2GlobalIndex(nxt_idx, save_last_include_index) + len(entries)
						rf.matchIndex[idx] = rf.nextIndex[idx] - 1
						for i := rf.commitIndex + 1; i < rf.localIndex2GlobalIndex(len(rf.logs), save_last_include_index); i++ {
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
							rf.commitCond.Broadcast()
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
						if reply.XTerm != -1 {
							nxt_idx = -1
							for i := len(rf.logs) - 1; i >= 1; i-- {
								if rf.logs[i].Term == reply.XTerm {
									nxt_idx = rf.localIndex2GlobalIndex(i, save_last_include_index)
									break
								}
							}
							if nxt_idx != -1 {
								rf.nextIndex[idx] = nxt_idx + 1
							} else {
								rf.nextIndex[idx] = reply.XIndex
							}
						} else {
							rf.nextIndex[idx] = reply.XIndex
						}
					}
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.lastApplied < rf.commitIndex) {
			rf.commitCond.Wait()
		}
		start_lastApplied := rf.lastApplied
		if rf.globalIndex2LocalIndex(start_lastApplied+1, rf.lastIncludeIndex) < 0 || rf.globalIndex2LocalIndex(rf.commitIndex+1, rf.lastIncludeIndex) > len(rf.logs) {
			start_lastApplied = rf.lastIncludeIndex
		}
		entries := make([]LogType, len(rf.logs[rf.globalIndex2LocalIndex(start_lastApplied+1, rf.lastIncludeIndex):rf.globalIndex2LocalIndex(rf.commitIndex+1, rf.lastIncludeIndex)]))
		copy(entries, rf.logs[rf.globalIndex2LocalIndex(start_lastApplied+1, rf.lastIncludeIndex):rf.globalIndex2LocalIndex(rf.commitIndex+1, rf.lastIncludeIndex)])
		can_apply := false

		if rf.state == Leader {
			for i := len(rf.logs) - 1; i >= 1; i-- {
				if rf.logs[i].Term == rf.currentTerm && rf.localIndex2GlobalIndex(i, rf.lastIncludeIndex) <= rf.commitIndex {
					can_apply = true
					break
				}
			}
		}

		if rf.state != Leader || (rf.state == Leader && can_apply) {
			Debug(dCommit, "S%d %v LA updates from %v to %v, entries: %+v", rf.me, rf.state.String(), rf.lastApplied, rf.commitIndex, entries)
			rf.lastApplied = rf.commitIndex
			for i, log := range entries {
				msg := ApplyMsg{CommandValid: true, Command: log.Command, CommandIndex: start_lastApplied + i + 1}
				// rf.logDebuger <- DebugMsg{apply: msg, msg: rf.state.String()}
				Debug(dCommit, "S%d %v commit log: %+v at index: %v, start_lastApplied: %v", rf.me, rf.state.String(), log, start_lastApplied+i+1, start_lastApplied)

				rf.tester <- msg
			}
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
		sendTrigger:       make(chan struct{}, 1024),
		batchTimestamp:    time.Now(),
		logDebuger:        make(chan DebugMsg, 1024),
		snapshot:          nil,
		lastIncludeIndex:  0,
		lastIncludeTerm:   0,
	}

	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	rf.lastApplied = rf.lastIncludeIndex
	rf.commitIndex = rf.lastIncludeIndex

	for i := 0; i < rf.cnt; i++ {
		rf.nextIndex[i] = rf.lastIncludeIndex + 1
		rf.matchIndex[i] = rf.lastIncludeIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()
	// go rf.WriteLog()

	return rf
}
