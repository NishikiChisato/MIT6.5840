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

	// leader election
	cnt                int         // number of servers
	state              ServerState // state of server
	current_term       int         // server's current term, in each RPC, it should be updated
	vote_for           int         // server id of this follower vote for
	election_timestamp time.Time   // timestamp for leader election

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

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	var rf_persist PersistType
	rf_persist.Current_Term = rf.current_term
	rf_persist.Vote_For = rf.vote_for
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
		rf.current_term = content.Current_Term
		rf.vote_for = content.Vote_For
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
	Candidate_term int // candidate term
	Candidate_Id   int // candidate id
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

	DPrintf(rf.me, "[RV] [args]: %+v, [term, vote for]: (%v, %v)", args, rf.current_term, rf.vote_for)
	if rf.current_term < args.Candidate_term {
		rf.convertFollower(args.Candidate_term)
	}
	if args.Candidate_term == rf.current_term && (rf.vote_for == -1 || rf.vote_for == args.Candidate_Id) {
		rf.vote_for, rf.election_timestamp = args.Candidate_Id, time.Now()
		reply.Vote_Grant = true
	} else {
		reply.Vote_Grant = false
	}
	reply.Reply_Term = rf.current_term
}

type AppendEntriesArgs struct {
	Leader_term int        // leader's term
	Leader_id   int        // leader id
	Entries     []*LogType // log struct, index 0 is the previous log entry, the remaining is new log entries
}

type AppendEntriesReply struct {
	Reply_term int  // server's term, used for leader to update its own term(but it will revert to follower if condition satified)
	Success    bool // indicate success of current RPC(true is success)
	// to mark whether current leader is outdated
	// the leader is outdated: term is lower or log is outdated
	Xterm  int // for fast recovery
	XIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(rf.me, "[AE] [args]: %+v, [term]: %v", args, rf.current_term)
	// if leader own higher term, no matter what state the server is, should convert to follower
	if rf.current_term < args.Leader_term {
		rf.convertFollower(args.Leader_term)
	}
	if rf.current_term == args.Leader_term {
		// state is candidate but term is the same as leader
		if rf.state != Follower {
			rf.convertFollower(args.Leader_term)
		}
		if len(args.Entries) == 0 {
			rf.election_timestamp = time.Now()
			reply.Success = true
		}
	}
	reply.Reply_term = rf.current_term
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

	return 0, term, isLeader
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
	start_term := rf.current_term
	for !rf.killed() {

		// Your code here (2A)

		rf.mu.Lock()
		// ticker only allow follower or candidate to run
		if rf.state != Follower && rf.state != Candidate {
			DPrintf(rf.me, "state change into %v in ticker, current ticker return", rf.state.String())
			rf.mu.Unlock()
			return
		}

		if start_term != rf.current_term {
			DPrintf(rf.me, "term change into %v in ticker, current ticker return", start_term)
			rf.mu.Unlock()
			return
		}

		if time.Since(rf.election_timestamp) > time.Millisecond*time.Duration(election_timeout) {
			DPrintf(rf.me, "start election in term: %v", rf.current_term)
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.vote_for = rf.me
	rf.current_term++
	rf.election_timestamp = time.Now()

	// during the execution of this function, the term of current server may be changed (multiple instance executed in background)
	// if one instance change this server's state, its term will grow at the same time.
	// we should use the initial term when starts this function to compare with reply term instead of current term
	start_term := rf.current_term
	vote := int32(1)
	for i := 0; i < rf.cnt; i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			args := RequestVoteArgs{
				Candidate_term: rf.current_term,
				Candidate_Id:   rf.me}
			reply := RequestVoteReply{}

			if rf.sendRequestVote(idx, &args, &reply) {
				rf.mu.Lock()
				// state may have changed by other startElection instance
				// if we omit this judgement, the term may be decreased (start_term is 2, rf.current_term is 5 and reply.Reply_Term is 4)
				if rf.state != Candidate {
					DPrintf(rf.me, "state not matchs with candidate in startElection")
					rf.mu.Unlock()
					return
				}
				if reply.Reply_Term > start_term {
					rf.convertFollower(reply.Reply_Term)
					DPrintf(rf.me, "reply term is greater than candidate's term, convert to follower in startElection")
					rf.mu.Unlock()
					return
				} else if reply.Reply_Term == start_term && reply.Vote_Grant {
					// only reply term match with start term, the following code can be executed
					// because if current candidate becomes leader, all follower should have the same term
					atomic.AddInt32(&vote, 1)
					if val := atomic.LoadInt32(&vote); int(val) >= (rf.cnt+1)/2 {
						DPrintf(rf.me, "become leader with votes %v", val)
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
	rf.current_term = new_term
	rf.vote_for = -1
	rf.election_timestamp = time.Now()
	go rf.ticker()
}

func (rf *Raft) startLeader() {
	rf.state = Leader
	go func() {
		heartbeat_timeout := 100
		heartbeat_ticker := time.NewTicker(time.Millisecond * time.Duration(heartbeat_timeout))
		defer heartbeat_ticker.Stop()
		for {
			rf.heartbeatMessage()
			<-heartbeat_ticker.C
			rf.mu.Lock()
			if rf.state != Leader {
				DPrintf(rf.me, "state changed in startLeader")
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()

}

func (rf *Raft) heartbeatMessage() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	start_term := rf.current_term
	rf.mu.Unlock()
	for i := 0; i < rf.cnt; i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			args := AppendEntriesArgs{
				Leader_term: rf.current_term,
				Leader_id:   rf.me}
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(idx, &args, &reply) {
				if rf.state != Leader {
					DPrintf(rf.me, "heartbeat routine %v in heartbeatMessage return due to state changed", idx)
					return
				}
				if reply.Reply_term > start_term {
					rf.convertFollower(reply.Reply_term)
					DPrintf(rf.me, "leader's term is outdated in heartbeatMessage, convert to follower")
					return
				}
			}
		}(i)
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

	rf.cnt = len(peers)
	rf.current_term = 0
	rf.state = Follower
	rf.election_timestamp = time.Now()
	rf.vote_for = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
