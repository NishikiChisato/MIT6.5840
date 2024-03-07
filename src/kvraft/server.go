package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       string // Put, Append, Get
	Key        string
	Value      string
	SeqNumber  int
	Identifier int64
}

// record result for each operation to handles duplicated operation
type OperationResult struct {
	Ret  string
	Term int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string]string
	// used to deduplicate operation
	cliSeqNumber map[int64]int
	// used to wake up corresponding goroutine
	// maybe sync.Map
	// to address deplicated request
	// clerk may issue deplicate request somehow(during leader election), the effect of duplicated request must equivalent to the effect of this request first execute
	// for example, if deplicated Get("key") arrives for one clerk, but during the interval of two Get("key"), another clerk update the value of "key"
	// now, the two Get("key") operations should return identical result
	// the same as Get operation, deplicated Put or Append should only executes once
	cliHistory map[int64]string

	opChan      map[int]*chan OperationResult
	CommittedCh chan raft.ApplyMsg

	persister *raft.Persister
	// for all msg.commitIndex <= snapshotIndex, log entries are stored in snapshot
	snapshotIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	if args.SeqNumber <= kv.cliSeqNumber[args.Identifier] {
		reply.Err = ErrCmdExist
		reply.Value = kv.cliHistory[args.Identifier]
		kv.mu.Unlock()
		DebugPrintf(dWarn, "S%d(server side) cmd exists", kv.me)
		return
	}

	cmd := Op{
		Type:       "Get",
		Key:        args.Key,
		Identifier: args.Identifier,
		SeqNumber:  args.SeqNumber,
	}

	idx, term, isLeader := kv.rf.Start(cmd)
	DebugPrintf(dLeader, "S%d start Get with seq: %v, idx: %v(rfat sz: %v, snapshot sz: %v)",
		kv.me, cmd.SeqNumber, idx, kv.persister.RaftStateSize(), kv.persister.SnapshotSize())

	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		DebugPrintf(dWarn, "S%d(server side after start) is wrong leader", kv.me)
		return
	}

	kv.opChan[idx] = new(chan OperationResult)
	*kv.opChan[idx] = make(chan OperationResult)
	ch := kv.opChan[idx]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		close(*kv.opChan[idx])
		delete(kv.opChan, idx)
		kv.mu.Unlock()
	}()

	select {
	case res := <-*ch:
		if res.Term == term {
			reply.Err = OK
			reply.Value = res.Ret
		} else {
			reply.Err = ErrNotMajority
			DebugPrintf(dError, "cmd: %+v(term: %v) leader outdated with term: %v", cmd, term, res.Term)
		}
	case <-time.After(time.Millisecond * time.Duration(1000)):
		reply.Err = ErrNotMajority
		DebugPrintf(dError, "cmd: %+v timeout", cmd)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if args.SeqNumber <= kv.cliSeqNumber[args.Identifier] {
		reply.Err = ErrCmdExist
		kv.mu.Unlock()
		DebugPrintf(dWarn, "S%d(server side) cmd exists", kv.me)
		return
	}

	cmd := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		Identifier: args.Identifier,
		SeqNumber:  args.SeqNumber,
	}

	idx, term, isLeader := kv.rf.Start(cmd)
	DebugPrintf(dLeader, "S%d start %v with seq: %v, idx: %v(raft state sz: %v, snapshot sz: %v)",
		kv.me, cmd.Type, cmd.SeqNumber, idx, kv.persister.RaftStateSize(), kv.persister.SnapshotSize())

	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		DebugPrintf(dWarn, "S%d(server side after start) is wrong leader", kv.me)
		return
	}

	kv.opChan[idx] = new(chan OperationResult)
	*kv.opChan[idx] = make(chan OperationResult)
	ch := kv.opChan[idx]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		close(*kv.opChan[idx])
		delete(kv.opChan, idx)
		kv.mu.Unlock()
	}()

	select {
	case res := <-*ch:
		if res.Term == term {
			reply.Err = OK

		} else {
			reply.Err = ErrNotMajority
			DebugPrintf(dError, "cmd: %+v(term: %v) leader outdated with term: %v", cmd, term, res.Term)
		}
	case <-time.After(time.Millisecond * time.Duration(1000)):
		reply.Err = ErrNotMajority
		DebugPrintf(dError, "cmd: %+v timeout", cmd)
	}
}

func (kv *KVServer) forward() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			kv.CommittedCh <- msg
		}
	}
}

func (kv *KVServer) readFromRaft() {
	for !kv.killed() {
		msg := <-kv.CommittedCh
		kv.mu.Lock()

		if msg.CommandValid {
			DebugPrintf(dLog, "S%d(replicate side) receive cmd: %+v from applyCh len: %v, committedCh len: %v", kv.me, msg.Command.(Op), len(kv.applyCh), len(kv.CommittedCh))
			cmd := msg.Command.(Op)

			result := OperationResult{}
			// since raft log doesn't strictly map to operations applied to KVServer, so we should pre-judge whether the current log is applied to KVServer or not
			// we should ignore log entries with index lower than kv.snapshotIndex, since these log entries must exist in snapshot
			if cmd.SeqNumber <= kv.cliSeqNumber[cmd.Identifier] || msg.CommandIndex <= kv.snapshotIndex {
				kv.mu.Unlock()
				continue
			}
			switch cmd.Type {
			case "Get":
				result.Ret = kv.db[cmd.Key]
			case "Put":
				kv.db[cmd.Key] = cmd.Value
			case "Append":
				kv.db[cmd.Key] += cmd.Value
			}

			// as long as raft committs a log, we can apply corresponding operations to KVServer
			// this is because raft server as consensus modul to make consistence for log accross multiple KVServer

			kv.cliSeqNumber[cmd.Identifier] = cmd.SeqNumber
			kv.cliHistory[cmd.Identifier] = kv.db[cmd.Key]

			term, isLeader := kv.rf.GetState()

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				// we cannot trim raft log at msg.CommandIndex, since there is a scenario that somehow one follower may not replicate log with index of msg.CommandIndex
				// in the case of this scenario, that follower may not receive that log entry forever
				threshold := 1
				kv.snapshot(msg.CommandIndex - threshold)
				kv.snapshotIndex = max(kv.snapshotIndex, threshold)
			}

			if isLeader {
				DebugPrintf(dLog, "S%d(replicate side) is leader", kv.me)
				if kv.opChan[msg.CommandIndex] != nil && *kv.opChan[msg.CommandIndex] != nil {
					DebugPrintf(dCommit, "S%d(replicate side) sending channel with seq: %v", kv.me, cmd.SeqNumber)
					result.Term = term
					*kv.opChan[msg.CommandIndex] <- result
					DebugPrintf(dCommit, "S%d(replicate side) channel sending done with seq: %v", kv.me, cmd.SeqNumber)
				} else {
					DebugPrintf(dError, "S%d(replicate side) channel close", kv.me)
				}
			}
		} else if msg.SnapshotValid {
			// when leader emit IS to follower, follower should read from that snapshot to rebuild its local database
			kv.readRaftSnapshot(msg.Snapshot)
			kv.snapshotIndex = max(kv.snapshotIndex, msg.SnapshotIndex)
		}
		kv.mu.Unlock()
	}
}

type PersistType struct {
	Db           map[string]string
	CliSeqNumber map[int64]int
	CliHistory   map[int64]string
}

func (kv *KVServer) snapshot(idx int) {
	var snapshot PersistType

	snapshot.Db = make(map[string]string)
	for key, val := range kv.db {
		snapshot.Db[key] = val
	}

	snapshot.CliSeqNumber = make(map[int64]int)
	for key, val := range kv.cliSeqNumber {
		snapshot.CliSeqNumber[key] = val
	}

	snapshot.CliHistory = make(map[int64]string)
	for key, val := range kv.cliHistory {
		snapshot.CliHistory[key] = val
	}

	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(&snapshot)

	DebugPrintf(dPersist, "S%d persist state at %v(rf state sz: %v)", kv.me, idx, kv.persister.RaftStateSize())
	kv.rf.SyncSnapshot(idx, buf.Bytes())
}

func (kv *KVServer) readRaftSnapshot(data []byte) {
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var snapshot PersistType
	if dec.Decode(&snapshot) == nil {
		for key, val := range snapshot.Db {
			kv.db[key] = val
		}

		for key, val := range snapshot.CliSeqNumber {
			kv.cliSeqNumber[key] = val
		}

		for key, val := range snapshot.CliHistory {
			kv.cliHistory[key] = val
		}
		DebugPrintf(dPersist, "S%d recover from crash ", kv.me)
	} else {
		DebugPrintf(dError, "S%d read from snapshot error", kv.me)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.cliSeqNumber = make(map[int64]int)
	kv.opChan = make(map[int]*chan OperationResult)
	kv.CommittedCh = make(chan raft.ApplyMsg, 10240)
	kv.cliHistory = make(map[int64]string)

	kv.readRaftSnapshot(persister.ReadSnapshot())

	go kv.readFromRaft()
	go kv.forward()

	return kv
}
