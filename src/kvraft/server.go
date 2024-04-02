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
	RetValue string
	ErrMsg   Err
}

type KVInterface interface {
	Get(key string) (string, Err)
	Put(key string, val string) Err
	Append(key string, val string) Err
	Copy() map[string]string
}

type InternalKV struct {
	db map[string]string
}

func NewInternalKV() *InternalKV {
	return &InternalKV{
		db: make(map[string]string),
	}
}

func (ikv *InternalKV) Get(key string) (string, Err) {
	if val, ok := ikv.db[key]; !ok {
		return "", ErrNoKey
	} else {
		return val, OK
	}
}

func (ikv *InternalKV) Put(key string, val string) Err {
	ikv.db[key] = val
	return OK
}

func (ikv *InternalKV) Append(key string, val string) Err {
	ikv.db[key] += val
	return OK
}

func (ikv *InternalKV) Copy() map[string]string {
	ret := make(map[string]string)
	for key, val := range ikv.db {
		ret[key] = val
	}
	return ret
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	ikv KVInterface

	// used to deduplicate operation
	cliSeqNumber map[int64]int
	cliHistory   map[int64]string
	opChan       map[int]*chan OperationResult

	persister *raft.Persister
	// for all msg.commitIndex <= snapshotIndex, log entries are stored in snapshot
	snapshotIndex int

	getCond sync.Cond
	doGet   bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	DebugPrintf(dInfo, "S%d receive Get(seq: %v, key: %v)", kv.me, args.SeqNumber, args.Key)
	rerr := kv.rf.ReadStart()

	if rerr != OK {
		DebugPrintf(dError, "S%d with cmd(Get: key: %v, seq: %v) return due to %v", kv.me, args.Key, args.SeqNumber, rerr)
		reply.Err = Err(rerr)
		return
	}

	// the speed of commitment in Raft layer is faster than the speed of processing log in KV Store layer
	// we specify the distence which is less than 10 is satified(in some rare scenario, error may occur, though)
	kv.mu.RLock()
	for !kv.doGet {
		kv.getCond.Wait()
	}
	val, err := kv.ikv.Get(args.Key)
	kv.mu.RUnlock()

	reply.Value = val
	reply.Err = err
	DebugPrintf(dInfo, "S%d respond cmd(Get: key: %v, seq: %v)", kv.me, args.Key, args.SeqNumber)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if args.SeqNumber <= kv.cliSeqNumber[args.Identifier] {
		reply.Err = ErrCmdExist
		kv.mu.Unlock()
		return
	}

	cmd := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		Identifier: args.Identifier,
		SeqNumber:  args.SeqNumber,
	}
	kv.mu.Unlock()

	idx, term, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DebugPrintf(dInfo, "S%d start cmd(%v: key: %v, val: %v, seq: %v) at term: %v", kv.me, args.Op, args.Key, args.Value, args.SeqNumber, term)

	kv.mu.Lock()
	ch := make(chan OperationResult)
	kv.opChan[idx] = &ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		close(*kv.opChan[idx])
		delete(kv.opChan, idx)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		DebugPrintf(dLog, "S%d(term: %v) receive result(seq: %v) from channle", kv.me, term, cmd.SeqNumber)
		reply.Err = res.ErrMsg
	case <-time.After(time.Millisecond * time.Duration(300)):
		reply.Err = ErrNotMajority
	}
}

func (kv *KVServer) getCondition() {
	for !kv.killed() {
		if !kv.doGet && len(kv.applyCh) < 1 {
			DebugPrintf(dInfo, "S%d len: %v\n", kv.me, len(kv.applyCh))
			kv.mu.Lock()
			kv.doGet = true
			kv.mu.Unlock()

			kv.getCond.Broadcast()
		} else if kv.doGet && len(kv.applyCh) >= 1 {
			DebugPrintf(dInfo, "S%d len: %v\n", kv.me, len(kv.applyCh))
			kv.mu.Lock()
			kv.doGet = false
			kv.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 25)
	}
}

func (kv *KVServer) readFromRaft() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			if msg.Command == nil {
				continue
			}
			DebugPrintf(dCommit, "S%d receive msg(seq: %v, ide: %v) from raft", kv.me, msg.Command.(Op).SeqNumber, msg.Command.(Op).Identifier)
			cmd := msg.Command.(Op)
			result := OperationResult{}
			// since raft log doesn't strictly map to operations applied to KVServer, so we should pre-judge whether the current log is applied to KVServer or not
			// we should ignore log entries with index lower than kv.snapshotIndex, since these log entries must exist in snapshot
			kv.mu.Lock()
			if cmd.SeqNumber > kv.cliSeqNumber[cmd.Identifier] && msg.CommandIndex > kv.snapshotIndex {
				switch cmd.Type {
				case "Put":
					result.ErrMsg = kv.ikv.Put(cmd.Key, cmd.Value)
				case "Append":
					result.ErrMsg = kv.ikv.Append(cmd.Key, cmd.Value)
				}

				kv.cliSeqNumber[cmd.Identifier] = cmd.SeqNumber
				kv.cliHistory[cmd.Identifier] = result.RetValue

				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
					// we cannot trim raft log at msg.CommandIndex, since there is a scenario that somehow one follower may not replicate log with index of msg.CommandIndex
					// in the case of this scenario, that follower may not receive that log entry forever
					threshold := 1
					kv.snapshot(msg.CommandIndex - threshold)
					kv.snapshotIndex = max(kv.snapshotIndex, msg.CommandIndex-threshold)
				}
			}

			if kv.opChan[msg.CommandIndex] != nil {
				DebugPrintf(dLeader, "S%d send cmd(seq: %v) to channel", kv.me, cmd.SeqNumber)

				*kv.opChan[msg.CommandIndex] <- result
			} else {
				DebugPrintf(dError, "S%d(replicate side) channel close", kv.me)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			DebugPrintf(dCommit, "S%d receive snapshot from raft", kv.me)
			// when leader emit IS to follower, follower should read from that snapshot to rebuild its local database
			kv.mu.Lock()
			kv.readRaftSnapshot(msg.Snapshot)
			kv.snapshotIndex = max(kv.snapshotIndex, msg.SnapshotIndex)
			kv.mu.Unlock()
		}

	}
}

type PersistType struct {
	Db           map[string]string
	CliSeqNumber map[int64]int
	CliHistory   map[int64]string
}

func (kv *KVServer) snapshot(idx int) {
	var snapshot PersistType

	snapshot.Db = kv.ikv.Copy()

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
	if data == nil || len(data) < 1 {
		DebugPrintf(dError, "S%d raft snapshot is nill", kv.me)
		return
	}
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var snapshot PersistType
	if dec.Decode(&snapshot) == nil {
		for key, val := range snapshot.Db {
			kv.ikv.Put(key, val)
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

	kv.applyCh = make(chan raft.ApplyMsg, 10240)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.ikv = NewInternalKV()
	kv.cliSeqNumber = make(map[int64]int)
	kv.opChan = make(map[int]*chan OperationResult)
	kv.cliHistory = make(map[int64]string)
	kv.readRaftSnapshot(persister.ReadSnapshot())
	kv.getCond = *sync.NewCond(kv.mu.RLocker())

	go kv.readFromRaft()
	go kv.getCondition()

	return kv
}
