package kvraft

import (
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
	op  Op
	ret string
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
	// used to track history of operation of each client
	cliHistory map[int64]map[int]*OperationResult
	// used to deduplicate operation
	cliSeqNumber map[int64]map[int]bool
	// used to wake up corresponding goroutine
	// maybe sync.Map
	opChan      map[int]*chan OperationResult
	CommittedCh chan raft.ApplyMsg
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		DebugPrintf(dWarn, "S%d(server side) is wrong leader", kv.me)
		return
	}

	if kv.cliSeqNumber[args.Identifier] != nil && kv.cliSeqNumber[args.Identifier][args.SeqNumber] {
		reply.Err = ErrCmdExist
		reply.Value = kv.cliHistory[args.Identifier][args.SeqNumber].ret
		kv.mu.Unlock()
		DebugPrintf(dWarn, "S%d(server side) cmd exists", kv.me)
		return
	}

	if kv.cliSeqNumber[args.Identifier] == nil {
		kv.cliSeqNumber[args.Identifier] = make(map[int]bool)
	}
	kv.cliSeqNumber[args.Identifier][args.SeqNumber] = true

	cmd := Op{
		Type:       "Get",
		Key:        args.Key,
		Identifier: args.Identifier,
		SeqNumber:  args.SeqNumber,
	}

	idx, _, isLeader := kv.rf.Start(cmd)
	DebugPrintf(dLeader, "S%d start Get with seq: %v, idx: %v", kv.me, cmd.SeqNumber, idx)

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
		reply.Err = OK
		reply.Value = res.ret
		if kv.cliHistory[cmd.Identifier] == nil {
			kv.cliHistory[cmd.Identifier] = make(map[int]*OperationResult)
		}
		kv.cliHistory[cmd.Identifier][cmd.SeqNumber] = &res
	case <-time.After(time.Second * time.Duration(5)):
		reply.Err = ErrNotMajority
		DebugPrintf(dError, "cmd: %+v timeout", cmd)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		DebugPrintf(dWarn, "S%d(server side) is wrong leader", kv.me)
		return
	}

	if kv.cliSeqNumber[args.Identifier] != nil && kv.cliSeqNumber[args.Identifier][args.SeqNumber] {
		reply.Err = ErrCmdExist
		kv.mu.Unlock()
		DebugPrintf(dWarn, "S%d(server side) cmd exists", kv.me)
		return
	}

	if kv.cliSeqNumber[args.Identifier] == nil {
		kv.cliSeqNumber[args.Identifier] = make(map[int]bool)
	}
	kv.cliSeqNumber[args.Identifier][args.SeqNumber] = true

	cmd := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		Identifier: args.Identifier,
		SeqNumber:  args.SeqNumber,
	}

	idx, _, isLeader := kv.rf.Start(cmd)
	DebugPrintf(dLeader, "S%d start %v with seq: %v, idx: %v", kv.me, cmd.Type, cmd.SeqNumber, idx)

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
	case <-*ch:
		reply.Err = OK
	case <-time.After(time.Second * time.Duration(5)):
		reply.Err = ErrNotMajority
		DebugPrintf(dError, "cmd: %+v timeout", cmd)
	}
}

func (kv *KVServer) forward() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			DebugPrintf(dWarn, "S%d receives cmd: %+v at: %v", kv.me, msg.Command.(Op), msg.CommandIndex)
			kv.CommittedCh <- msg
		}
	}
}

func (kv *KVServer) readFromRaft() {
	for !kv.killed() {
		msg := <-kv.CommittedCh
		kv.mu.Lock()
		DebugPrintf(dCommit, "S%d(replicate side) receive cmd: %+v from applyCh len: %v, committedCh len: %v", kv.me, msg.Command.(Op), len(kv.applyCh), len(kv.CommittedCh))
		if msg.CommandValid {
			cmd := msg.Command.(Op)

			result := OperationResult{
				op: cmd,
			}
			switch cmd.Type {
			case "Get":
				result.ret = kv.db[cmd.Key]
			case "Put":
				kv.db[cmd.Key] = cmd.Value
			case "Append":
				kv.db[cmd.Key] += cmd.Value
			}

			if isLeader := <-kv.rf.AsyncGetState(); isLeader {
				DebugPrintf(dCommit, "S%d(replicate side) is leader", kv.me)
				if kv.opChan[msg.CommandIndex] != nil && *kv.opChan[msg.CommandIndex] != nil {
					DebugPrintf(dCommit, "S%d(replicate side) sending channel with seq: %v", kv.me, cmd.SeqNumber)
					*kv.opChan[msg.CommandIndex] <- result
					DebugPrintf(dCommit, "S%d(replicate side) channel sending done with seq: %v", kv.me, cmd.SeqNumber)
				} else {
					DebugPrintf(dError, "S%d(replicate side) channel close", kv.me)
				}
			}
		}
		kv.mu.Unlock()
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.cliHistory = make(map[int64]map[int]*OperationResult)
	kv.cliSeqNumber = make(map[int64]map[int]bool)
	kv.opChan = make(map[int]*chan OperationResult)
	kv.CommittedCh = make(chan raft.ApplyMsg, 10240)

	go kv.readFromRaft()
	go kv.forward()

	return kv
}
