package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	identifier int64
	seqNumber  int
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seqNumber = 0
	ck.identifier = nrand()
	ck.lastLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	ck.seqNumber++
	args := GetArgs{
		Key:        key,
		SeqNumber:  ck.seqNumber,
		Identifier: ck.identifier,
	}
	ck.mu.Unlock()

	var ret string
	i := ck.lastLeader
	for {
		reply := GetReply{}
		DebugPrintf(dClient, "S%d Get(seq: %v) key is %v to %v", ck.identifier, args.SeqNumber, key, i)
		if ok := ck.sendGet(i, &args, &reply); ok && (reply.Err == OK || reply.Err == ErrCmdExist || reply.Err == ErrNoKey) {
			ck.lastLeader = i
			ret = reply.Value
			break
		}
		DebugPrintf(dError, "S%d Get(seq: %v) err: %v to %v", ck.identifier, args.SeqNumber, reply.Err, i)
		i = (i + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * time.Duration(10))
	}
	return ret
}

func (ck *Clerk) sendGet(i int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[i].Call("KVServer.Get", args, reply)
	return ok
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.seqNumber++
	args := PutAppendArgs{
		Op:         op,
		Key:        key,
		Value:      value,
		SeqNumber:  ck.seqNumber,
		Identifier: ck.identifier,
	}
	ck.mu.Unlock()

	i := ck.lastLeader
	for {
		reply := PutAppendReply{}
		DebugPrintf(dClient, "S%d PutAppend(seq: %v, type: %v) key is %v, val is %v", ck.identifier, args.SeqNumber, args.Op, key, value)
		if ok := ck.sendPutAppend(i, &args, &reply); ok && (reply.Err == OK || reply.Err == ErrCmdExist) {
			ck.lastLeader = i
			break
		}
		DebugPrintf(dError, "S%d PutAppend(seq: %v, type: %v) err: %v", ck.identifier, args.SeqNumber, args.Op, reply.Err)
		i = (i + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * time.Duration(10))
	}
}

func (ck *Clerk) sendPutAppend(i int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
