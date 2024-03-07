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
	time.Sleep(time.Millisecond * time.Duration(300))
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
		DebugPrintf(dTest, "sending Get key: %v to %v, args: %+v", key, i, args)
		ok := ck.sendGet(i, &args, &reply)
		DebugPrintf(dTest, "reply: %+v", reply.Err)
		if ok {
			if reply.Err == OK || reply.Err == ErrCmdExist {
				ck.lastLeader = i
				ret = reply.Value
				break
			}
		}
		i = (i + 1) % len(ck.servers)
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
		DebugPrintf(dTest, "sending %v: key: %v, val: %v to %v, args: %+v", op, key, value, i, args)
		ok := ck.sendPutAppend(i, &args, &reply)
		DebugPrintf(dTest, "reply: %+v", reply.Err)
		if ok {
			if reply.Err == OK || reply.Err == ErrCmdExist {
				ck.lastLeader = i
				break
			}
		}
		i = (i + 1) % len(ck.servers)
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
