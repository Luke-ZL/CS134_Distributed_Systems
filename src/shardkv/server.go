package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Key     string
	Value   string
	ConfNum int
	Op      string
	Id      string
	PrevId  string
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	seq      int
	db       map[string]string
	requests map[string]Request

	conf   shardmaster.Config
	confDb map[int]map[string]string
}

// isDuplicatedGet ...
func (kv *ShardKV) isDuplicatedGet(args *GetArgs) bool {
	dup, ok := kv.requests[args.Id]
	return ok && dup.Key == args.Key
}

// isDuplicatedPutAppend ...
func (kv *ShardKV) isDuplicatedPutAppend(args *PutAppendArgs) bool {
	dup, ok := kv.requests[args.Id]
	return ok && dup.Key == args.Key && dup.Value == args.Value && dup.Op == args.Op
}

// isCorrectGroup ...
func (kv *ShardKV) isCorrectGroup(key string) bool {
	return kv.conf.Shards[key2shard(key)] == kv.gid
}

// waitPaxosAgreement ...
func (kv *ShardKV) waitPaxosAgreement(seq int, value Op) Op {
	to := 10 * time.Millisecond
	for {
		status, res := kv.px.Status(seq)
		if status == paxos.Decided {
			return res.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// freeRequest ...
func (kv *ShardKV) freeRequest(prevId string) {
	_, ok := kv.requests[prevId]
	if ok {
		delete(kv.requests, prevId)
	}
}

// operate ...
func (kv *ShardKV) operate(operation Op) {
	if operation.Op == "Get" {
		value, ok := kv.db[operation.Key]
		if ok {
			kv.requests[operation.Id] = Request{Op: "Get", Key: operation.Key, Value: value, ConfNum: -1}
		} else {
			kv.requests[operation.Id] = Request{}
		}

	} else if operation.Op == "Put" {
		kv.db[operation.Key] = operation.Value
		kv.requests[operation.Id] = Request{Op: operation.Op, Key: operation.Key, Value: operation.Value, ConfNum: -1}

	} else if operation.Op == "Append" {
		kv.db[operation.Key] = kv.db[operation.Key] + operation.Value
		kv.requests[operation.Id] = Request{Op: operation.Op, Key: operation.Key, Value: operation.Value, ConfNum: -1}

	} else if operation.Op == "Reconfigure" {
		for kv.conf.Num < operation.ConfNum {

			if kv.conf.Num == 0 {
				kv.conf = kv.sm.Query(1)
				continue
			}

			updatedConf := kv.sm.Query(kv.conf.Num + 1)

			dbCopy := make(map[string]string)
			for k, v := range kv.db {
				if kv.conf.Shards[key2shard(k)] == kv.gid {
					dbCopy[k] = v
				}
			}
			kv.confDb[kv.conf.Num] = dbCopy

			for s, g := range kv.conf.Shards {

				updatedGroup := updatedConf.Shards[s]
				if g != updatedGroup && updatedGroup == kv.gid {

					flag := false
					for !flag {
						for _, server := range kv.conf.Groups[g] {
							var reply RetrieveDbReply
							ok := call(server, "ShardKV.RetrieveDb", &RetrieveDbArgs{ConfNum: kv.conf.Num}, &reply)
							if ok && reply.Err == OK {
								for k, v := range reply.Db {
									kv.db[k] = v
								}
								flag = true
								break
							}
						}
					}

				}
			}
			kv.conf = updatedConf
		}

	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isCorrectGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return nil
	}

	if kv.isDuplicatedGet(args) {
		reply.Value = kv.db[args.Key]
		reply.Err = OK
		return nil
	}

	for {
		currSeq := kv.seq
		kv.seq++
		var operation Op
		status, res := kv.px.Status(currSeq)
		if status == paxos.Decided {
			operation = res.(Op)
		} else {
			op := Op{Key: args.Key, Value: "", ConfNum: -1, Op: "Get", Id: args.Id, PrevId: args.PrevId}
			kv.px.Start(currSeq, op)
			operation = kv.waitPaxosAgreement(currSeq, op)
		}

		kv.freeRequest(args.PrevId)
		kv.operate(operation)
		kv.px.Done(currSeq)

		if operation.Id == args.Id {
			if kv.requests[args.Id] == (Request{}) {
				reply.Value = ""
				reply.Err = ErrNoKey
			} else {
				reply.Value = kv.requests[args.Id].Value
				reply.Err = OK
			}
			break
		}
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isCorrectGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return nil
	}

	if kv.isDuplicatedPutAppend(args) {
		reply.Err = OK
		return nil
	}

	for {
		currSeq := kv.seq
		kv.seq++
		var operation Op
		status, res := kv.px.Status(currSeq)
		if status == paxos.Decided {
			operation = res.(Op)
		} else {
			op := Op{Key: args.Key, Value: args.Value, ConfNum: -1, Op: args.Op, Id: args.Id, PrevId: args.PrevId}
			kv.px.Start(currSeq, op)
			operation = kv.waitPaxosAgreement(currSeq, op)
		}

		kv.freeRequest(args.PrevId)
		kv.operate(operation)
		kv.px.Done(currSeq)

		if operation.Id == args.Id {
			break
		}
	}
	reply.Err = OK
	return nil
}

func (kv *ShardKV) RetrieveDb(args *RetrieveDbArgs, reply *RetrieveDbReply) error {

	db, ok := kv.confDb[args.ConfNum]
	if ok {
		reply.Db = make(map[string]string)
		for k, v := range db {
			reply.Db[k] = v
		}
		reply.Err = OK
	} else {
		reply.Err = ErrInvalidConfNum
	}

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	updatedConf := kv.sm.Query(-1)
	if kv.conf.Num != updatedConf.Num {

		for {
			currSeq := kv.seq
			kv.seq++
			var operation Op
			status, res := kv.px.Status(currSeq)
			if status == paxos.Decided {
				operation = res.(Op)
			} else {
				op := Op{Key: "", Value: "", ConfNum: updatedConf.Num, Op: "Reconfigure", Id: "", PrevId: ""}
				kv.px.Start(currSeq, op)
				operation = kv.waitPaxosAgreement(currSeq, op)
			}

			kv.operate(operation)
			kv.px.Done(currSeq)

			if operation.Id == "" {
				break
			}
		}

	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.seq = 0
	kv.db = make(map[string]string)
	kv.requests = make(map[string]Request)
	kv.conf = kv.sm.Query(-1)
	kv.confDb = make(map[int]map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
