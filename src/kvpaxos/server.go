package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
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
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	Op     string
	Id     string
	PrevId string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seq      int
	db       map[string]string
	requests map[string]Request
}

// isDuplicatedGet ...
func (kv *KVPaxos) isDuplicatedGet(args *GetArgs) bool {
	dup, ok := kv.requests[args.Id]
	return ok && dup.Key == args.Key
}

// isDuplicatedPutAppend ...
func (kv *KVPaxos) isDuplicatedPutAppend(args *PutAppendArgs) bool {
	dup, ok := kv.requests[args.Id]
	return ok && dup.Key == args.Key && dup.Value == args.Value && dup.Op == args.Op
}

// waitPaxosAgreement ...
func (kv *KVPaxos) waitPaxosAgreement(seq int, value Op) Op {
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
func (kv *KVPaxos) freeRequest(prevId string) {
	_, ok := kv.requests[prevId]
	if ok {
		delete(kv.requests, prevId)
	}
}

// operate
func (kv *KVPaxos) operate(operation Op) {
	if operation.Op == "Get" {
		value, ok := kv.db[operation.Key]
		if ok {
			kv.requests[operation.Id] = Request{Op: "Get", Key: operation.Key, Value: value}
		} else {
			kv.requests[operation.Id] = Request{}
		}
	} else if operation.Op == "Put" {
		kv.db[operation.Key] = operation.Value
		kv.requests[operation.Id] = Request{Op: operation.Op, Key: operation.Key, Value: operation.Value}
	} else if operation.Op == "Append" {
		kv.db[operation.Key] = kv.db[operation.Key] + operation.Value
		kv.requests[operation.Id] = Request{Op: operation.Op, Key: operation.Key, Value: operation.Value}
	}
}

// Get ...
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
			op := Op{Key: args.Key, Value: "", Op: "Get", Id: args.Id, PrevId: args.PrevId}
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

// PutAppend ...
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
			op := Op{Key: args.Key, Value: args.Value, Op: args.Op, Id: args.Id, PrevId: args.PrevId}
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

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.seq = 0
	kv.db = make(map[string]string)
	kv.requests = make(map[string]Request)

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
