package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs    []Config // indexed by config num
	currentSeq int
}

type Op struct {
	// Your data here.
	OpType      string
	OpId        string //unique identifier
	GID         int64
	ServerPorts []string
	ShardNum    int
	ConfigNum   int
}

func CreateId() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10) + strconv.FormatInt(rand.Int63(), 10)
}

// waitPaxosAgreement ...
func (sm *ShardMaster) waitPaxosAgreement(seq int, value Op) Op {
	to := 10 * time.Millisecond
	for {
		status, res := sm.px.Status(seq)
		if status == paxos.Decided {
			return res.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func getMinMax(numShards map[int64]int) (int64, int64) {
	i := 0
	var min, max int64
	min = 0
	max = 0
	for k, v := range numShards {
		if i == 0 {
			min = k
			max = k
		} else {
			if v < numShards[min] {
				min = k
			}
			if v > numShards[max] {
				max = k
			}
		}
		i++
	}
	return min, max
}

func Balance(conf *Config, gID int64, opType string) {
	if len(conf.Groups) == 1 {
		for k, _ := range conf.Groups {
			for i, _ := range conf.Shards {
				conf.Shards[i] = k
			}
			break
		}
		return
	}

	numShardsNow := make(map[int64]int)
	for k, _ := range conf.Groups {
		numShardsNow[k] = 0
	}
	for _, v := range conf.Shards {
		_, ok := conf.Groups[v]
		if ok {
			numShardsNow[v]++
		}
	}

	min, max := getMinMax(numShardsNow)
	if opType == "leave" {
		for i, v := range conf.Shards {
			if v == gID {
				conf.Shards[i] = min
				numShardsNow[min]++
			}
		}
	}

	min, max = getMinMax(numShardsNow)
	for numShardsNow[max]-numShardsNow[min] > 1 {
		for i, v := range conf.Shards {
			if v == max {
				conf.Shards[i] = min
				numShardsNow[max]--
				numShardsNow[min]++
				break
			}
		}
		min, max = getMinMax(numShardsNow)
	}
}

func (sm *ShardMaster) Operate(op Op) Config {
	for {
		conf := Config{}
		curSeq := sm.currentSeq
		sm.currentSeq++
		var operation Op
		status, res := sm.px.Status(curSeq)
		if status == paxos.Decided {
			operation = res.(Op)
		} else {
			sm.px.Start(curSeq, op)
			operation = sm.waitPaxosAgreement(curSeq, op)
		}

		lastIndex := len(sm.configs) - 1
		conf.Num = sm.configs[lastIndex].Num + 1
		for i, v := range sm.configs[lastIndex].Shards {
			conf.Shards[i] = v
		}
		conf.Groups = make(map[int64][]string)
		for k, v := range sm.configs[lastIndex].Groups {
			conf.Groups[k] = v
		}

		switch operation.OpType {
		case "join":
			conf.Groups[operation.GID] = operation.ServerPorts
			Balance(&conf, operation.GID, "join")
			sm.configs = append(sm.configs, conf)
		case "leave":
			delete(conf.Groups, operation.GID)
			Balance(&conf, operation.GID, "leave")
			sm.configs = append(sm.configs, conf)
		case "move":
			conf.Shards[operation.ShardNum] = operation.GID
			sm.configs = append(sm.configs, conf)
		case "query":
			if operation.ConfigNum == -1 || operation.ConfigNum > lastIndex {
				conf = sm.configs[lastIndex]
			} else {
				conf = sm.configs[operation.ConfigNum]
			}
		}

		sm.px.Done(curSeq)

		if operation.OpId == op.OpId {
			return conf
		}
	}
	return Config{}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	operation := Op{"join", CreateId(), args.GID, args.Servers, -1, -1}
	sm.Operate(operation)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	operation := Op{"leave", CreateId(), args.GID, nil, -1, -1}
	sm.Operate(operation)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	operation := Op{"move", CreateId(), args.GID, nil, args.Shard, -1}
	sm.Operate(operation)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	operation := Op{"query", CreateId(), -1, nil, -1, args.Num}
	reply.Config = sm.Operate(operation)
	sm.mu.Unlock()
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.currentSeq = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
