package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type AInstance struct {
	n_p    int
	n_a    int
	v_a    interface{}
	status Fate
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instanceSequence map[int]*AInstance
	peerDone         []int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//prepare handler using lecture pseudo code
func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	//log.Printf("%v %v",pb.me, )
	_, ok := px.instanceSequence[args.SeqNum]

	if ok {
		if args.N_p > px.instanceSequence[args.SeqNum].n_p {
			px.instanceSequence[args.SeqNum].n_p = args.N_p
			reply.N_p = args.N_p
			reply.N_a = px.instanceSequence[args.SeqNum].n_a
			reply.V_a = px.instanceSequence[args.SeqNum].v_a
			reply.Err = OK
		} else {
			reply.N_p = px.instanceSequence[args.SeqNum].n_p
			reply.Err = ErrPrepareReject
		}
	} else {
		px.instanceSequence[args.SeqNum] = &AInstance{args.N_p, -1, nil, Pending}
		reply.N_p = args.N_p
		reply.N_a = -1
		reply.V_a = nil
		reply.Err = OK
	}

	px.mu.Unlock()
	return nil
}

//accept handler
func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	_, ok := px.instanceSequence[args.SeqNum]

	if ok {
		if args.N_a >= px.instanceSequence[args.SeqNum].n_p {
			px.instanceSequence[args.SeqNum].n_a = args.N_a
			px.instanceSequence[args.SeqNum].n_p = args.N_a
			px.instanceSequence[args.SeqNum].v_a = args.V_a
			reply.N_p = args.N_a
			reply.Err = OK
		} else {
			reply.N_p = px.instanceSequence[args.SeqNum].n_p
			reply.Err = ErrAcceptReject
		}
	} else {
		px.instanceSequence[args.SeqNum] = &AInstance{args.N_a, args.N_a, args.V_a, Pending}
		reply.N_p = args.N_a
		reply.Err = OK
	}

	px.mu.Unlock()
	return nil
}

//learn handler
func (px *Paxos) LearnHandler(args *LearnArgs, reply *LearnReply) error {
	px.mu.Lock()
	_, ok := px.instanceSequence[args.SeqNum]
	if ok {
		px.instanceSequence[args.SeqNum].v_a = args.V_a
		px.instanceSequence[args.SeqNum].status = Decided
		reply.Err = OK
	} else {
		px.instanceSequence[args.SeqNum] = &AInstance{-1, -1, args.V_a, Decided}
		reply.Err = OK
	}

	px.mu.Unlock()
	return nil
}

//done handler
func (px *Paxos) DoneHandler(args *DoneArgs, reply *DoneReply) error {
	if args.SeqNum < px.peerDone[args.FromIndex] {
		reply.Err = OK
	} else {
		px.mu.Lock()
		px.peerDone[args.FromIndex] = args.SeqNum
		for k, v := range px.instanceSequence {
			if k < px.Min() && v.status == Decided {
				delete(px.instanceSequence, k)
			}
		}
		px.mu.Unlock()
		reply.Err = OK
	}
	return nil
}

//px.me will always be a single number in the test file, so 100000 is more than enough
func GenerateProposalNumber(meInt int, highestNow int) int {
	return highestNow*100000 + meInt
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//

func (px *Paxos) StartHelper(seq int, v interface{}) {
	px.mu.Lock()
	if _, ok := px.instanceSequence[seq]; !ok {
		px.instanceSequence[seq] = &AInstance{-1, -1, nil, Pending}
	}
	px.mu.Unlock()

	hasAccpeted := false
	highestAccepted := -1
	// n_p := -1
	proposalNumber := GenerateProposalNumber(px.me, 1)
	for !(px.isdead() || hasAccpeted) {
		//log.Printf(px.peers[px.me])
		//log.Printf(strconv.Itoa(proposalNumber))
		prepareCount := 0
		for i, peer := range px.peers {
			var reply PrepareReply
			if i == px.me {
				px.PrepareHandler(&PrepareArgs{seq, proposalNumber}, &reply)
			} else {
				ok := call(peer, "Paxos.PrepareHandler", &PrepareArgs{seq, proposalNumber}, &reply)
				// if ok && reply.Err == OK {
				// 	prepareCount++
				// 	if reply.N_a > highestAccepted {
				// 		v = reply.V_a
				// 		highestAccepted = reply.N_a
				// 	}
				// } else if ok && reply.Err == ErrPrepareReject {
				// 	if n_p < reply.N_p {
				// 		n_p = reply.N_p
				// 	}
				// }
				// new start
				if !ok {
					reply.Err = ErrPrepareReject
				}
				// new end
			}
			// new start
			if reply.Err == OK {
				prepareCount++
				if reply.N_a > highestAccepted {
					highestAccepted = reply.N_a
					v = reply.V_a
				}
			}
			// new end
		}
		//log.Printf(strconv.Itoa(prepareCount))
		if prepareCount > (len(px.peers) / 2) {
			//log.Printf("Accept %v", px.me)
			acceptCount := 0
			for i, peer := range px.peers {
				var reply AcceptReply
				if i == px.me {
					px.AcceptHandler(&AcceptArgs{seq, proposalNumber, v}, &reply)
				} else {
					ok := call(peer, "Paxos.AcceptHandler", &AcceptArgs{seq, proposalNumber, v}, &reply)
					// if ok && reply.Err == OK {
					// 	acceptCount++
					// } else if ok && reply.Err == ErrAcceptReject {
					// 	if n_p < reply.N_p {
					// 		n_p = reply.N_p
					// 	}
					// }
					// new start
					if !ok {
						reply.Err = ErrAcceptReject
					}
					// new end
				}
				// new start
				if reply.Err == OK {
					acceptCount++
				}
				// new end
			}

			if acceptCount > (len(px.peers) / 2) {
				//log.Printf("Learn %v", px.me)
				hasAccpeted = true
				for i, peer := range px.peers {
					var reply LearnReply
					if i == px.me {
						px.LearnHandler(&LearnArgs{seq, v}, &reply)
					} else {
						ok := call(peer, "Paxos.LearnHandler", &LearnArgs{seq, v}, &reply)
						if !ok {
							//log.Printf("LearnHandler call not OK")
							// new start
							reply.Err = ErrLearnReject
							// new end
						}
					}
				}
			} else {
				if highestAccepted > proposalNumber {
					proposalNumber = GenerateProposalNumber(px.me, highestAccepted/100000)
				}
			}
		}
		/*
			if proposalNumber < n_p || proposalNumber < highestAccepted {
				var higher int
				if n_p > highestAccepted {
					higher = n_p
				} else {
					higher = highestAccepted
				}
				//higher = higher/100000 + 1
				proposalNumber = higher + 1 //GenerateProposalNumber(px.me, higher)
			} else {
				proposalNumber++
			}*/

		proposalNumber++
	}
}

func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//if seq < px.Min() {
	//return
	//}
	go px.StartHelper(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if seq > px.peerDone[px.me] {
		for i, peer := range px.peers {
			var reply DoneReply
			if i == px.me {
				px.DoneHandler(&DoneArgs{seq, px.me}, &reply)
			} else {
				ok := call(peer, "Paxos.DoneHandler", &DoneArgs{seq, px.me}, &reply)
				if !ok {
					reply.Err = ErrDoneReject
					//log.Printf("DoneHandler call not OK")
				}
			}
		}
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	if len(px.instanceSequence) == 0 {
		//log.Printf("instanceSequence has 0 len")
		return -1
	}
	max := -1
	for key, _ := range px.instanceSequence {
		if key > max {
			max = key
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	min := px.peerDone[0]
	for _, peerMin := range px.peerDone {
		if min > peerMin {
			min = peerMin
		}
	}

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	ins, ok := px.instanceSequence[seq]
	px.mu.Unlock()
	if ok {
		if ins.status == Decided {
			return Decided, ins.v_a
		}
	}
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instanceSequence = make(map[int]*AInstance)
	px.peerDone = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		px.peerDone[i] = -1
	}
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
