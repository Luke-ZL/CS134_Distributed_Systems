package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currentView     View
	pingTimes       map[string]time.Time
	idleServers     []string
	hasACKedPrimary bool
}

//helper function
func Contains(idleServerArray []string, server string) bool {
	for _, i := range idleServerArray {
		if i == server {
			return true
		}
	}
	return false
}

func (vs *ViewServer) Update(primary, backup string, fromIdle bool) {
	vs.currentView.Viewnum += 1
	vs.currentView.Primary = primary
	vs.currentView.Backup = backup
	vs.hasACKedPrimary = false
	if fromIdle {
		vs.idleServers[0] = ""
		vs.idleServers = vs.idleServers[1:]
	}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()

	vs.pingTimes[args.Me] = time.Now()

	switch server := args.Me; server {
	case vs.currentView.Primary:
		if args.Viewnum == 0 && vs.hasACKedPrimary {
			if len(vs.idleServers) > 0 {
				vs.Update(vs.currentView.Backup, vs.idleServers[0], true)
				vs.idleServers = append(vs.idleServers, args.Me)
			} else {
				vs.Update(vs.currentView.Backup, vs.currentView.Primary, false)
			}
		} else if args.Viewnum == vs.currentView.Viewnum && !vs.hasACKedPrimary {
			vs.hasACKedPrimary = true
		}
	case vs.currentView.Backup:
		if args.Viewnum == 0 && vs.hasACKedPrimary {
			if len(vs.idleServers) > 0 {
				vs.Update(vs.currentView.Primary, vs.idleServers[0], true)
				vs.idleServers = append(vs.idleServers, args.Me)
			} else {
				vs.Update(vs.currentView.Primary, vs.currentView.Backup, false)
			}
		}
	default:
		if vs.currentView.Viewnum == 0 {
			vs.Update(args.Me, "", false)
		} else {
			if !Contains(vs.idleServers, args.Me) {
				vs.idleServers = append(vs.idleServers, args.Me)
			}
		}
	}

	reply.View = vs.currentView
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.currentView
	vs.mu.Unlock()

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()

	deadInterval := DeadPings * PingInterval
	t := time.Now()

	if t.Sub(vs.pingTimes[vs.currentView.Primary]) >= deadInterval {
		if vs.hasACKedPrimary {
			if len(vs.idleServers) > 0 {
				vs.Update(vs.currentView.Backup, vs.idleServers[0], true)
			} else {
				vs.Update(vs.currentView.Backup, "", false)
			}
		}
	}

	if t.Sub(vs.pingTimes[vs.currentView.Backup]) >= deadInterval {
		if vs.hasACKedPrimary {
			if len(vs.idleServers) > 0 {
				vs.Update(vs.currentView.Primary, vs.idleServers[0], true)
			} else {
				vs.Update(vs.currentView.Primary, "", false)
			}
		}
	}

	for i := 0; i < len(vs.idleServers); i++ {
		if t.Sub(vs.pingTimes[vs.idleServers[i]]) >= deadInterval {
			vs.idleServers[i] = vs.idleServers[len(vs.idleServers)-1]
			vs.idleServers[i] = ""
			vs.idleServers = vs.idleServers[:len(vs.idleServers)-1]
		}
	}

	if vs.currentView.Backup == "" && vs.hasACKedPrimary && len(vs.idleServers) > 0 {
		vs.Update(vs.currentView.Primary, vs.idleServers[0], true)
	}

	vs.mu.Unlock()
	// Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.hasACKedPrimary = false
	vs.pingTimes = make(map[string]time.Time)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
