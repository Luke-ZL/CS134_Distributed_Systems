package paxos

const (
	OK               = "OK"
	ErrPrepareReject = "ErrPrepareReject"
	ErrAcceptReject  = "ErrAcceptReject"
	ErrLearnReject   = "ErrLearnReject"
	ErrDoneReject    = "ErrDoneReject"
)

type PrepareArgs struct {
	SeqNum int
	N_p    int
}

type PrepareReply struct {
	N_p int
	N_a int
	V_a interface{}
	Err string
}

type AcceptArgs struct {
	SeqNum int
	N_a    int
	V_a    interface{}
}

type AcceptReply struct {
	N_p int
	Err string
}

type LearnArgs struct {
	SeqNum int
	V_a    interface{}
}

type LearnReply struct {
	Err string
}

type DoneArgs struct {
	SeqNum    int
	FromIndex int
}

type DoneReply struct {
	Err string
}
