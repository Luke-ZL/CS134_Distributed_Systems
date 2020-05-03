package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	OpType string
	Id     string

	ShouldForward bool
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id string

	ShouldForward bool
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type Request struct {
	OpType string
	Key    string
	Value  string
}

type SyncArgs struct {
	KeyValue   map[string]string
	RequestHis map[string]Request
}

type SyncReply struct {
	Err Err
}

//Id for a request should be {timestamp}+{nrand()}
