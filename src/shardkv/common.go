package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrInvalidConfNum = "ErrInvalidConfNum"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id     string
	PrevId string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id     string
	PrevId string
}

type GetReply struct {
	Err   Err
	Value string
}

type RetrieveDbArgs struct {
	ConfNum int
}

type RetrieveDbReply struct {
	Err Err
	Db  map[string]string
}

type Request struct {
	Op      string
	Key     string
	Value   string
	ConfNum int
}
