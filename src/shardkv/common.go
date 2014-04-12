package shardkv
import "hash/fnv"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)

const (
  Get = "Get"
  Put = "Put"
  Noop = "Noop"
  Reconfiguration = "Reconfiguration"
  GetShard = "GetShard"
)

type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  SeqNum int64
  ClientID int64
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  SeqNum int64
  ClientID int64
}

type GetReply struct {
  Err Err
  Value string
}

type GetShardArgs struct {
  ConfigNum int
  GID int64
  ShardNum int
}

type GetShardReply struct {
  Err Err
  KV map[string]string
}


func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

