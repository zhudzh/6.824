package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
  SeqNum int64
  ClientID int64

  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
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


// Your RPC definitions here.

type StateTransferArgs struct{
  Kv map[string]string
  ClientMap map[int64]map[int64]string
}

type StateTransferReply struct{
  Err Err
}

type GetUpdateArgs struct{
  Key string
  Value string
  SeqNum int64
  ClientID int64
}

type GetUpdateReply struct{
  Err Err
  Value string
}

type PutUpdateArgs struct{
  Key string
  Value string
  SeqNum int64
  ClientID int64
}

type PutUpdateReply struct{
  Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

