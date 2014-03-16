package kvpaxos

import "net/rpc"
import "fmt"
import crrand "crypto/rand"
import "math/big"
import "time"
import "math/rand"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
  id int64
  seq_num int64
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  ck.id = nrand()
  ck.seq_num = 0
  return ck
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crrand.Int(crrand.Reader, max)
  x := bigx.Int64()
  return x
}
//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  ck.seq_num++
  DPrintf("client %d, seq %d sends get request with key %s", ck.id, ck.seq_num, key)
  args := &GetArgs{Key: key, SeqNum:ck.seq_num, ClientID: ck.id}
  var reply GetReply

  to := 10 * time.Millisecond
  server := rand.Int() % len(ck.servers)

  ok := call(ck.servers[server], "KVPaxos.Get", args, &reply)
  DPrintf("client %d, seq %d received from server %d get reply! OK: %t reply: %#v",ck.id, ck.seq_num, server, ok, reply)
  for ok == false || reply.Err != OK  {
    DPrintf("client %d, seq %d received from server %d failed get reply! OK: %t reply: %#v",ck.id, ck.seq_num, server, ok, reply)
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
    server = (server + 1) % len(ck.servers)
    DPrintf("client %d, seq %d trying again with server %d", ck.id, ck.seq_num, server)
    ok = call(ck.servers[server], "KVPaxos.Get", args, &reply)
    
  }
  return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  ck.seq_num++
  DPrintf("client %d, seq %d sends put request with key %s, value %s, dohash %t",ck.id, ck.seq_num, key, value, dohash)
  args := &PutArgs{Key: key, Value: value, DoHash: dohash, SeqNum:ck.seq_num, ClientID: ck.id}
  var reply PutReply

  to := 10 * time.Millisecond
  server := rand.Int() % len(ck.servers)

  ok := call(ck.servers[server], "KVPaxos.Put", args, &reply)
  DPrintf("client %d, seq %d received from server %d put reply! OK: %t reply: %#v",ck.id, ck.seq_num, server, ok, reply)
  for ok == false || reply.Err != OK  {
    DPrintf("client %d, seq %d received from server %d failed put reply! OK: %t reply: %#v",ck.id, ck.seq_num, server, ok, reply)
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
    server = (server + 1) % len(ck.servers)
    DPrintf("client %d, seq %d trying again with server %d", ck.id, ck.seq_num, server)
    ok = call(ck.servers[server], "KVPaxos.Put", args, &reply)
    
  }
  return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
