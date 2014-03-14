package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"
// import "reflect"


const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format + "\n", a...)
  }
  return
}

const (
  Get = "Get"
  Put = "Put"
  Noop = "Noop"
)

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Type string
  GetArgs *GetArgs
  GetReply *GetReply
  PutArgs *PutArgs  
  PutReply *PutReply

}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kv map[string]string
  min_seq int // smallest op seq number which has been already applied
}


func (kv *KVPaxos) getOpConensus(op Op, seq_num int) bool{
  DPrintf("server %d trying to get consenus for %d with possible op %#v", kv.me, seq_num, op)
  kv.px.Start(seq_num, op)
  var op_rcv interface{}
  var decided bool

  to := 10 * time.Millisecond
  for {
    decided, op_rcv = kv.px.Status(seq_num)
    if decided {
      break 
    }
    DPrintf("server %d trying to get consenus for %d with possible op %#v. Op is not decided, wait and try again!", kv.me, seq_num, op)
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  DPrintf("server %d got paxos consensus for # %d for op %#v Op is same?: %t", kv.me, seq_num, op_rcv,  op_rcv == op)

  return op_rcv == op
}

func (kv *KVPaxos) fillHoles(start int, end int) {
  DPrintf("server %d trying to fill holes from [%d to %d)", kv.me, start, end)
  for seq := start; seq < end; seq++ {
    decided, _ := kv.px.Status(seq)
    if ! decided {
      DPrintf("Server %d is requesting a noop for # %d because there is a hole!", kv.me, seq)
      kv.getOpConensus(Op{Type: Noop}, seq)      
    }
  }
  DPrintf("server %d has filled holes from [%d to %d)!", kv.me, start, end)
}

func (kv *KVPaxos) applyOps(start int, end int) {
  DPrintf("server %d trying to apply ops from [%d to %d)", kv.me, start, end)
  for seq := start; seq < end; seq++ {
    decided, op := kv.px.Status(seq)
    if ! decided {
      DPrintf("WE have a problem!!!! seq %d was never decided!", seq)
    } else {
      DPrintf("Server %d Applying op %d: %#v ", kv.me,seq, op)
      kv.performOp(op.(Op))     
    }
  }
  kv.min_seq = end - 1
  DPrintf("server %d has applied ops from [%d to %d)! Has now applied ops up to %d", kv.me, start, end, kv.min_seq)
  DPrintf("server %d kv is %v", kv.me, kv.kv)
  
  kv.px.Done(kv.min_seq)

}

func (kv *KVPaxos) performOp(op Op) {
  switch op.Type{
    case Get:
      kv.performGet(op)
    case Put:
      kv.performPut(op)
    case Noop:
      DPrintf("Server %d Applying Noop operation", kv.me)
  }
}

func (kv *KVPaxos) putOpInLog(op Op) int{
  attempted_instance := kv.px.Max() + 1
  DPrintf("Server %d Op %#v is trying with seq %d", kv.me, op, attempted_instance)
  performed := kv.getOpConensus(op, attempted_instance)
  for ! performed{
    attempted_instance++
    DPrintf("Server %d Op %#v was not performed! is trying again with seq %d", kv.me, op, attempted_instance)    
    performed = kv.getOpConensus(op, attempted_instance)
    
  }
  kv.fillHoles(kv.min_seq + 1, attempted_instance + 1)
  kv.applyOps(kv.min_seq + 1, attempted_instance + 1)

  return attempted_instance
}

func (kv *KVPaxos) performGet(op Op){
  DPrintf("Server %d Applying Get operation", kv.me)
  op.GetReply.Value = kv.kv[op.GetArgs.Key]
  op.GetReply.Err = OK
  // DPrintf("get is %s", op.GetReply.Value)
  //update state to reflect the put has been done and the value it should be
}

func (kv *KVPaxos) performPut(op Op) {
  DPrintf("Server %d Applying Put operation", kv.me)
  if op.PutArgs.DoHash{
      prev_val, exists := kv.kv[op.PutArgs.Key]
      if exists == false {
        prev_val = ""
      }
      op.PutArgs.Value = strconv.Itoa(int(hash(prev_val + op.PutArgs.Value))) // new value is the hash of the prev_val and value
      op.PutReply.PreviousValue = prev_val
    }
    kv.kv[op.PutArgs.Key] = op.PutArgs.Value
    op.PutReply.Err = OK
    //update state to reflect the put has been done and the value it should be
}

// Get RPC handler, primary handles get request from client
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf("server %d received get request!", kv.me)
  op := Op{Type: Get, GetArgs: args, GetReply: reply}
  instance_num := kv.putOpInLog(op)

  DPrintf("server %d finished get request %d, %#v!", kv.me, instance_num, op)
  return nil
}

// Put RPC handler, primary handles put request from client
func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf("server %d received put request %v -> %v! DoHash? %t. Client Seq #: %d", kv.me, args.Key, args.Value, args.DoHash, args.SeqNum)
  op := Op{Type: Put, PutArgs: args, PutReply: reply}
  instance_num := kv.putOpInLog(op)
  
  DPrintf("server %d finished put request %d, %#v!", kv.me, instance_num, op)
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
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
  kv.kv = make(map[string]string)
  kv.min_seq = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

