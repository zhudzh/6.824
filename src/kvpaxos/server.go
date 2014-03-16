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

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Type string
  GetArgs *GetArgs
  GetReply *GetReply
  PutArgs *PutArgs  
  PutReply *PutReply
  PaxosSeq int
  ClientID int64
  SeqNum int64

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
  client_last_op map[int64]*Op
  client_max_seq map[int64]int64
  state_mu sync.Mutex
  request_noop_channel chan int
}

func (kv *KVPaxos) sequenceHandled(client_id int64, seq_num int64) (*Op, bool){
  kv.state_mu.Lock()
  defer kv.state_mu.Unlock()

  _, exists := kv.client_last_op[client_id]
  if exists == false{
    kv.client_last_op[client_id] = &Op{}
    kv.client_max_seq[client_id] = -1
  }

  if seq_num <= kv.client_max_seq[client_id] {
    return kv.client_last_op[client_id], true
  }
  return &Op{}, false
}

func (kv *KVPaxos) performGet(op *Op){
  DPrintf("Server %d Applying Get operation", kv.me)
  op.GetReply.Value = kv.kv[op.GetArgs.Key]
  op.GetReply.Err = OK
}

func (kv *KVPaxos) performPut(op *Op) {
  DPrintf("Server %d Applying Put operation. (%d %d) with %s -> %s \n", kv.me, op.ClientID, op.SeqNum, op.PutArgs.Key, op.PutArgs.Value)
  var new_val string
  if op.PutArgs.DoHash{
    prev_val, exists := kv.kv[op.PutArgs.Key]
    if exists == false {
      prev_val = ""
    }
    new_val = strconv.Itoa(int(hash(prev_val + op.PutArgs.Value))) // new value is the hash of the prev_val and value
    op.PutReply.PreviousValue = prev_val
  } else {
    new_val = op.PutArgs.Value
  }
  kv.kv[op.PutArgs.Key] = new_val
  op.PutReply.Err = OK
}


func (kv *KVPaxos) getOpConensus(op *Op, seq_num int) bool{
  DPrintf("Server %d trying to get consenus for %d with possible op %#v", kv.me, seq_num, op)
  kv.px.Start(seq_num, op)
  var op_rcv interface{}
  var decided bool

  to := 10 * time.Millisecond
  for {
    decided, op_rcv = kv.px.Status(seq_num)
    if decided {
      break 
    }
    DPrintf("Server %d trying to get consenus for %d with possible op %#v. Op is not decided, wait and try again!", kv.me, seq_num, op)
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  DPrintf("Server %d got paxos consensus for # %d for op %#v Op is same?: %t", kv.me, seq_num, op_rcv,  op_rcv == op)

  return op_rcv == op
}

func (kv *KVPaxos) performOp(op *Op, seq int) {
  if op.Type == Noop{
    DPrintf("Server %d Applying Noop operation", kv.me)
    return
  }

  handled_op, handled := kv.sequenceHandled(op.ClientID, op.SeqNum)
  if handled{
    DPrintf("Server %d has already handled op from client %d, seq num %d (%d, %d) Paxos seq %d!", kv.me, op.ClientID, op.SeqNum, handled_op.ClientID, handled_op.SeqNum, handled_op.PaxosSeq)
  } else {
    switch op.Type{
    case Get:
      kv.performGet(op)
    case Put:
      kv.performPut(op)      
    }
    kv.state_mu.Lock()
    kv.client_max_seq[op.ClientID] = op.SeqNum
    kv.client_last_op[op.ClientID] = op
    kv.state_mu.Unlock()
  }
}

func (kv *KVPaxos) applyOps() {
  time.Sleep(100 * time.Millisecond)
  seq := 0
  for {
    select {      
    case max_seq := <- kv.request_noop_channel:
      for seq_start := seq; seq_start < max_seq; seq_start++{
        DPrintf("Server %d Proposing noop for paxos instance: %d", kv.me, seq_start)
        kv.px.Start(seq_start, &Op{Type: Noop})
      }  

    default:
      decided, rcv_op := kv.px.Status(seq)
      var op *Op
      switch rcv_op.(type){
      case Op:
        temp := rcv_op.(Op)
        op = &temp
      case *Op:
        op = rcv_op.(*Op)
      }
      if decided {
        DPrintf("Server %d Applying op %d: %#v ", kv.me,seq, op)
        kv.performOp(op, seq)
        // kv.px.Done(seq)
        seq++
      } else {          
        time.Sleep(3 * time.Millisecond)
      }
    }    
  }
}

func (kv *KVPaxos) putOpInLog(op *Op) int{
  attempted_instance := kv.px.Max() + 1
  DPrintf("Server %d Op %#v is trying with seq %d", kv.me, op, attempted_instance)
  performed := kv.getOpConensus(op, attempted_instance)
  for ! performed{
    attempted_instance++
    DPrintf("Server %d Op %#v was not performed! is trying again with seq %d", kv.me, op, attempted_instance)    
    performed = kv.getOpConensus(op, attempted_instance)
  }
  op.PaxosSeq = attempted_instance

  //now op is in log, must get noops if needed
  time.Sleep(50 * time.Millisecond)
  _, handled := kv.sequenceHandled(op.ClientID, op.SeqNum)
  for ! handled{
    kv.request_noop_channel <- attempted_instance
    time.Sleep(750 * time.Millisecond)
    _, handled = kv.sequenceHandled(op.ClientID, op.SeqNum)
  }
  
  kv.px.Done(attempted_instance -1)
  DPrintf("Server %d has handled op for first time from client %d, seq num %d Paxos seq %d!", kv.me, op.ClientID, op.SeqNum, op.PaxosSeq)
  return attempted_instance
}

// Get RPC handler, primary handles get request from client
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // kv.mu.Lock()
  // defer kv.mu.Unlock()

  handled_op, handled := kv.sequenceHandled(args.ClientID, args.SeqNum)

  if handled{
    if args.SeqNum == kv.client_max_seq[args.ClientID]{ // has already processed 
      DPrintf("Server %d says op has already been processed and is most recent! %#v", kv.me, handled_op)
      reply.Value = handled_op.GetReply.Value
      reply.Err = handled_op.GetReply.Err
      DPrintf("Server %d finished request! Op was duplicate, so returning original reply %#v", kv.me, reply)
      return nil
    } else {
      DPrintf("Server %d says op has already been processed and is not most recent! %#v", kv.me, handled_op)
      return nil
    }
  }

  DPrintf("Server %d Received get request!", kv.me)
  op := &Op{Type: Get, GetArgs: args, GetReply: reply, ClientID: args.ClientID, SeqNum: args.SeqNum}
  instance_num := kv.putOpInLog(op)

  DPrintf("Server %d finished get request %d, %#v!", kv.me, instance_num, op)
  return nil
}

// Put RPC handler, primary handles put request from client
func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // kv.mu.Lock()
  // defer kv.mu.Unlock()

  handled_op, handled := kv.sequenceHandled(args.ClientID, args.SeqNum)

  if handled{
    if args.SeqNum == kv.client_max_seq[args.ClientID]{ // has already processed 
      DPrintf("Server %d says op has already been processed and is most recent! %#v", kv.me, handled_op)
      reply.PreviousValue = handled_op.PutReply.PreviousValue
      reply.Err = handled_op.PutReply.Err
      DPrintf("Server %d finished request! Op was duplicate, so returning original reply %#v", kv.me, reply)
      return nil
    } else {
      DPrintf("Server %d says op has already been processed and is not most recent! %#v", kv.me, handled_op)
      return nil
    }
  }

  DPrintf("Server %d received put request %v -> %v! DoHash? %t. Client Seq #: %d", kv.me, args.Key, args.Value, args.DoHash, args.SeqNum)
  op := &Op{Type: Put, PutArgs: args, PutReply: reply, ClientID: args.ClientID, SeqNum: args.SeqNum}
  instance_num := kv.putOpInLog(op)
  
  DPrintf("Server %d finished put request %d, %#v!", kv.me, instance_num, op)
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
  kv.client_last_op = make(map[int64]*Op)
  kv.client_max_seq = make(map[int64]int64)
  kv.request_noop_channel = make(chan int)

  go kv.applyOps()

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

