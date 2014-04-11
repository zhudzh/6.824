package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "math"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 1 {
    n, err = fmt.Printf(format + "\n", a...)
  }
  return
}

func DPrintf2(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format + "\n", a...)
  }
  return
}

func DPrintf3(format string, a ...interface{}) (n int, err error) {
  if Debug > -1 {
    n, err = fmt.Printf(format + "\n", a...)
  }
  return
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  id string
  socket_me string 
  kvs map[int]map[string]string
  my_gid map[int]int64
  client_last_op map[int64]*Op
  client_max_seq map[int64]int64
  request_noop_channel chan int
  state_mu sync.Mutex
  curr_config shardmaster.Config
  max_config_in_log int
}

type Op struct {
  Type string
  GetArgs *GetArgs
  GetReply *GetReply
  PutArgs *PutArgs  
  PutReply *PutReply
  ReconfigurationConfig shardmaster.Config
  PaxosSeq int
  ClientID int64
  SeqNum int64
}

func (kv *ShardKV) sequenceHandled(client_id int64, seq_num int64) (*Op, bool){
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

func (kv *ShardKV) performGet(op *Op){
  DPrintf("Server %s Applying Get operation", kv.id)
  op.GetReply.Value = kv.kvs[kv.curr_config.Num][op.GetArgs.Key]
  op.GetReply.Err = OK
}

func (kv *ShardKV) performPut(op *Op) {
  DPrintf("Server %s Applying Put operation. (%d %d) with %s -> %s \n", kv.id, op.ClientID, op.SeqNum, op.PutArgs.Key, op.PutArgs.Value)
  var new_val string
  if op.PutArgs.DoHash{
    prev_val, exists := kv.kvs[kv.curr_config.Num][op.PutArgs.Key]
    if exists == false {
      prev_val = ""
    }
    new_val = strconv.Itoa(int(hash(prev_val + op.PutArgs.Value))) // new value is the hash of the prev_val and value
    op.PutReply.PreviousValue = prev_val
  } else {
    new_val = op.PutArgs.Value
  }
  kv.kvs[kv.curr_config.Num][op.PutArgs.Key] = new_val
  op.PutReply.Err = OK
}

func (kv *ShardKV) getOpConensus(op *Op, seq_num int) bool{
  DPrintf("Server %s trying to get consenus for %d with possible op client %d, seq %d", kv.id, seq_num, op.ClientID, op.SeqNum)
  kv.px.Start(seq_num, op)
  var op_rcv interface{}
  var decided bool

  to := 10 * time.Millisecond
  for {
    decided, op_rcv = kv.px.Status(seq_num)
    if decided {
      break 
    }
    DPrintf("Server %s trying to get consenus for %d with possible op client %d, seq %d. Op is not decided, wait and try again!", kv.id, seq_num, op.ClientID, op.SeqNum)
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  DPrintf("Server %s got paxos consensus for # %d for op client %d, seq %d Op is same?: %t", kv.id, seq_num, op.ClientID, op.SeqNum,  op_rcv == op)

  return op_rcv == op
}

func (kv *ShardKV) performOp(op *Op, seq int) {
  switch op.Type{
  case Noop:
    DPrintf("Server %s Applying Noop operation", kv.socket_me)
    return
  }

  handled_op, handled := kv.sequenceHandled(op.ClientID, op.SeqNum)
  if handled{
    DPrintf("Server %s has already handled op from client %d, seq num %d (%d, %d) Paxos seq %d!", kv.id, op.ClientID, op.SeqNum, handled_op.ClientID, handled_op.SeqNum, handled_op.PaxosSeq)
  } else {
    switch op.Type{
    case Get:
      kv.performGet(op)
    case Put:
      kv.performPut(op)
    case Reconfiguration:
      DPrintf2("Server %s applying reconfig: cli : %v, seq: %v seq handled", kv.id, op.ClientID, op.SeqNum)
      kv.performReconfiguration(op)
    }
    kv.state_mu.Lock()
    kv.client_max_seq[op.ClientID] = op.SeqNum
    kv.client_last_op[op.ClientID] = op
    kv.state_mu.Unlock()
  }
}

func (kv *ShardKV) applyOps() {
  time.Sleep(100 * time.Millisecond)
  seq := 0
  for {
    select {      
    case max_seq := <- kv.request_noop_channel:
      for seq_start := seq; seq_start < max_seq; seq_start++{
        DPrintf("Server %s Proposing noop for paxos instance: %d", kv.id, seq_start)
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
        DPrintf("Server %s Applying op %d: Cli %d, SeqNum %d ", kv.id, seq, op.ClientID, op.SeqNum)
        kv.performOp(op, seq)
        seq++
      } else {          
        time.Sleep(5 * time.Millisecond)
      }
    }    
  }
}

func (kv *ShardKV) putOpInLog(op *Op) int{
  attempted_instance := kv.px.Max() + 1
  DPrintf("Server %s Client %v Seq %v is trying with seq %d", kv.id, op.ClientID, op.SeqNum, attempted_instance)
  performed := kv.getOpConensus(op, attempted_instance)
  for ! performed{
    attempted_instance++
    DPrintf("Server %s Op %#v was not performed! is trying again with seq %d", kv.id, op, attempted_instance)    
    performed = kv.getOpConensus(op, attempted_instance)
  }
  op.PaxosSeq = attempted_instance

  //now op is in log, must get noops if needed
  time.Sleep(50 * time.Millisecond)
  _, handled := kv.sequenceHandled(op.ClientID, op.SeqNum)
  for ! handled{
    kv.request_noop_channel <- attempted_instance
    time.Sleep(500 * time.Millisecond)
    _, handled = kv.sequenceHandled(op.ClientID, op.SeqNum)
  }
  
  kv.px.Done(attempted_instance)
  DPrintf("Server %s has handled op for first time from client %d, seq num %d Paxos seq %d!", kv.id, op.ClientID, op.SeqNum, op.PaxosSeq)
  return attempted_instance
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  shard := key2shard(args.Key)
  gid := kv.curr_config.Shards[shard]
  if gid != kv.gid {
    DPrintf2("Server %s: Wrong Server in Get Request! I am gid %v and shard belongs to gid: %v", kv.id, kv.gid, gid)
    reply.Err = ErrWrongGroup
    return nil
  }

  handled_op, handled := kv.sequenceHandled(args.ClientID, args.SeqNum)

  if handled{
    if args.SeqNum == kv.client_max_seq[args.ClientID]{ // has already processed 
      DPrintf("Server %s says op has already been processed and is most recent! %#v", kv.id, handled_op)
      reply.Value = handled_op.GetReply.Value
      reply.Err = handled_op.GetReply.Err
      DPrintf("Server %s finished request! Op was duplicate, so returning original reply %#v", kv.id, reply)
      return nil
    } else {
      DPrintf("Server %s says op has already been processed and is not most recent! %#v", kv.id, handled_op)
      return nil
    }
  }

  DPrintf("Server %s Received get request!", kv.id)
  op := &Op{Type: Get, GetArgs: args, GetReply: reply, ClientID: args.ClientID, SeqNum: args.SeqNum}
  instance_num := kv.putOpInLog(op)

  DPrintf("Server %s finished get request %d, %v, %v!", kv.id, instance_num, args, reply)
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  shard := key2shard(args.Key)
  gid := kv.curr_config.Shards[shard]
  if gid != kv.gid {
    DPrintf2("Server %s: Wrong Server in Put request! I am gid %v and shard belongs to gid: %v", kv.id, kv.gid, gid)
    reply.Err = ErrWrongGroup
    return nil
  }

  handled_op, handled := kv.sequenceHandled(args.ClientID, args.SeqNum)

  if handled{
    if args.SeqNum == kv.client_max_seq[args.ClientID]{ // has already processed 
      DPrintf("Server %s says op has already been processed and is most recent! %#v", kv.id, handled_op)
      reply.PreviousValue = handled_op.PutReply.PreviousValue
      reply.Err = handled_op.PutReply.Err
      DPrintf("Server %s finished request! Op was duplicate, so returning original reply %#v", kv.id, reply)
      return nil
    } else {
      DPrintf("Server %s says op has already been processed and is not most recent! %#v", kv.id, handled_op)
      return nil
    }
  }

  DPrintf("Server %s received put request %v -> %v! DoHash? %t. Client: %d Seq #: %d", kv.id, args.Key, args.Value, args.DoHash, args.ClientID, args.SeqNum)
  op := &Op{Type: Put, PutArgs: args, PutReply: reply, ClientID: args.ClientID, SeqNum: args.SeqNum}
  instance_num := kv.putOpInLog(op)
  
  DPrintf("Server %s finished put request %d, Args: %v Reply: %v!", kv.id, instance_num, args, reply)
  return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error{
  //locking?
  //make sure do not reply until I have applied up to config change where config.Num == args.ConfigNum
  DPrintf2("Server %s received GetShard request for the state of config num: %d (gid: %d)!", kv.id, args.ConfigNum, args.GID)
  if args.GID != kv.my_gid[args.ConfigNum]{
    DPrintf2("I DID NOT HAVE THIS GID!")
    reply.Err = ErrWrongGroup
    return nil
  }
  reply.KV = kv.kvs[args.ConfigNum]
  reply.Err = OK
  time.Sleep(1000*time.Millisecond)
  return nil
}

func (kv *ShardKV) performReconfiguration(op *Op) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf2("Server %s Applying Reconfiguration! %v", kv.id, op.ReconfigurationConfig)
  if op.ReconfigurationConfig.Num != kv.curr_config.Num + 1 {
    log.Fatal("Applying a config which is not sequential!", op.ReconfigurationConfig.Num, kv.curr_config.Num + 1)
  }
  old_config := kv.curr_config
  kv.curr_config = op.ReconfigurationConfig

  new_gid := int64(-1)
  new_me := -1
  for gid, replica_groups := range op.ReconfigurationConfig.Groups {
    for idx, socket := range replica_groups {
      if socket == kv.socket_me{
        new_gid = gid
        new_me = idx
        break
      }
    }
  }

  if new_gid != kv.gid{
    kv.gid = new_gid
    DPrintf2("Server %s has changed its role in this Config! My new gid is %v", kv.id, kv.gid)

    // do state transfer
    // modify to get correct shards, not just old group!
    servers, ok := old_config.Groups[kv.gid]
    args := &GetShardArgs{ConfigNum: old_config.Num}
    var reply GetShardReply
    if ok { // there is state to get!
      DPrintf2("Server %s is getting state from member of new replica group %v!", kv.id, kv.gid)
      for _, srv := range servers {
        ok := call(srv, "ShardKV.GetShard", args, reply)
        if ok && reply.Err == OK{
          break
        }
      }
      DPrintf3("Server %s has finished getting state from member of new replica group! %v", kv.id, kv.gid)
      if reply.Err != OK{
        log.Fatal("Server could not get state because none of the pinged servers were in the replica group!")
      }
      kv.kvs[op.ReconfigurationConfig.Num] = reply.KV
    } else { //there is no state to get!
      DPrintf3("Server %s has no old state from replica group %v! Initializing state", kv.id, kv.gid)
      kv.kvs[op.ReconfigurationConfig.Num] = make(map[string]string)
    }
    if kv.gid != -1 { // we have an actual gid
      //change paxos groups
      DPrintf3("Server %s has changed groups, must make new paxos group!", kv.id)
      rpcs := rpc.NewServer()
      rpcs.Register(kv)

      kv.px = paxos.Make(op.ReconfigurationConfig.Groups[kv.gid], new_me, rpcs)
      fmt.Println(old_config.Groups)
    }  

  }
}

func (kv *ShardKV) startReconfiguration(new_config shardmaster.Config){
  DPrintf2("Server %s starting a new reconfiguration! new config: %d. Max in log: %d", kv.id, new_config.Num, kv.max_config_in_log)
  op := &Op{Type: Reconfiguration, ReconfigurationConfig: new_config, ClientID: 0, SeqNum: int64(new_config.Num)}
  _, handled := kv.sequenceHandled(op.ClientID, op.SeqNum)
  if handled{
    DPrintf2("Reconfig already handled!")
  } else{
    kv.putOpInLog(op)
  }
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  DPrintf("Server %s called tick function! Current max_config_in_log is %d", kv.id, kv.max_config_in_log)
  new_config := kv.sm.Query(-1)
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.max_config_in_log = int(math.Max(float64(kv.max_config_in_log), float64(kv.curr_config.Num)))
  if new_config.Num > kv.max_config_in_log { // change this to kv.max_config_in_log
    DPrintf2("Server %s Configuration is old! Putting configs from %d to %d in log", kv.id, kv.max_config_in_log +1, new_config.Num)
    for config_num := kv.max_config_in_log +1; config_num <= new_config.Num; config_num++{
      kv.max_config_in_log = config_num
      kv.mu.Unlock()
      kv.startReconfiguration(kv.sm.Query(config_num))
      kv.mu.Lock()
    }    
  }
  // DPrintf("Server %s ending tick function", kv.id)
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(GetArgs{})
  gob.Register(GetReply{})
  gob.Register(PutArgs{})
  gob.Register(PutReply{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.socket_me = servers[me]
  kv.id = kv.socket_me[len(kv.socket_me) -1 :]
  kv.kvs = make(map[int]map[string]string)
  kv.kvs[-1] = make(map[string]string)
  kv.client_last_op = make(map[int64]*Op)
  kv.client_max_seq = make(map[int64]int64)
  kv.request_noop_channel = make(chan int)
  kv.curr_config = shardmaster.Config{Num:-1}
  kv.max_config_in_log = -1
  

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
