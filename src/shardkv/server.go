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

const Debug=-1

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
  kvs [shardmaster.NShards]map[string]string
  client_last_op map[int64]*Op
  client_max_seq map[int64]int64
  request_noop_channel chan int
  state_mu sync.Mutex
  getshard_mu sync.Mutex
  curr_config shardmaster.Config
  handled_shards [shardmaster.NShards]int64
  max_config_in_log int
}

type Op struct {
  Type string
  GetArgs *GetArgs
  GetReply *GetReply
  PutArgs *PutArgs  
  PutReply *PutReply
  ReconfigurationConfig shardmaster.Config
  GetShardArgs *GetShardArgs
  GetShardReply *GetShardReply
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
  if client_id == 0 {
  }
  return &Op{}, false
}

func (kv *ShardKV) performGet(op *Op){
  DPrintf("Server %s Applying Get operation", kv.id)
  op.GetReply.Value = kv.kvs[key2shard(op.GetArgs.Key)][op.GetArgs.Key]
  op.GetReply.Err = OK
}

func (kv *ShardKV) performPut(op *Op) {
  DPrintf("Server %s Applying Put operation. (%d %d) with %s -> %s", kv.id, op.ClientID, op.SeqNum, op.PutArgs.Key, op.PutArgs.Value)
  var new_val string
  if op.PutArgs.DoHash{
    prev_val, exists := kv.kvs[key2shard(op.PutArgs.Key)][op.PutArgs.Key]
    if exists == false {
      prev_val = ""
    }
    new_val = strconv.Itoa(int(hash(prev_val + op.PutArgs.Value))) // new value is the hash of the prev_val and value
    op.PutReply.PreviousValue = prev_val
  } else {
    new_val = op.PutArgs.Value
  }
  kv.kvs[key2shard(op.PutArgs.Key)][op.PutArgs.Key] = new_val
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
    DPrintf("Server %s Applying Noop operation", kv.id)
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
      DPrintf2("Server %s applying reconfig: cli : %v, seq: %v ", kv.id, op.ClientID, op.SeqNum)
      kv.performReconfiguration(op)
    case GetShard:
      DPrintf3("Server %s applying getshard: cli : %v, seq: %v", kv.id, op.ClientID, op.SeqNum)
      kv.performGetShard(op)
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
      DPrintf("%s got here! case channel", kv.id)
      for seq_start := seq; seq_start < max_seq; seq_start++{
        DPrintf("Server %s Proposing noop for paxos instance: %d", kv.id, seq_start)
        kv.px.Start(seq_start, &Op{Type: Noop})
      }  

    default:
      // DPrintf("%s got here! default case", kv.id)
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
  op_in_log := kv.getOpConensus(op, attempted_instance)
  for ! op_in_log{
    attempted_instance++
    DPrintf("Server %s Op %#v was not op_in_log! is trying again with seq %d", kv.id, op, attempted_instance)    
    op_in_log = kv.getOpConensus(op, attempted_instance)
  }
  op.PaxosSeq = attempted_instance

  //now op is in log, must get noops if needed
  time.Sleep(50 * time.Millisecond)
  _, handled := kv.sequenceHandled(op.ClientID, op.SeqNum)
  for ! handled{
    DPrintf("Server %s Waiting for noops for op Type %s for seq %d", kv.id, op.Type, attempted_instance)    
    kv.request_noop_channel <- attempted_instance
    DPrintf("Server %s Sent noop request for op Type %s for seq %d", kv.id, op.Type, attempted_instance) 
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
  if gid != kv.gid || kv.gid != kv.handled_shards[shard]{
    DPrintf2("Server %s: Wrong Server in Get Request! I am gid %v and shard belongs to gid: %v. Or, I have made a commitment to not handle!", kv.id, kv.gid, gid)
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
  if gid != kv.gid || kv.gid != kv.handled_shards[shard]{
    DPrintf2("Server %s: Wrong Server in Put request! I am gid %v and shard belongs to gid: %v. Or, I have made a commitment to not handle!", kv.id, kv.gid, gid)
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
  kv.getshard_mu.Lock()
  defer kv.getshard_mu.Unlock()

  DPrintf2("Server %s received GetShard request for the state of shard %d", kv.id, args.ShardNum)
  if args.GID != kv.gid{
    DPrintf2("REQUESTED FROM WRONG GID!")
    reply.Err = ErrWrongGroup
    return nil
  }

  client_id := int64(1000 + args.ShardNum)
  seq_num := int64(args.ConfigNum)

  handled_op, handled := kv.sequenceHandled(client_id, seq_num)

  if handled{
    if seq_num == kv.client_max_seq[client_id]{ // has already processed 
      DPrintf3("Server %s says op has already been processed and is most recent! %#v", kv.id, handled_op)
      reply.KV = handled_op.GetShardReply.KV
      reply.Err = handled_op.GetShardReply.Err
      DPrintf3("Server %s finished request! Op was duplicate, so returning original reply %#v", kv.id, reply)
      return nil
    } else {
      DPrintf3("Server %s says op has already been processed and is not most recent! %#v", kv.id, handled_op)
      return nil
    }
  }

  reply.KV = kv.kvs[args.ShardNum]
  reply.Err = OK
  op := &Op{Type: GetShard, GetShardArgs: args, GetShardReply: reply, ClientID: client_id, SeqNum: seq_num}
  kv.putOpInLog(op)
  return nil
}

//called in performOp
func (kv *ShardKV) performGetShard(op *Op) {
  if op.GetShardArgs.ConfigNum != kv.curr_config.Num || op.GetShardArgs.GID != kv.gid{
    DPrintf3("Server %s cannot perform GetShard because config has changed: current %d, asked %d or not the right gid me %d, asked %d!", kv.id, kv.curr_config.Num, op.GetShardArgs.ConfigNum, kv.gid, op.GetShardArgs.GID)
  } else {
    DPrintf3("Server %s performing GetShard! Not handling shard : %d", kv.id, op.GetShardArgs.ShardNum)
    kv.handled_shards[op.GetShardArgs.ShardNum] = -1
  }  
}

//helper function for state transfer for Perform reconfig
func (kv *ShardKV) requestShard(shard_num int, old_config shardmaster.Config){
  DPrintf2("Server %s is requesting shard_num %v", kv.id, shard_num)
  // do state transfer
  target_gid := old_config.Shards[shard_num]
  servers, ok := old_config.Groups[target_gid]
  args := &GetShardArgs{ShardNum: shard_num, ConfigNum: old_config.Num, GID: target_gid}
  var reply GetShardReply
  if ok { // there is state to get!
    DPrintf2("Server %s is getting shard num: %v from group: %v!", kv.id, shard_num, target_gid)
    for _, srv := range servers {
      ok := call(srv, "ShardKV.GetShard", args, &reply)
      if ok && reply.Err == OK{
        DPrintf2("Server %s received shard num: %v from group: %v!", kv.id, shard_num, target_gid)
        break
      }
    }
    DPrintf2("Server %s has finished getting state from member of new replica group! %v", kv.id, target_gid)
    if reply.Err != OK{
      log.Fatal("Server could not get state because none of the pinged servers were in the replica group!")
    }
    kv.kvs[shard_num] = reply.KV
  } else { //there is no state to get!
    DPrintf2("Server %s has no old state from replica group %v! Initializing state", kv.id, target_gid)
    kv.kvs[shard_num] = make(map[string]string)
  }
}

//called in performOp
func (kv *ShardKV) performReconfiguration(op *Op) {
  DPrintf2("Server %s Applying Reconfiguration! %v", kv.id, op.ReconfigurationConfig)
  if op.ReconfigurationConfig.Num != kv.curr_config.Num + 1 {
    log.Fatal("Applying a config which is not sequential!", op.ReconfigurationConfig.Num, kv.curr_config.Num + 1)
  }
  old_config := kv.curr_config
  kv.curr_config = op.ReconfigurationConfig
  kv.handled_shards = kv.curr_config.Shards

  for shard_num, gid := range kv.curr_config.Shards {
    if gid == kv.gid && old_config.Shards[shard_num] != kv.gid{ //I own this shard now, but didn't before!
      kv.requestShard(shard_num, old_config)
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
  kv.mu.Lock()
  defer kv.mu.Unlock()

  new_config := kv.sm.Query(-1)
  kv.max_config_in_log = int(math.Max(float64(kv.max_config_in_log), float64(kv.curr_config.Num)))
  if new_config.Num > kv.max_config_in_log { // change this to kv.max_config_in_log
    DPrintf2("Server %s Configuration is old! Putting configs from %d to %d in log", kv.id, kv.max_config_in_log +1, new_config.Num)
    for config_num := kv.max_config_in_log +1; config_num <= new_config.Num; config_num++{
      kv.max_config_in_log = config_num
      kv.startReconfiguration(kv.sm.Query(config_num))
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
func StartServer(gid int64, shardmasters []string, servers []string, me int) *ShardKV {
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
  kv.id = strconv.Itoa(kv.me) + "_" + strconv.Itoa(int(kv.gid))
  kv.kvs = [shardmaster.NShards]map[string]string{}
  for idx, _ := range(kv.kvs) {
    kv.kvs[idx] = make(map[string]string)
  }
  kv.client_last_op = make(map[int64]*Op)
  kv.client_max_seq = make(map[int64]int64)
  kv.request_noop_channel = make(chan int)
  kv.curr_config = shardmaster.Config{Num:0}
  kv.handled_shards = [shardmaster.NShards]int64{}
  kv.max_config_in_log = 0
  

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
