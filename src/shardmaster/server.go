package shardmaster

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
import cryptorand "crypto/rand"
import "math/big"
import "time"
import "math"


const Debug=0

func (sm *ShardMaster) DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format + "\n", a...)
  }
  return
}

type ShardMaster struct {
  mu sync.Mutex
  state_mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  prev_config int
  next_config int
}


type Op struct {
  Type string
  Args interface{}
  Reply interface{}
  Identifier int64
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62 )
  bigx, _ := cryptorand.Int(cryptorand.Reader, max)
  x := bigx.Int64()
  return x
}

func initializeNewConfig(shards [NShards]int64, groups map[int64][]string) ([NShards]int64, map[int64][]string){
  new_shards := [NShards]int64{}
  new_groups := make(map[int64][]string)

  // copy old shards
  for idx, v := range shards{
    new_shards[idx] = v
  }  
  
  //copy old groups
  for k, v := range groups {
    new_groups[k] = v
  }

  return new_shards, new_groups
}

func (sm *ShardMaster) findMaximum( groups_to_num_shards map[int64]int) (int, int64){
  max_group := int64(0)
  max_number := 0
  for group, num_shards := range groups_to_num_shards{
    if num_shards > max_number{
      max_group = group
      max_number = num_shards
    }
  }
  return max_number, max_group
}

func (sm *ShardMaster) findMiniumum( groups_to_num_shards map[int64]int) (int, int64){
  // sm.DPrintf("Server %d group map: %v", sm.me, groups_to_num_shards)
  min_group := int64(0)
  min_number := NShards*2
  for group, num_shards := range groups_to_num_shards{
    if num_shards < min_number{
      min_group = group
      min_number = num_shards
    }
  }
  return min_number, min_group
}

func (sm *ShardMaster) getMaxNumberShards( groups_to_num_shards map[int64]int) int{
  max_number, _ := sm.findMaximum(groups_to_num_shards)
  return max_number
}

func (sm *ShardMaster) getMinNumberShards( groups_to_num_shards map[int64]int) int{
  min_number, _ := sm.findMaximum(groups_to_num_shards)
  return min_number
}

func (sm *ShardMaster) getLeastLoadedGroup( groups_to_num_shards map[int64]int) int64{
  _, min_group := sm.findMiniumum(groups_to_num_shards)
  return min_group
}

func (sm *ShardMaster) getMostLoadedGroup( groups_to_num_shards map[int64]int) int64{
  _, max_group := sm.findMiniumum(groups_to_num_shards)
  return max_group
}

func (sm *ShardMaster) balance(new_shards [NShards]int64, new_groups map[int64][]string) ([NShards]int64, map[int64][]string){
  max_shards_per_group := int(math.Ceil(float64(NShards) / float64(len(new_groups))))
  min_shards_per_group := int(NShards/ len(new_groups))
  groups_to_num_shards := make(map[int64]int)

  // sm.DPrintf("Max shards per group %v, min : %v", max_shards_per_group, min_shards_per_group)

  // initialize groups_to_num_shards. Get groups from new groups. Get mum_shards from new_shards
  for group, _ := range new_groups {
    groups_to_num_shards[group] = 0
  }
  for _, group := range new_shards{
    _, exists := groups_to_num_shards[group]
    if exists {
      groups_to_num_shards[group]++
    }
  }

  for shard, group := range new_shards{
    if group == 0 {
      min_group := sm.getLeastLoadedGroup(groups_to_num_shards)
      new_shards[shard] = min_group
      groups_to_num_shards[min_group]++
    }
  }

  for sm.getMaxNumberShards(groups_to_num_shards) > max_shards_per_group {
    for shard, group := range new_shards{
      if groups_to_num_shards[group] > max_shards_per_group {
        min_group := sm.getLeastLoadedGroup(groups_to_num_shards)
        new_shards[shard] = min_group
        groups_to_num_shards[min_group]++
        groups_to_num_shards[group]--
      } 
    }
  }

  for sm.getMinNumberShards(groups_to_num_shards) < min_shards_per_group{
    for shard, group := range new_shards{
      if group == sm.getMostLoadedGroup(groups_to_num_shards) {
        min_group := sm.getLeastLoadedGroup(groups_to_num_shards)
        new_shards[shard] = min_group
        groups_to_num_shards[min_group]++
        groups_to_num_shards[group]--
      } 
    }
  }

  actual_max :=sm.getMaxNumberShards(groups_to_num_shards)
  actual_min :=sm.getMinNumberShards(groups_to_num_shards)
  if actual_max> max_shards_per_group || actual_min < min_shards_per_group {
    log.Fatalf("Server %d Uh OH!!!! actual max: %d %d", sm.me, actual_max, max_shards_per_group)
  }

  sm.DPrintf("Number of groups: %d", len(new_groups))

  return new_shards, new_groups
}

func (sm *ShardMaster) performJoin(op *Op) Config{
  
  gid := op.Args.(JoinArgs).GID
  servers := op.Args.(JoinArgs).Servers

  old_config := sm.configs[len(sm.configs)-1]

  sm.DPrintf("Server %d performing join: gid: %d, servers: %s. Old config is %#v", sm.me, gid, servers, old_config)

  new_shards, new_groups := initializeNewConfig(old_config.Shards, old_config.Groups)  
  new_groups[gid] = servers // add new group

  new_shards, new_groups = sm.balance(new_shards, new_groups)  

  new_config := Config{Num: old_config.Num + 1, Shards: new_shards, Groups: new_groups}
  sm.configs = append(sm.configs, new_config)
  sm.DPrintf("Server %d finished perform join! Gid: %v New groups are %#v", sm.me, gid, new_config.Groups)
  return new_config  
}

func (sm *ShardMaster) performLeave(op *Op) Config{
  
  gid := op.Args.(LeaveArgs).GID
  old_config := sm.configs[len(sm.configs)-1]


  sm.DPrintf("Server %d performing leave: gid: %d. Old config is %#v", sm.me, gid, old_config)

  new_shards, new_groups := initializeNewConfig(old_config.Shards, old_config.Groups)
  delete(new_groups, gid) // delete leaving group

  for shard, group := range new_shards{
    if group == gid{
      new_shards[shard] = 0
    }
  }

  new_shards, new_groups = sm.balance(new_shards, new_groups)  

  new_config := Config{Num: old_config.Num + 1, Shards: new_shards, Groups: new_groups}
  sm.configs = append(sm.configs, new_config)
  sm.DPrintf("Server %d finished perform Leave! Gid: %v New groups are %#v", sm.me, gid, new_config.Groups) 
  return new_config 
}

func (sm *ShardMaster) performMove(op *Op) Config{
  gid := op.Args.(MoveArgs).GID
  shard := op.Args.(MoveArgs).Shard
  old_config := sm.configs[len(sm.configs)-1]
  sm.DPrintf("Server %d performing move: gid: %d, shard: %d. Old config is %#v", sm.me, gid, shard, old_config)
  
  new_shards, new_groups := initializeNewConfig(old_config.Shards, old_config.Groups)

  new_shards[shard] = gid

  new_config := Config{Num: old_config.Num + 1, Shards: new_shards, Groups: new_groups}
  sm.configs = append(sm.configs, new_config)
  sm.DPrintf("Server %d finished perform Move! New config is %#v", sm.me, new_config)  
  return new_config
}

func (sm *ShardMaster) performQuery(op *Op) Config{
  conf_num := op.Args.(QueryArgs).Num
  reply := op.Reply.(QueryReply)
  // fmt.Println(len(sm.configs)-1, sm.configs)
  if conf_num == -1 || conf_num > len(sm.configs)-1{
      reply.Config = sm.configs[len(sm.configs)-1 ]
  } else{
    reply.Config = sm.configs[conf_num]
  }
  // sm.reply_channel <- reply
  sm.DPrintf("Server %d performed query: conf_num: %d. Return value is %#v", sm.me, conf_num, reply.Config)
  return reply.Config
}

func (sm *ShardMaster) addtoLog(op Op) error {
  sm.state_mu.Lock()  
  defer sm.state_mu.Unlock()
  for {
    sm.px.Start(sm.next_config, op)
    var received_op Op
    received_op = sm.getValue(sm.next_config).(Op)
    if(received_op.Identifier == op.Identifier){
      break
    } else {
      sm.next_config++
    }
  }
  return nil
}

func (sm *ShardMaster) performOp(op *Op) interface{}{
  var config Config
  switch op.Type{
  case Join:
    config = sm.performJoin(op)
  case Leave:
    config =sm.performLeave(op)
  case Move:
    config = sm.performMove(op)
  case Noop:
    fmt.Println("GOT NOOPS")
  }
  sm.DPrintf("done with perform OP! current_config: %v", sm.next_config)
  return config
}

func (sm *ShardMaster) applyLog() interface {} {
  sm.state_mu.Lock()  
  defer sm.state_mu.Unlock()

  var cur_config Config
  for i := sm.prev_config; i <= sm.next_config; i++ {
    _ = sm.getValue(i)
    var decided, rcv_op = sm.px.Status(i)
    var op *Op
    switch rcv_op.(type){
    case Op:
      temp := rcv_op.(Op)
      op = &temp
    case *Op:
      op = rcv_op.(*Op)
    }
    if decided && op.Type != Query {
      sm.DPrintf("Server %d Applying op %d: %#v ", sm.me, sm.next_config, op)
      cur_config = sm.performOp(op).(Config)
      if(i == sm.next_config) {
        return cur_config 
      }
    } else if decided && op.Type == Query  && i == sm.next_config{
      cur_config = sm.performQuery(op)
    }
  }
  return cur_config
}

func (sm *ShardMaster) getValue(config_num int) interface {} {
  to := 10 * time.Millisecond
  for {
      decided, op_rcv_interface := sm.px.Status(config_num)
    if decided {
        return op_rcv_interface
    }
    time.Sleep(to)
    if to < 10 * time.Second {
        to *= 2
      }
    if(to >= time.Second) {
      sm.px.Start(config_num, Op{Type: Noop})
      time.Sleep(to)
    }
  }
  return nil
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()  
  defer sm.mu.Unlock()
  
  sm.DPrintf("Server %d Handling join request! current config is: %#v ", sm.me, sm.configs[len(sm.configs)-1])
  op := Op{Type: Join, Args: *args, Reply: *reply, Identifier: nrand()}
  
  sm.prev_config = sm.next_config
  sm.addtoLog(op)
  
  sm.applyLog()
  sm.px.Done(sm.next_config)  
  sm.next_config++
  
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  sm.DPrintf("Server %d Handling leave request! current config is: %#v ", sm.me, sm.configs[len(sm.configs)-1])
  op := Op{Type: Leave, Args: *args, Reply: *reply, Identifier: nrand()}
  
  sm.prev_config = sm.next_config
  sm.addtoLog(op)
  
  sm.applyLog()
  sm.px.Done(sm.next_config)  
  sm.next_config++
  
    return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  sm.DPrintf("Server %d Handling move request! current config is: %#v ", sm.me, sm.configs[len(sm.configs)-1])
  op := Op{Type: Move, Args: *args, Reply: *reply, Identifier: nrand()}

  sm.prev_config = sm.next_config
  sm.addtoLog(op)
  
  sm.applyLog()
  sm.px.Done(sm.next_config)  
  sm.next_config++
  
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  sm.DPrintf("Server %d Handling query request! ", sm.me)
  op := Op{Type: Query, Args: *args, Reply: *reply, Identifier: nrand()}
  
  sm.prev_config = sm.next_config
  sm.addtoLog(op)
  
  reply.Config = sm.applyLog().(Config)
  sm.px.Done(sm.next_config)  
  sm.next_config++
  
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(JoinArgs{})
  gob.Register(JoinReply{})
  gob.Register(LeaveArgs{})
  gob.Register(LeaveReply{})
  gob.Register(MoveArgs{})
  gob.Register(MoveReply{})
  gob.Register(QueryArgs{})
  gob.Register(QueryReply{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  
  sm.prev_config = 1
  sm.next_config = 1

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
