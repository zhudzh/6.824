package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // mr.nMap = nmap
  // mr.nReduce = nreduce
  // mr.file = file
  // mr.MasterAddress = master
  // mr.alive = true
  // mr.registerChannel = make(chan string)
  // mr.DoneChannel = make(chan bool)

  //   File string
  // Operation JobType
  // JobNumber int       // this job's number
  // NumOtherPhase int   // total number of jobs in other phase (map or reduce)

  //only return when all map and reduce tasks have been executed
  // master wants to: listen to channel- when receives connection, send job through RPC
  // quits when done is all finished
  fmt.Println(mr.nMap, mr.nReduce)

  worker_info := WorkerInfo{address : <- mr.registerChannel}

  args := new(DoJobArgs)
  args.File = mr.file
  args.Operation = Map
  args.NumOtherPhase = mr.nReduce

  var reply DoJobReply
  call(worker_info.address, "Worker.DoJob", args, &reply)

  done := <- mr.DoneChannel
  if done {
    return mr.KillWorkers()
  } 
  return mr.KillWorkers()
}
