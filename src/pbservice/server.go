package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format + "\n", a...)
	}
	return
}

type PBServer struct {
	l net.Listener
	dead bool // for testing
	unreliable bool // for testing
	me string
	vs *viewservice.Clerk
	done sync.WaitGroup
	finish chan interface{}
	// Your declarations here.
	myview viewservice.View
	kv map[string]string
	mu sync.Mutex
	client_map map[int64]map[int64]string
}

func (pb *PBServer) SequenceHandled(client_id int64, seq_num int64) bool{
	client, exists := pb.client_map[client_id]
	if exists == false{
		client = make(map[int64]string)
		pb.client_map[client_id] = client
	}
	_, handled := client[seq_num]
	// DPrintf("has sequence been handled? %t", handled)
	return handled
}

// Put RPC handler, primary handles put request from client
func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	handled := pb.SequenceHandled(args.ClientID, args.SeqNum)

	if pb.myview.Primary == pb.me && ! handled {
		DPrintf("server %s received put request %v -> %v! DoHash? %t. Seq #: %d", pb.me, args.Key, args.Value, args.DoHash, args.SeqNum)
		if args.DoHash{
			prev_val, exists := pb.kv[args.Key]
			if exists == false {
				prev_val = ""
			}
			args.Value = strconv.Itoa(int(hash(prev_val + args.Value))) // new value is the hash of the prev_val and value
			reply.PreviousValue = prev_val
		}
		pb.kv[args.Key] = args.Value
		reply.Err = OK
		pb.client_map[args.ClientID][args.SeqNum] = reply.PreviousValue
		if pb.myview.Backup != "" && ! handled{
			pb.PutUpdateRequest(args.Key, args.Value, args.ClientID, args.SeqNum)
		}
	} else if ! (pb.myview.Primary == pb.me) {
		DPrintf("I %s am not primary and cannot put this!", pb.me)
		reply.Err = ErrWrongServer
	} else {
		DPrintf("sequence %d, client %d has already been handled ", args.SeqNum, args.ClientID)
		reply.Err = OK
		reply.PreviousValue = pb.client_map[args.ClientID][args.SeqNum]
	}
	return nil
}

// PutUpdate RPC handler, backup handles putupdate request from primary
func (pb * PBServer) PutUpdate(args *PutUpdateArgs, reply *PutUpdateReply) error{
	if pb.myview.Backup == pb.me{
		DPrintf("server %s received putupdate request %v -> %v! State is %v", pb.me, args.Key, args.Value, args.Value)
		pb.kv[args.Key] = args.Value
		pb.SequenceHandled(args.ClientID, args.SeqNum)
		pb.client_map[args.ClientID][args.SeqNum] = args.Value
		reply.Err = OK
	} else {
		DPrintf("I %s am not backup and cannot get this!", pb.me)
		reply.Err = ErrWrongServer
	}
	return nil
}
// PutUpdateRequest function- primary calls to send RPC call to backup
func (pb *PBServer) PutUpdateRequest(key string, value string, client_id int64, seq_num int64) {
	DPrintf("server %s sending putupdate request %v -> %v!", pb.me, key, value)
	args := &PutUpdateArgs{Key: key, Value: value, ClientID: client_id, SeqNum: seq_num}
	var reply PutUpdateReply
	ok := call(pb.myview.Backup, "PBServer.PutUpdate", args, &reply)
	for ok == false || reply.Err != OK{
		DPrintf("primary received error from backup %s when trying send putupdate! OK: %t reply.Err: %s", pb.myview.Backup, ok, reply.Err)
		time.Sleep(viewservice.PingInterval)
		pb.tick()
		ok = call(pb.myview.Backup, "PBServer.PutUpdate", args, &reply)
		
	}
	return
}

// Get RPC handler, primary handles get request from client
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.myview.Primary == pb.me{
		handled := pb.SequenceHandled(args.ClientID, args.SeqNum)
		DPrintf("server %s received get request!", pb.me)
		reply.Value = pb.kv[args.Key]
		reply.Err = OK
		pb.client_map[args.ClientID][args.SeqNum] = reply.Value
		if pb.myview.Backup != "" && ! handled{
			pb.GetUpdateRequest(args.Key, reply.Value, args.ClientID, args.SeqNum)
		}
		DPrintf("get request is %v -> %v", args.Key, reply.Value)
	} else {
		DPrintf("I %s am not primary and cannot get this!", pb.me)
		reply.Err = OK
		reply.Value = pb.kv[args.Key]
	} 
	return nil
}


// GetUpdate RPC handler, backup handles getupdate request from primary
func (pb * PBServer) GetUpdate(args *GetUpdateArgs, reply *GetUpdateReply) error{
	if pb.myview.Backup == pb.me{
		DPrintf("server %s received getupdate request %v! state is %v", pb.me, args.Key, args.Value)
		pb.SequenceHandled(args.ClientID, args.SeqNum)
		pb.client_map[args.ClientID][args.SeqNum] = args.Value
		reply.Err = OK
	} else {
		DPrintf("I %s am not backup and cannot get this!", pb.me)
		reply.Err = ErrWrongServer
	}
	return nil
}
// GetUpdateRequest function- primary calls to send RPC call to backup
func (pb *PBServer) GetUpdateRequest(key string, value string, client_id int64, seq_num int64) {
	DPrintf("server %s sending getupdate request %v!", pb.me, key)
	args := &GetUpdateArgs{Key: key, Value: value, ClientID: client_id, SeqNum: seq_num}
	var reply GetUpdateReply
	ok := call(pb.myview.Backup, "PBServer.GetUpdate", args, &reply)
	for ok == false || reply.Err != OK{
		DPrintf("primary received error from backup when trying send getupdate! OK: %t reply.Err: %s", ok, reply.Err)
		time.Sleep(viewservice.PingInterval)
		pb.tick()
		ok = call(pb.myview.Backup, "PBServer.GetUpdate", args, &reply)	
		
	}
	return
}



// ping the viewserver periodically.
func (pb *PBServer) tick() {
	DPrintf("%s Pinging VS with viewnum: %d", pb.me, pb.myview.Viewnum)
	View, error := pb.vs.Ping(pb.myview.Viewnum)
	if error != nil{
		DPrintf("Received error in ping! %s", error)
	} else if View != pb.myview{
		pb.myview = View
		DPrintf("NEW view received from vs: %d %s %s", View.Viewnum, View.Primary, View.Backup)
		if pb.myview.Primary == pb.me{
			DPrintf("vs says I am primary! %s", pb.me)
			if pb.myview.Backup != ""{
				DPrintf("vs says I am primary %s! and need to transfer state to: %s", pb.me, pb.myview.Backup)
				//vs says that there is a backup: need to transfer state- wait for reply before moving on
				pb.TransferStateRequest()
				pb.vs.Ping(pb.myview.Viewnum)
			}
		} else if pb.myview.Backup == pb.me{
			DPrintf("vs says I am backup! %s", pb.me)
		}
		

	} else {
		DPrintf("OLD view received from vs: %d", View.Viewnum)
	}
}


func (pb *PBServer) TransferStateRequest() {
	DPrintf("primary is attempting to transfer state to backup! state is : %v %v", pb.kv, pb.client_map)
	args := &StateTransferArgs{Kv: pb.kv, ClientMap: pb.client_map}
	var reply StateTransferReply
	ok := call(pb.myview.Backup, "PBServer.TransferState", args, &reply)
	for (ok == false || reply.Err != OK) && pb.myview.Backup != ""{
		DPrintf("primary received error from backup when trying to transfer state! OK: %t reply.Err: %s", ok, reply.Err)
		time.Sleep(viewservice.PingInterval)
		pb.tick()
		ok = call(pb.myview.Backup, "PBServer.TransferState", args, &reply)	
		
	}
	return
}

func (pb *PBServer) TransferState(args *StateTransferArgs, reply *StateTransferReply) error {
	if pb.myview.Backup == pb.me {
		DPrintf("backup received transfer state request!")
		pb.kv = args.Kv
		pb.client_map = args.ClientMap
		reply.Err = OK
		DPrintf("new state is : %v %v", pb.kv, pb.client_map)
	} else {
		DPrintf("I %s am not backup and cannot get this transfer!", pb.me)
		reply.Err = ErrWrongServer
	}
	return nil
}



// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.myview = viewservice.View{0, "", ""}
	pb.kv = make(map[string]string)
	pb.client_map = make(map[int64]map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me);
	if e != nil {
		log.Fatal("listen error: ", e);
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63() % 1000) < 100 {
					// discard the request.
					DPrintf("request is being discarded!")
					conn.Close()
				} else if pb.unreliable && (rand.Int63() % 1000) < 200 {
					// process the request but force discard of reply.
					DPrintf("reply is being discarded!")
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait() 
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
