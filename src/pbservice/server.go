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
const Debug = 1

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
	am_i_primary bool
	am_i_backup bool
	kv map[string]string
}


func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	if pb.am_i_primary {
		DPrintf("server received put request! DoHash? %t", args.DoHash)
		if args.DoHash{
			prev_val, exists := pb.kv[args.Key]
			if exists == false {
				prev_val = ""
			}
			pb.kv[args.Key] = strconv.Itoa(int(hash(prev_val + args.Value)))
			reply.PreviousValue = prev_val
			
		} else{
			pb.kv[args.Key] = args.Value
		}
		reply.Err = OK
	} else {
		DPrintf("I %s am not primary and cannot put this!", pb.me)
		reply.Err = ErrWrongServer
	}	
	return nil
}


// type GetArgs struct {
//   Key string
//   // You'll have to add definitions here.
// }

// type GetReply struct {
//   Err Err
//   Value string
// }
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	if pb.am_i_primary {
		DPrintf("server received get request!")
		reply.Value = pb.kv[args.Key]
		reply.Err = OK
	} else {
		DPrintf("I %s am not primary and cannot get this!", pb.me)
		reply.Err = ErrWrongServer
	}
	return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
	DPrintf("%s Pinging VS with viewnum: %d", pb.me, pb.myview.Viewnum)
	View, error := pb.vs.Ping(pb.myview.Viewnum)
	if error != nil{
		DPrintf("Received error in ping! %s", error)
	} else if View != pb.myview{
		pb.myview = View

		DPrintf("NEW view received from vs: %d", View.Viewnum)
		if pb.myview.Primary == pb.me{
			DPrintf("vs says I am primary! %s", pb.me)
			pb.am_i_primary = true
			if pb.myview.Backup != ""{
				DPrintf("vs says I am primary %s! and need to transfer state to: %s", pb.me, pb.myview.Backup)
				//vs says that there is a backup: need to transfer state- wait for reply before moving on
				// pb.myview.Backup
			}
		} else if pb.myview.Backup == pb.me{
			DPrintf("vs says I am backup! %s", pb.me)
			pb.am_i_backup = true

		}
		

	} else {
		DPrintf("OLD view received from vs: %d", View.Viewnum)
	}
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
	pb.am_i_backup = false
	pb.am_i_primary = false
	pb.kv = make(map[string]string)

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
					conn.Close()
				} else if pb.unreliable && (rand.Int63() % 1000) < 200 {
					// process the request but force discard of reply.
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
