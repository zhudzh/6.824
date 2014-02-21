
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format + "\n", a...)
	}
	return
}

type ViewServer struct {
	mu sync.Mutex
	l net.Listener
	dead bool
	me string


	// Your declarations here.
	view View
	has_heard_from_primary bool
	missing_backup_pings int
	missing_primary_pings int
	missing_secondary_backup_pings int
	secondary_backup string
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if args.Viewnum == 0 && vs.view.Primary == "" {
		//first time- initialize this machine to primary
		DPrintf(" Message at VS: No primary! primary is now me %s", args.Me)
		vs.view = View{Viewnum: vs.view.Viewnum + 1, Primary: args.Me, Backup: ""}
		vs.has_heard_from_primary = false

	} else if args.Me == vs.view.Primary && args.Viewnum == vs.view.Viewnum && ! vs.has_heard_from_primary {
		//primary has acknowledged new responsibility -> OK to change view
		DPrintf(" Message at VS: Primary just acknowledged")
		vs.has_heard_from_primary = true

	} else if args.Me == vs.view.Primary && args.Viewnum == 0 && vs.has_heard_from_primary{
		//primary has crashed -> backup becomes primary
		DPrintf(" Message at VS: primary crashed! New Primary ", args.Me)
		vs.view = View{Viewnum: vs.view.Viewnum + 1, Primary: vs.view.Backup, Backup: ""}
		if vs.secondary_backup != ""{
			vs.view.Backup = vs.secondary_backup
			vs.secondary_backup = ""
		}
		vs.has_heard_from_primary = false

	} else if args.Me == vs.view.Backup && args.Viewnum == 0 && vs.has_heard_from_primary{
		//backup has crashed -> remove backup
		DPrintf(" Message at VS: backup crashed! New Backup from secondary ", vs.secondary_backup)
		vs.view = View{Viewnum: vs.view.Viewnum + 1, Primary: vs.view.Primary, Backup: ""}
		if vs.secondary_backup != ""{
			vs.view.Backup = vs.secondary_backup
			vs.secondary_backup = ""
		}
		vs.has_heard_from_primary = false

	} else if vs.view.Backup == "" && vs.view.Primary != args.Me && vs.has_heard_from_primary{
		//there is no backup -> initialize this machine to backup
		DPrintf(" Message at VS: No backup! backup is now me ", args.Me)
		vs.view = View{Viewnum: vs.view.Viewnum + 1, Primary: vs.view.Primary, Backup: args.Me}
		vs.has_heard_from_primary = false

	} else if vs.secondary_backup == "" && args.Me != vs.view.Primary && args.Me != vs.view.Backup {
		//there is no secondary backup -> add secondary backup
		DPrintf(" Message at VS: No secondary backup! secondary backup is now me", args.Me)
		vs.secondary_backup = args.Me
		vs.missing_secondary_backup_pings = 0
	} else {
		// DPrintf(" Message at VS: default case", args.Me, vs.view.Viewnum, vs.view.Primary, vs.view.Backup)
		DPrintf(" Message at VS: default case received by %s", args.Me)

	}

	if args.Me == vs.view.Backup{
		vs.missing_backup_pings = 0
	} else if args.Me == vs.view.Primary{
		vs.missing_primary_pings = 0
	} else if args.Me == vs.secondary_backup{
		vs.missing_secondary_backup_pings = 0
	}

	reply.View = vs.view

	return nil


}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	reply.View = vs.view
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.missing_backup_pings++
	vs.missing_primary_pings++
	vs.missing_secondary_backup_pings++

	if vs.missing_primary_pings >= DeadPings && vs.missing_backup_pings >= DeadPings && vs.view.Primary != "" {
		DPrintf("both primary and secondary crashed due to timeout")
	}

	if vs.missing_secondary_backup_pings >= DeadPings {
		vs.secondary_backup = ""
	}

	if vs.missing_backup_pings >= DeadPings && vs.has_heard_from_primary && vs.view.Backup != ""{
		//backup has crashed -> remove backup
		DPrintf(" Message at VS: backup %s crashed due to timeout!", vs.view.Backup)
		vs.view = View{Viewnum: vs.view.Viewnum + 1, Primary: vs.view.Primary, Backup: ""}
		if vs.secondary_backup != ""{
			vs.view.Backup = vs.secondary_backup
			vs.secondary_backup = ""
		}
		vs.has_heard_from_primary = false
		// DPrintf(v Message at VS: s.view.Viewnum, vs.view.Primary, vs.view.Backup)

	} else if vs.missing_primary_pings >= DeadPings && vs.has_heard_from_primary && vs.view.Primary != ""{
		//primary has crashed -> backup becomes primary
		DPrintf(" Message at VS: primary %s crashed due to timeout!", vs.view.Primary)
		vs.view = View{Viewnum: vs.view.Viewnum + 1, Primary: vs.view.Backup, Backup: ""}
		if vs.secondary_backup != ""{
			vs.view.Backup = vs.secondary_backup
			vs.secondary_backup = ""
		}
		vs.has_heard_from_primary = false
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}



func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.has_heard_from_primary = true
	vs.missing_backup_pings = 0
	vs.missing_primary_pings = 0
	vs.missing_secondary_backup_pings = 0
	vs.secondary_backup = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me);
	if e != nil {
		log.Fatal("listen error: ", e);
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
