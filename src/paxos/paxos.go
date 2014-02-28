package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
// import "reflect"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format + "\n", a...)
	}
	return
}

const (
	PrepareOK = "PrepareOK"
	PrepareReject = "PrepareReject"
	AcceptOK = "AcceptOK"
	AcceptReject = "AcceptReject"
)

type Reply string

type PrepareArgs struct {
	PrepareNumber int
	Seq int
}

type PrepareReply struct {
	HighestNumberAccepted int
	ValueAccepted interface{}
	Reply Reply
	MyDone int
}

type AcceptArgs struct {
	AcceptNumber int
	AcceptValue interface{}
	Seq int
}

type AcceptReply struct {
	AcceptNumber int
	Reply Reply
	MyDone int
}

type DecidedArgs struct {
	DecidedValue interface{}
	Seq int
}

type DecidedReply struct {
}


type PaxosInstance struct{
	seq int
	n_p, n_a int
	v_a, v_decided interface{}

}

type Paxos struct {
	mu sync.Mutex
	l net.Listener
	dead bool
	unreliable bool
	rpcCount int
	peers []string
	me int // index into peers[]

	// Your data here.
	paxos_instances map[int] *PaxosInstance
	done_array []int
}


func (px *Paxos) generateN() int {
	return (int(time.Now().Unix()) << 8) + px.me
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	DPrintf("received a prepare message! at %d for seq %d", px.me, args.Seq)
	_, exists := px.paxos_instances[args.Seq]
	if ! exists{
		px.makePaxosInstance(args.Seq)
	}
	if args.PrepareNumber > px.paxos_instances[args.Seq].n_p {
		px.paxos_instances[args.Seq].n_p = args.PrepareNumber
		reply.Reply = PrepareOK
		reply.HighestNumberAccepted = px.paxos_instances[args.Seq].n_a
		reply.ValueAccepted = px.paxos_instances[args.Seq].v_a
	} else {
		reply.Reply = PrepareReject
	}
	reply.MyDone = px.myDone()
	DPrintf("reply of server %d for seq %d is %s", px.me, args.Seq, reply.Reply)
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	DPrintf("received an accept message! at %d for seq %d", px.me, args.Seq)
	_, exists := px.paxos_instances[args.Seq]
	if ! exists{
		px.makePaxosInstance(args.Seq)
	}
	if args.AcceptNumber >= px.paxos_instances[args.Seq].n_p {
		px.paxos_instances[args.Seq].n_p = args.AcceptNumber
		px.paxos_instances[args.Seq].n_a = args.AcceptNumber
		px.paxos_instances[args.Seq].v_a = args.AcceptValue
		reply.Reply = AcceptOK
	} else {
		reply.Reply = AcceptReject
	}
	reply.MyDone = px.myDone()
	DPrintf("reply of server %d for seq %d is %s", px.me, args.Seq, reply.Reply)
	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error{
	DPrintf("received an decided message! at %d for seq %d", px.me, args.Seq)
	_, exists := px.paxos_instances[args.Seq]
	if ! exists{
		px.makePaxosInstance(args.Seq)
	}
	px.paxos_instances[args.Seq].v_decided = args.DecidedValue
	return nil
}

func (px *Paxos) handleDone(min_seq int){
	for seq, _ := range px.paxos_instances {
		if seq < min_seq {
			delete(px.paxos_instances, seq)
		}
	}

}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	DPrintf("Start method called! seq: %d, proposed value: %s", seq, v)
	DPrintf("I am %d of my peers: %s", px.me, px.peers)

	_, exists := px.paxos_instances[seq]
	if ! exists{
		DPrintf("Start method called, but paxos instance already exists! seq: %d, proposed value: %s", seq, v)
		px.makePaxosInstance(seq)
	}

	done, _ := px.Status(seq)


	go func(decided bool) {
		for ! decided {
			n := px.generateN()

			highest_n_a := 0
			v_prime := v
			prepare_oks := 0
			for idx, peer := range px.peers{
				args := &PrepareArgs{PrepareNumber: n, Seq: seq}
				var reply PrepareReply
				DPrintf("Sending prepare message to %d with seq %d with prepare number %d", idx, seq, n)
				if idx == px.me {
					px.Prepare(args, &reply)
				} else {
					call(peer, "Paxos.Prepare", args, &reply)
				}
				if reply.Reply == PrepareOK{
					DPrintf("Processed a PrepareOK at %d with seq %d", idx, seq)
					prepare_oks++
					if reply.HighestNumberAccepted > highest_n_a {
						highest_n_a = reply.HighestNumberAccepted
						v_prime = reply.ValueAccepted
					}
				}
				px.done_array[idx] = reply.MyDone
			}
			if prepare_oks < len(px.peers)/ 2 + 1 {
				DPrintf("Server %d with seq %d did not receive a majority of prepareOK statements! Retry with different n", px.me, seq)
				time.Sleep(time.Second)
				continue
			}
			
			DPrintf("Sucessfully passed the Prepare Stage with seq %d, value %v, highest_n_a %d, and n %d", seq, v_prime, highest_n_a, n)

			accept_oks := 0
			for idx, peer := range px.peers{
				args := &AcceptArgs{AcceptNumber: n, AcceptValue: v_prime, Seq: seq}
				var reply AcceptReply
				DPrintf("Sending accept message to %d with seq %d with accept number %d", idx, seq, n)
				if idx == px.me {
					px.Accept(args, &reply)
				} else {
					call(peer, "Paxos.Accept", args, &reply)
				}
				if reply.Reply == AcceptOK{
					DPrintf("Processed a AcceptOK at %d with seq %d", idx, seq)
					accept_oks++
				}
				px.done_array[idx] = reply.MyDone
			}

			if accept_oks < len(px.peers)/ 2 + 1 {
				DPrintf("Server %d with seq %d did not receive a majority of AcceptOK statements! Retry with different n", px.me, seq)
				time.Sleep(time.Second)
				continue
			}

			DPrintf("Sucessfully passed the Accept Stage with seq %d, value %v, and n %d", seq, v_prime,  n)
			decided = true

			for idx, peer := range px.peers{
				args := &DecidedArgs{DecidedValue: v_prime, Seq: seq}
				var reply DecidedReply
				DPrintf("Sending decided message to %d with seq %d with decided value %v", idx, seq, v_prime)
				if idx == px.me {
					px.Decided(args, &reply)
				} else {
					call(peer, "Paxos.Decided", args, &reply)
				}
			}
		}
	}(done)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	DPrintf("application has called Done method on %d with seq %d ", px.me, seq)
	if seq > px.done_array[px.me] {
		px.done_array[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	max_seq := -1
	for seq, _ := range px.paxos_instances {
		if seq > max_seq{
			max_seq = seq
		}
	}
	DPrintf("application has called Max method on %d. Answer is %d ", px.me, max_seq)
	return max_seq
}

//returns the paxos's instance highest done seq number
func (px *Paxos) myDone() int{
	return px.done_array[px.me]
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
	min_zi := 2147483647 // max int32
	for _, z_i := range px.done_array {
		if z_i < min_zi{
			min_zi = z_i
		}
	}
	DPrintf("application has called Min method on %d . Array is %v, Answer is %d", px.me, px.done_array, min_zi + 1)
	px.handleDone(min_zi)
	return min_zi + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	_, exists := px.paxos_instances[seq]
	if ! exists{
		DPrintf("application has called Status method on %d with seq %d. Answer is %v %t", px.me, seq, nil, false)
		return false, nil
	}
	value := px.paxos_instances[seq].v_decided
	exists = value != nil
	DPrintf("application has called Status method on %d with seq %d. Answer is %v %t", px.me, seq, value, exists)
	return exists, value
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()
		
	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// make a paxos instance with a sequence number
func (px *Paxos) makePaxosInstance(seq int){
	DPrintf("Making a new paxos instance for peer %d with seq %d", px.me, seq)
	pxi := &PaxosInstance{}
	pxi.n_p, pxi.n_a, pxi.v_a, pxi.v_decided = 0, 0, nil, nil

	px.paxos_instances[seq] = pxi
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	DPrintf("Make method called. I am %d with peers %s", me, peers)
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.done_array = make([]int, len(peers))
	for idx := range px.done_array{
		px.done_array[idx] = -1
	}

	// Your initialization code here.
	px.paxos_instances = make(map[int] *PaxosInstance)
	

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me]);
		if e != nil {
			log.Fatal("listen error: ", e);
		}
		px.l = l
		
		// please do not change any of the following code,
		// or do anything to subvert it.
		
		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63() % 1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63() % 1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
