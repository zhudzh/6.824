package mapreduce

import "container/list"
import "fmt"
import "time"
// import "reflect"

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


	workers := make(map[WorkerInfo]bool)

	var map_jobs []string
	map_jobs = make([]string, mr.nMap, mr.nMap)

	var reduce_jobs []string
	reduce_jobs = make([]string, mr.nReduce, mr.nReduce)



	get_job := func(jobs_array []string) int{
		//return first empty string, then return fist pending string
		for idx, _ := range jobs_array{
			if jobs_array[idx] == "" {
				return idx
			}
		}
		for idx, _ := range jobs_array{
			if jobs_array[idx] == "pending" {
				return idx
			}
		}
		return -1 // all strings done
	}
	send_job := func(worker_info WorkerInfo, job JobType, reply DoJobReply) DoJobReply{
		//initialize args
		args := new(DoJobArgs)
		args.File = mr.file
		args.Operation = job
		switch job {
			case Map:
				args.JobNumber = get_job(map_jobs)
				map_jobs[args.JobNumber] = "pending"
				args.NumOtherPhase = mr.nReduce
			case Reduce:
				args.JobNumber = get_job(reduce_jobs)
				reduce_jobs[args.JobNumber] = "pending"
				args.NumOtherPhase = mr.nMap
		}

		call(worker_info.address, "Worker.DoJob", args, &reply)
		if reply.OK {
			switch job {
			case Map:
				map_jobs[args.JobNumber] = "done"
			case Reduce:
				reduce_jobs[args.JobNumber] = "done"
			}
		} else {
			switch job {
			case Map:
				if map_jobs[args.JobNumber] == "pending"{
					map_jobs[args.JobNumber] = ""
				}
				
			case Reduce:
				if reduce_jobs[args.JobNumber] == "pending"{
					reduce_jobs[args.JobNumber] = ""
				}
			}
		}
		return reply
	}

	go func() {
		for {
			worker_info := WorkerInfo{address : <- mr.registerChannel}
			// fmt.Println("registered: ", worker_info)
			workers[worker_info] = true
			go func(){
				for get_job(map_jobs) != -1{
					var reply DoJobReply //initialize reply
					reply = send_job(worker_info, Map, reply)
					if ! reply.OK {
						delete(workers, worker_info)
						break
					}
				}
			}()
		}
	}()
	
	for get_job(map_jobs) != -1{
		time.Sleep(600)
	}

	// fmt.Println("Maps are done!")

	go func() {
		for {
			worker_info := WorkerInfo{address : <- mr.registerChannel}
			fmt.Println("registered: ", worker_info)
			// workers[worker_info] = true
			go func(){
				for get_job(reduce_jobs) != -1{
					var reply DoJobReply //initialize reply
					reply = send_job(worker_info, Reduce, reply)
					if ! reply.OK {
						delete(workers, worker_info)
						break
					}
				}
			}()
		}
	}()

	go func() {
		for worker_info, _ := range workers {
			fmt.Println("using worker: ", worker_info)
			go func(){
				for get_job(reduce_jobs) != -1{
					var reply DoJobReply //initialize reply
					reply = send_job(worker_info, Reduce, reply)
					if ! reply.OK {
						delete(workers, worker_info)
						break
					}
				}
			}()
		}
	}()
	
	// fmt.Println(reduce_jobs)
	for get_job(reduce_jobs) != -1{
		time.Sleep(600)
	}

	for worker_info, _ := range workers {
			fmt.Println("reg workers: ", worker_info)
	}


	return mr.KillWorkers()
}
