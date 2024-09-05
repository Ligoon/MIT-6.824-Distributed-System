package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
// import "fmt"

const WAIT_TIME = 10

var mutex = sync.Mutex{}
var wg = sync.WaitGroup{}

type Coordinator struct {
	// Your definitions here.
	map_tasks map[string]int // key: file name, value: 0: undo, 1: doing, 2: finished
	map_workers map[string]int // key: file name, value: corresponding worker id for the task
	files_idx map[string]int

	reduce_tasks map[int]int // key: nReduce num, value: 0: undo, 1: doing, 2: finished
	reduce_workers map[int]int // key: nReduce num, value: corresponding worker id for the task

	status string
	nReduce int
	nMap int
}

func (c *Coordinator) init(files []string, nReduce int) {
	c.status = "Map"
	c.nReduce = nReduce
	c.nMap = len(files)
	c.map_tasks = make(map[string]int)
	c.map_workers = make(map[string]int)
	c.files_idx = make(map[string]int)
	c.reduce_tasks = make(map[int]int)
	c.reduce_workers = make(map[int]int)
	i := 0
	for _, f := range files {
		c.map_tasks[f] = 0
		c.map_workers[f] = -1
		c.files_idx[f] = i
		i++
	}
	for i=0; i<nReduce; i++ {
		c.reduce_tasks[i] = 0
		c.reduce_workers[i] = -1
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	mutex.Lock()
	if args.TaskType == "Map" {
		if c.map_tasks[args.FileName] == 1 && c.map_workers[args.FileName] == args.WorkerID {
			c.map_tasks[args.FileName] = 2
			reply.Success = true
			// fmt.Printf("[Coordinator]: Map Complete, file name: %v, worker ID: %v\n", args.FileName, args.WorkerID)
		} else {
			reply.Success = false
			mutex.Unlock()
			return nil
		}
		for _, status := range c.map_tasks {
			if status != 2 {
				mutex.Unlock()
				return nil
			}
		}
		if c.status == "Map" {
			c.status = "Reduce"
		}
		mutex.Unlock()
		return nil
	} else if args.TaskType == "Reduce" {
		if c.reduce_tasks[args.ReduceIdx] == 1 && c.reduce_workers[args.ReduceIdx] == args.WorkerID {
			c.reduce_tasks[args.ReduceIdx] = 2
			reply.Success = true
			// fmt.Printf("[Coordinator]: Reduce Complete, reduce index: %v, worker ID: %v\n", args.ReduceIdx, args.WorkerID)
		} else {
			reply.Success = false
			mutex.Unlock()
			return nil
		}
		for _, status := range c.reduce_tasks {
			if status != 2 {
				mutex.Unlock()
				return nil
			}
		}
		if c.status == "Reduce" {
			c.status = "Done"
		}
		mutex.Unlock()
		return nil
	}
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	mutex.Lock()
	if c.status == "Map" {
		for file, status := range c.map_tasks {
			if status == 0 {
				reply.TaskType = "Map"
				reply.FileName = file
				reply.FileIdx = c.files_idx[file]
				reply.NReduce = c.nReduce
				c.map_tasks[file] = 1 // set to executing
				c.map_workers[file] = args.WorkerID
				wg.Add(1)
				go c.waitMapTask(file)
				
				mutex.Unlock()
				return nil
			}
		}
		// if map tasks do not finish
		reply.TaskType = "Wait"
		mutex.Unlock()
		return nil
	} else if c.status == "Reduce" {
		for task_num, status := range c.reduce_tasks {
			if status == 0 {
				reply.TaskType = "Reduce"
				reply.ReduceIdx = task_num
				reply.NMap = c.nMap
				c.reduce_tasks[task_num] = 1
				c.reduce_workers[task_num] = args.WorkerID
				wg.Add(1)
				go c.waitReduceTask(task_num)

				mutex.Unlock()
				return nil
			}
		}
	} else if c.status == "Done" {
		reply.TaskType = "Done"
	}
	mutex.Unlock()
	return nil
}

func (c *Coordinator) waitReduceTask(task_num int) {
	time.Sleep(WAIT_TIME * time.Second)
	mutex.Lock()
	if c.reduce_tasks[task_num] == 1 {
		c.reduce_tasks[task_num] = 0
	}
	mutex.Unlock()
	wg.Done()
}

func (c *Coordinator) waitMapTask(fileName string) {
	time.Sleep(WAIT_TIME * time.Second)
	mutex.Lock()
	if c.map_tasks[fileName] == 1 {
		c.map_tasks[fileName] = 0
	}
	mutex.Unlock()
	wg.Done()
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.status == "Done" {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	
	// Your code here.
	c.init(files, nReduce)

	c.server()
	return &c
}
