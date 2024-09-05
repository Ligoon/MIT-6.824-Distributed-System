package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type CompleteTaskReply struct {
	Success bool
}

type CompleteTaskArgs struct {
	WorkerID int
	TaskType string
	// for map task
	FileName string
	// for reduce task
	ReduceIdx int
}

type RequestTaskArgs struct {
	WorkerID int
}

type RequestTaskReply struct {
	TaskType string
	// for map task
	FileName string
	FileIdx int
	NReduce int
	// for reduce task
	ReduceIdx int
	NMap int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
