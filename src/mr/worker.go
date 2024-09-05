package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapper(reply *RequestTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	// fmt.Printf("[Worker]: file name, worker ID: %v, %v\n", reply.FileName,  os.Getpid())
	storeIntermediateFile(kva, reply.NReduce, reply.FileIdx)
}

func storeIntermediateFile(kva []KeyValue, nReduce int, taskNum int) {
	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduce_idx := ihash(kv.Key) % nReduce
		intermediate[reduce_idx] = append(intermediate[reduce_idx], kv)
	}
	for i:=0; i<nReduce; i++ {
		filename := fmt.Sprintf("mr-%v-%v", taskNum, i)
		file, _ := ioutil.TempFile(".", "*")

		encoder := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			err := encoder.Encode(&kv)
			if err != nil {
				fmt.Println("Error encoding JSON:", err)
				return
			}
		}
		os.Rename(file.Name(), filename)
	}
}

func reducer(reply *RequestTaskReply, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i:=0; i<reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, reply.ReduceIdx)
		file, _ := os.Open(filename)
		
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	storeFinalFile(kva, reply.ReduceIdx, reducef)
	// fmt.Printf("[Worker]: reducer ID, worker ID: %v, %v\n", reply.ReduceIdx,  os.Getpid())
}

func storeFinalFile(intermediate []KeyValue, reduceIdx int, reducef func(string, []string) string) {
	oname := fmt.Sprintf("mr-out-%v", reduceIdx)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[], and print the result to mr-out-X
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply, success := CallRequestTask()
		if !success { return }
		if reply.TaskType == "Map" {
			mapper(reply, mapf)
			CallCompleteTask("Map", reply.FileName, -1)
		} else if reply.TaskType == "Reduce" {
			reducer(reply, reducef)
			CallCompleteTask("Reduce", "", reply.ReduceIdx)
		} else if reply.TaskType == "Done" { return }
	}
}

func CallCompleteTask(taskType string, fileName string, reduceIdx int) (*CompleteTaskReply, bool) {
	args := CompleteTaskArgs{}

	args.WorkerID = os.Getpid()
	args.TaskType = taskType
	args.FileName = fileName
	args.ReduceIdx = reduceIdx

	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	return &reply, ok
}

func CallRequestTask() (*RequestTaskReply, bool) {
	args := RequestTaskArgs{}
	args.WorkerID = os.Getpid()
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	return &reply, ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
