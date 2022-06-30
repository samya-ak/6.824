package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	job := RequestMapJob()
	intFiles := Map(job.InputFile, job.ReducerCount, job.MapJobNumber, mapf)
	if len(intFiles) > 0 {
		ReportMapJobCompleted(job.InputFile, intFiles)
	}
}

func Map(filename string, reduceCount int, mapJobNo int, mapf func(string, string) []KeyValue) []string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	partitionedKva := make([][]KeyValue, reduceCount)
	for _, v := range kva {
		partitionKey := ihash(v.Key) % reduceCount
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}
	intermediatefiles := make([]string, 0)
	for i, v := range partitionedKva {
		// mr - map job number - partition key
		intermediateFile := fmt.Sprintf("mr-%d-%d", mapJobNo, i)
		ofile, _ := os.Create(intermediateFile)
		enc := json.NewEncoder(ofile)
		for _, kv := range v {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("error %+v", err)
				break
			}
		}
		intermediatefiles = append(intermediatefiles, intermediateFile)
	}
	return intermediatefiles
}

func RequestMapJob() MapJob {
	args := ExampleArgs{}

	reply := MapJob{}
	call("Coordinator.GetMapJob", &args, &reply)
	fmt.Printf("reply--> %+v \n\n", reply)
	return reply
}

func ReportMapJobCompleted(filename string, intFiles []string) MapJobCompletedRes {
	args := MapJobCompleted{}
	args.InputFile = filename
	args.IntermediateFiles = intFiles
	reply := MapJobCompletedRes{}
	call("Coordinator.MapJobCompleted", &args, &reply)
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
