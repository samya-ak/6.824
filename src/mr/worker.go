package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		job := RequestMapJob()
		if job.AllCompleted {
			break
		}
		if len(job.InputFile) <= 0 {
			time.Sleep(time.Second)
			continue
		}
		intFiles := Map(job.InputFile, job.ReducerCount, job.MapJobNumber, mapf)
		if len(intFiles) > 0 {
			ReportMapJobCompleted(job.InputFile, intFiles)
		}
	}

	for {
		job := RequestReduceJob()
		if job.AllCompleted {
			break
		}

		if len(job.IntermediateFiles) <= 0 {
			time.Sleep(time.Second)
			continue
		}
		Reduce(job.IntermediateFiles, job.PartitionKey, reducef)
		ReportReduceJobCompleted(job.PartitionKey)
	}
}

func Reduce(files []string, partitionKey int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", partitionKey)
	ofile, _ := ioutil.TempFile(".", "temp-")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-X.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	os.Rename(ofile.Name(), oname)
	ofile.Close()
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

func RequestReduceJob() ReduceJob {
	args := ExampleArgs{}

	reply := ReduceJob{}
	call("Coordinator.GetReduceJob", &args, &reply)
	fmt.Printf("reply--> %+v \n\n", reply)
	return reply
}

func RequestMapJob() MapJob {
	args := ExampleArgs{}

	reply := MapJob{}
	call("Coordinator.GetMapJob", &args, &reply)
	fmt.Printf("reply--> %+v \n\n", reply)
	return reply
}

func ReportMapJobCompleted(filename string, intFiles []string) JobCompletedRes {
	args := MapJobCompleted{}
	args.InputFile = filename
	args.IntermediateFiles = intFiles
	reply := JobCompletedRes{}
	call("Coordinator.MapJobCompleted", &args, &reply)
	return reply
}

func ReportReduceJobCompleted(partitionKey int) JobCompletedRes {
	args := ReduceJobCompleted{}
	args.PartitionKey = partitionKey
	reply := JobCompletedRes{}
	call("Coordinator.ReduceJobCompleted", &args, &reply)
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
