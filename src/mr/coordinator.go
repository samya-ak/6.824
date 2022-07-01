package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Coordinator struct {
	mapStatus         map[string]int
	mapTaskId         int
	reduceStatus      map[int]int
	nReducer          int
	intermediateFiles map[int][]string
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetReduceJob(args *ExampleArgs, reply *ReduceJob) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.reduceStatus {
		if v == 0 {
			c.reduceStatus[k] = 1
			reply.IntermediateFiles = c.intermediateFiles[k]
			reply.PartitionKey = k
			return nil
		}
	}

	reply.AllCompleted = c.isReduceJobsCompleted()
	fmt.Printf("RedStatus>>>> %+v\n\n", c.reduceStatus)
	fmt.Printf("RedReply>>>>> %+v\n\n", reply)
	return nil
}

func (c *Coordinator) GetMapJob(args *ExampleArgs, reply *MapJob) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.mapStatus {
		if v == 0 {
			c.mapStatus[k] = 1
			reply.InputFile = k
			c.mapTaskId += 1
			reply.MapJobNumber = c.mapTaskId
			reply.ReducerCount = c.nReducer
			return nil
		}
	}
	reply.AllCompleted = c.isMapJobsCompleted()
	fmt.Printf("Status>>>> %+v\n\n", c.mapStatus)
	fmt.Printf("Reply>>>>> %+v\n\n", reply)
	// return errors.New("map job not available")
	return nil
}

func (c *Coordinator) isMapJobsCompleted() bool {
	for _, v := range c.mapStatus {
		if v != 2 {
			return false
		}
	}
	fmt.Printf("mjc--> %+v \n\n", c.intermediateFiles)
	return true
}

func (c *Coordinator) isReduceJobsCompleted() bool {
	for _, v := range c.reduceStatus {
		if v != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) MapJobCompleted(args *MapJobCompleted, reply *JobCompletedRes) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapStatus[args.InputFile] = 2
	for _, v := range args.IntermediateFiles {
		arr := strings.Split(v, "-")
		partitionKey, _ := strconv.Atoi(arr[len(arr)-1])
		if val, ok := c.intermediateFiles[partitionKey]; ok {
			newArr := append(val, v)
			c.intermediateFiles[partitionKey] = newArr
		} else {
			arr := make([]string, 0)
			arr = append(arr, v)
			c.intermediateFiles[partitionKey] = arr
		}
	}
	reply.Completed = true
	return nil
}

func (c *Coordinator) ReduceJobCompleted(args *ReduceJobCompleted, reply *JobCompletedRes) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceStatus[args.PartitionKey] = 2
	reply.Completed = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isReduceJobsCompleted()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReducer: nReduce,
	}
	jobs := make(map[string]int)
	for _, file := range files {
		jobs[file] = 0
	}
	c.mapStatus = jobs
	c.intermediateFiles = make(map[int][]string)
	reduceStatus := make(map[int]int)
	for i := 0; i < nReduce; i++ {
		reduceStatus[i] = 0
	}
	c.reduceStatus = reduceStatus

	c.server()
	return &c
}
