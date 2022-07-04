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
	"time"
)

type Coordinator struct {
	// Status Code for mapStatus and reduceStatus:
	// 0 - not started
	// 1 - in progress
	// 2 - completed
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

	for k, v := range c.reduceStatus {
		if v == 0 {
			c.reduceStatus[k] = 1
			reply.IntermediateFiles = c.intermediateFiles[k]
			reply.PartitionKey = k
			break
		}
	}

	reply.AllCompleted = c.isReduceJobsCompleted()
	c.mu.Unlock()
	// fmt.Printf("RedStatus>>>> %+v\n\n", c.reduceStatus)
	// fmt.Printf("RedReply>>>>> %+v\n\n", reply)

	// handle worker crash
	if len(reply.IntermediateFiles) > 0 {
		go func(reply *ReduceJob) {
			<-time.After(time.Second * 10)
			c.mu.Lock()
			defer c.mu.Unlock()

			if c.reduceStatus[reply.PartitionKey] == 1 {
				fmt.Printf("Worker Crash- R %d \n\n", reply.PartitionKey)
				// fmt.Printf("Status %d \n\n", c.reduceStatus[reply.PartitionKey])
				c.reduceStatus[reply.PartitionKey] = 0
			}
		}(reply)
	}
	return nil
}

func (c *Coordinator) GetMapJob(args *ExampleArgs, reply *MapJob) error {
	c.mu.Lock()

	for k, v := range c.mapStatus {
		if v == 0 {
			c.mapStatus[k] = 1
			reply.InputFile = k
			c.mapTaskId += 1
			reply.MapJobNumber = c.mapTaskId
			reply.ReducerCount = c.nReducer
			break
		}
	}
	reply.AllCompleted = c.isMapJobsCompleted()
	// fmt.Printf("Status>>>> %+v\n\n", c.mapStatus)
	// fmt.Printf("Reply>>>>> %+v\n\n", reply)

	c.mu.Unlock()
	// handle worker crash
	// start timer only if job is assigned
	if len(reply.InputFile) > 0 {
		// fmt.Printf("Register Timer %+v \n\n", reply)
		go func(reply *MapJob) {
			<-time.After(time.Second * 10)
			c.mu.Lock()
			defer c.mu.Unlock()
			fmt.Printf("Worker Time out --> Check if task is completed %+v\n\n", reply)
			if c.mapStatus[reply.InputFile] == 1 {
				// fmt.Printf("Crashed: Status M%d \n\n", c.mapStatus[reply.InputFile])
				c.mapStatus[reply.InputFile] = 0
			}
		}(reply)
	}
	return nil
}

func (c *Coordinator) isMapJobsCompleted() bool {
	for _, v := range c.mapStatus {
		if v != 2 {
			return false
		}
	}
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
