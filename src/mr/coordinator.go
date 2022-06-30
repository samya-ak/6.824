package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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

func (c *Coordinator) GetMapJob(args *ExampleArgs, reply *MapJob) error {
	for k, v := range c.mapStatus {
		if v == 0 {
			reply.InputFile = k
			c.mapTaskId += 1
			reply.MapJobNumber = c.mapTaskId
			reply.ReducerCount = c.nReducer
			return nil
		}
	}
	return errors.New("map job not available")
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
	ret := false
	for _, v := range c.mapStatus {
		if v == 0 && len(c.reduceStatus) == 0 {
			return ret
		}
	}
	return !ret
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

	c.server()
	return &c
}
