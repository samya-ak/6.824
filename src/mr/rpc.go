package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MapJob struct {
	InputFile    string
	MapJobNumber int
	ReducerCount int
	AllCompleted bool
}

type ReduceJob struct {
	IntermediateFiles []string
	PartitionKey      int
	AllCompleted      bool
}

type MapJobCompleted struct {
	InputFile         string
	IntermediateFiles []string
}

type ReduceJobCompleted struct {
	PartitionKey int
}

type JobCompletedRes struct {
	Completed bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
