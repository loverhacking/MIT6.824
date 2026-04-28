package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add RPC definitions here.
// Worker -> Master
// Worker request a task from Master
type AskTaskArgs struct {
}

type AskTaskReply struct {
	TaskID   int
	TaskType TaskType
	FileName string // needed for MapTask
	NReduce  int
	NMap     int
}

// Worker -> Master
// Worker report a task to Master
type ReportTaskArgs struct {
	TaskID   int
	TaskType TaskType
}

type ReportTaskReply struct {
	Ok bool
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
